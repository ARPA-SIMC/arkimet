#include <arki/dataset/merged.h>
#include <arki/configfile.h>
#include <arki/metadata.h>
#include <arki/matcher.h>
#include <arki/summary.h>
#include <arki/sort.h>
#include <arki/utils/string.h>
#include <arki/wibble/sys/mutex.h>
#include <arki/wibble/sys/thread.h>
#include <cstring>
// #include <iostream>

using namespace std;
using namespace arki::utils;

namespace arki {
namespace dataset {

/// Fixed size synchronised queue
class SyncBuffer
{
protected:
    static const int buf_size = 10;
    wibble::sys::Mutex mutex;
    wibble::sys::Condition cond;
    Metadata* buffer[buf_size];
    size_t head, tail, size;
    mutable bool m_done;

public:
    SyncBuffer() : head(0), tail(0), size(buf_size), m_done(false)
    {
        memset(buffer, 0, buf_size * sizeof(Metadata*));
    }
    ~SyncBuffer()
    {
        for (unsigned i = 0; i < buf_size; ++i)
            delete buffer[i];
    }

    void push(unique_ptr<Metadata> val)
    {
        wibble::sys::MutexLock lock(mutex);
        while ((head + 1) % size == tail)
            cond.wait(lock);
        buffer[head] = val.release();
        head = (head + 1) % size;
        cond.broadcast();
    }

    unique_ptr<Metadata> pop()
    {
        wibble::sys::MutexLock lock(mutex);
        if (head == tail)
            throw wibble::exception::Consistency("removing an element from a SyncBuffer", "the buffer is empty");
        // Reset the item (this will take care, for example, to dereference
        // refcounted values in the same thread that took them over)
        unique_ptr<Metadata> res(buffer[tail]);
        buffer[tail] = 0;
        tail = (tail + 1) % size;
        cond.broadcast();
        return res;
    }

    // Get must be done by the same thread that does pop
    const Metadata* get()
    {
        wibble::sys::MutexLock lock(mutex);
        while (head == tail && !m_done)
            cond.wait(lock);
        if (head == tail && m_done) return 0;
        return buffer[tail];
    }

    bool isDone()
    {
        wibble::sys::MutexLock lock(mutex);
        if (head != tail)
            return false;
        return m_done;
    }

    void done()
    {
        wibble::sys::MutexLock lock(mutex);
        m_done = true;
        cond.broadcast();
    }
};

class MetadataReader : public wibble::sys::Thread
{
private:
	MetadataReader(const MetadataReader&);
	MetadataReader& operator=(const MetadataReader&);

protected:
	struct Consumer : public metadata::Eater
	{
        SyncBuffer& buf;

        Consumer(SyncBuffer& buf) : buf(buf) {}

        bool eat(std::unique_ptr<Metadata>&& md) override
        {
            buf.push(move(md));
            return true;
        }
	};
	ReadonlyDataset* dataset;
	const DataQuery* query;
	string errorbuf;

	virtual void* main()
	{
		try {
			if (!query)
				throw wibble::exception::Consistency("executing query in subthread", "no query has been set");
			Consumer cons(mdbuf);
			dataset->queryData(*query, cons);
			mdbuf.done();
			return 0;
		} catch (std::exception& e) {
			mdbuf.done();
			errorbuf = e.what();
			return (void*)errorbuf.c_str();
		}
	}

public:
    SyncBuffer mdbuf;

	MetadataReader() : dataset(0), query(0) {}

	void init(ReadonlyDataset& dataset, const DataQuery* query)
	{
		this->dataset = &dataset;
		this->query = query;
	}
};

class SummaryReader : public wibble::sys::Thread
{
protected:
	const Matcher* matcher;
	ReadonlyDataset* dataset;
	string errorbuf;

	virtual void* main()
	{
		try {
			dataset->querySummary(*matcher, summary);
			return 0;
		} catch (std::exception& e) {
			errorbuf = e.what();
			return (void*)errorbuf.c_str();
		}
	}

public:
	Summary summary;

	SummaryReader() : matcher(0), dataset(0) {}

	void init(const Matcher& matcher, ReadonlyDataset& dataset)
	{
		this->matcher = &matcher;
		this->dataset = &dataset;
	}
};

Merged::Merged()
{
}

Merged::~Merged()
{
}

void Merged::addDataset(ReadonlyDataset& ds)
{
	datasets.push_back(&ds);
}

namespace {
template<typename T>
struct RAIIDeleter
{
	T* val;
	RAIIDeleter(T* val) : val(val) {}
	~RAIIDeleter() { delete[] val; }
};
}

void Merged::queryData(const dataset::DataQuery& q, metadata::Eater& consumer)
{
	// Handle the trivial case of only one dataset
	if (datasets.size() == 1)
	{
		datasets[0]->queryData(q, consumer);
		return;
	}

	MetadataReader* readers = new MetadataReader[datasets.size()];
	RAIIDeleter<MetadataReader> cleanup(readers);

	// Start all the readers
	for (size_t i = 0; i < datasets.size(); ++i)
	{
		readers[i].init(*datasets[i], &q);
		readers[i].start();
	}

	// Output items in time-sorted order or in the order asked by q
	// Note: we assume that every dataset will give us data sorted as q
	// asks, so here we just merge sorted data
	refcounted::Pointer<sort::Compare> sorter = q.sorter;
	if (!sorter)
		sorter = sort::Compare::parse("");

	while (true)
	{
        const Metadata* minmd = 0;
        int minmd_idx = 0;
		for (size_t i = 0; i < datasets.size(); ++i)
		{
            const Metadata* md = readers[i].mdbuf.get();
            if (!md) continue;
			if (!minmd || sorter->compare(*md, *minmd) < 0)
			{
				minmd = md;
				minmd_idx = i;
			}
		}
        // When there's nothing more to read, we exit
        if (minmd == 0) break;
        consumer.eat(readers[minmd_idx].mdbuf.pop());
    }

	// Collect all the results
	vector<string> errors;
	for (size_t i = 0; i < datasets.size(); ++i)
		if (void* res = readers[i].join())
			errors.push_back((const char*)res);
	
	if (!errors.empty())
		throw wibble::exception::Consistency("running metadata queries on multiple datasets", str::join("; ", errors.begin(), errors.end()));
}

void Merged::querySummary(const Matcher& matcher, Summary& summary)
{
	// Handle the trivial case of only one dataset
	if (datasets.size() == 1)
	{
		datasets[0]->querySummary(matcher, summary);
		return;
	}

	SummaryReader* readers = new SummaryReader[datasets.size()];
	RAIIDeleter<SummaryReader> cleanup(readers);

	// Start all the readers
	for (size_t i = 0; i < datasets.size(); ++i)
	{
		readers[i].init(matcher, *datasets[i]);
		readers[i].start();
	}

	// Collect all the results
	vector<string> errors;
	for (size_t i = 0; i < datasets.size(); ++i)
	{
		if (void* res = readers[i].join())
			errors.push_back((const char*)res);
		else
			summary.add(readers[i].summary);
	}
	
	if (!errors.empty())
		throw wibble::exception::Consistency("running summary queries on multiple datasets", str::join("; ", errors.begin(), errors.end()));
}

void Merged::queryBytes(const dataset::ByteQuery& q, std::ostream& out)
{
	// Here we must serialize, as we do not know how to merge raw data streams
	//
	// We cannot just wrap queryData because some subdatasets could be
	// remote, and that would mean doing postprocessing on the client side,
	// potentially transferring terabytes of data just to produce a number

    // TODO: data_start_hook may be called more than once here
    // TODO: we might be able to do something smarter, like if we're merging
    // many datasets from the same server we can run it all there; if we're
    // merging all local datasets, wrap queryData; and so on.

	for (std::vector<ReadonlyDataset*>::iterator i = datasets.begin();
			i != datasets.end(); ++i)
		(*i)->queryBytes(q, out);
}

AutoMerged::AutoMerged() {}
AutoMerged::AutoMerged(const ConfigFile& cfg)
{
    for (ConfigFile::const_section_iterator i = cfg.sectionBegin();
            i != cfg.sectionEnd(); ++i)
        datasets.push_back(ReadonlyDataset::create(*(i->second)));
}
AutoMerged::~AutoMerged()
{
    for (std::vector<ReadonlyDataset*>::iterator i = datasets.begin();
            i != datasets.end(); ++i)
        delete *i;
}

}
}
// vim:set ts=4 sw=4:
