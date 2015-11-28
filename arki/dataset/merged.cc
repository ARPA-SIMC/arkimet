#include "arki/dataset/merged.h"
#include "arki/configfile.h"
#include "arki/metadata.h"
#include "arki/matcher.h"
#include "arki/summary.h"
#include "arki/sort.h"
#include "arki/utils/string.h"
#include "arki/wibble/exception.h"
#include <cstring>
#include <thread>
#include <mutex>
#include <condition_variable>
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
    std::mutex mutex;
    std::condition_variable cond;
    Metadata* buffer[buf_size];
    size_t head, tail, size;
    mutable bool m_done;

public:
    SyncBuffer(const SyncBuffer&) = delete;
    SyncBuffer(const SyncBuffer&&) = delete;
    SyncBuffer& operator=(const SyncBuffer&) = delete;
    SyncBuffer& operator=(const SyncBuffer&&) = delete;

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
        std::unique_lock<std::mutex> lock(mutex);
        cond.wait(lock, [this] { return (head + 1) % size != tail; });
        buffer[head] = val.release();
        head = (head + 1) % size;
        cond.notify_all();
    }

    unique_ptr<Metadata> pop()
    {
        std::lock_guard<std::mutex> lock(mutex);
        if (head == tail)
            throw wibble::exception::Consistency("removing an element from a SyncBuffer", "the buffer is empty");
        // Reset the item (this will take care, for example, to dereference
        // refcounted values in the same thread that took them over)
        unique_ptr<Metadata> res(buffer[tail]);
        buffer[tail] = 0;
        tail = (tail + 1) % size;
        cond.notify_all();
        return res;
    }

    // Get must be done by the same thread that does pop
    const Metadata* get()
    {
        std::unique_lock<std::mutex> lock(mutex);
        cond.wait(lock, [this] { return head != tail || m_done; });
        if (head == tail && m_done) return 0;
        return buffer[tail];
    }

    bool isDone()
    {
        std::lock_guard<std::mutex> lock(mutex);
        if (head != tail)
            return false;
        return m_done;
    }

    void done()
    {
        std::lock_guard<std::mutex> lock(mutex);
        m_done = true;
        cond.notify_all();
    }
};

class MetadataReader
{
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

public:
    ReadonlyDataset* dataset = 0;
    const DataQuery* query = 0;
    string errorbuf;
    SyncBuffer mdbuf;
    std::thread thread;

    MetadataReader() {}

    void main()
    {
        try {
            if (!query)
                throw wibble::exception::Consistency("executing query in subthread", "no query has been set");
            Consumer cons(mdbuf);
            dataset->queryData(*query, cons);
            mdbuf.done();
        } catch (std::exception& e) {
            mdbuf.done();
            errorbuf = e.what();
        }
    }

	void init(ReadonlyDataset& dataset, const DataQuery* query)
	{
		this->dataset = &dataset;
		this->query = query;
	}
};

class SummaryReader
{
public:
    const Matcher* matcher = 0;
    ReadonlyDataset* dataset = 0;
    Summary summary;
    string errorbuf;

    SummaryReader() {}

    void main()
    {
        try {
            dataset->querySummary(*matcher, summary);
        } catch (std::exception& e) {
            errorbuf = e.what();
        }
    }

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

void Merged::queryData(const dataset::DataQuery& q, metadata::Eater& consumer)
{
	// Handle the trivial case of only one dataset
	if (datasets.size() == 1)
	{
		datasets[0]->queryData(q, consumer);
		return;
	}

    vector<MetadataReader> readers(datasets.size());
    vector<std::thread> threads;

    // Start all the readers
    for (size_t i = 0; i < datasets.size(); ++i)
    {
        readers[i].init(*datasets[i], &q);
        threads.emplace_back(&MetadataReader::main, &readers[i]);
    }

    // Output items in time-sorted order or in the order asked by q
    // Note: we assume that every dataset will give us data sorted as q
    // asks, so here we just merge sorted data
    shared_ptr<sort::Compare> sorter = q.sorter;
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
    {
        try {
            threads[i].join();
        } catch (std::exception& e) {
            errors.push_back(e.what());
            continue;
        }
        if (!readers[i].errorbuf.empty())
            errors.push_back(readers[i].errorbuf);
    }
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

    vector<SummaryReader> readers(datasets.size());
    vector<std::thread> threads;

    // Start all the readers
    for (size_t i = 0; i < datasets.size(); ++i)
    {
        readers[i].init(matcher, *datasets[i]);
        threads.emplace_back(&SummaryReader::main, &readers[i]);
    }

    // Collect all the results
    vector<string> errors;
    for (size_t i = 0; i < datasets.size(); ++i)
    {
        threads[i].join();
        if (readers[i].errorbuf.empty())
            summary.add(readers[i].summary);
        else
            errors.push_back(readers[i].errorbuf);
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
