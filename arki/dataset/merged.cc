#include "arki/dataset/merged.h"
#include "arki/exceptions.h"
#include "arki/metadata.h"
#include "arki/metadata/sort.h"
#include "arki/matcher.h"
#include "arki/summary.h"
#include "arki/utils/string.h"
#include <cstring>
#include <thread>
#include <mutex>
#include <condition_variable>

using namespace std;
using namespace arki::core;
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
    std::shared_ptr<Metadata> buffer[buf_size];
    size_t head, tail, size;
    mutable bool m_done;

public:
    SyncBuffer(const SyncBuffer&) = delete;
    SyncBuffer(const SyncBuffer&&) = delete;
    SyncBuffer& operator=(const SyncBuffer&) = delete;
    SyncBuffer& operator=(const SyncBuffer&&) = delete;

    SyncBuffer() : head(0), tail(0), size(buf_size), m_done(false)
    {
    }
    ~SyncBuffer()
    {
    }

    void push(std::shared_ptr<Metadata> val)
    {
        std::unique_lock<std::mutex> lock(mutex);
        cond.wait(lock, [this] { return (head + 1) % size != tail; });
        buffer[head] = val;
        head = (head + 1) % size;
        cond.notify_all();
    }

    std::shared_ptr<Metadata> pop()
    {
        std::lock_guard<std::mutex> lock(mutex);
        if (head == tail)
            throw_consistency_error("removing an element from a SyncBuffer", "the buffer is empty");
        // Reset the item (this will take care, for example, to dereference
        // refcounted values in the same thread that took them over)
        std::shared_ptr<Metadata> res(std::move(buffer[tail]));
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
        return buffer[tail].get();
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
public:
    Reader* dataset = 0;
    const DataQuery* query = 0;
    string errorbuf;
    SyncBuffer mdbuf;
    std::thread thread;

    MetadataReader() {}

    void main()
    {
        try {
            if (!query)
                throw_consistency_error("executing query in subthread", "no query has been set");
            dataset->query_data(*query, [&](std::shared_ptr<Metadata> md) {
                mdbuf.push(md);
                return true;
            });
            mdbuf.done();
        } catch (std::exception& e) {
            mdbuf.done();
            errorbuf = e.what();
        }
    }

    void init(Reader& dataset, const DataQuery* query)
    {
        this->dataset = &dataset;
        this->query = query;
    }
};

class SummaryReader
{
public:
    const Matcher* matcher = 0;
    Reader* dataset = 0;
    Summary summary;
    string errorbuf;

    SummaryReader() {}

    void main()
    {
        try {
            dataset->query_summary(*matcher, summary);
        } catch (std::exception& e) {
            errorbuf = e.what();
        }
    }

    void init(const Matcher& matcher, Reader& dataset)
    {
        this->matcher = &matcher;
        this->dataset = &dataset;
    }
};

Merged::Merged(std::shared_ptr<Session> session)
{
    dataset::Config* cfg;
    m_config = std::shared_ptr<dataset::Config>(cfg = new dataset::Config(session));
    cfg->name = "merged";
}

Merged::~Merged()
{
}

std::string Merged::type() const { return "merged"; }

void Merged::add_dataset(std::shared_ptr<Reader> ds)
{
    datasets.emplace_back(ds);
}

void Merged::add_dataset(const core::cfg::Section& cfg)
{
    add_dataset(move(dataset::Reader::create(m_config->session, cfg)));
}

bool Merged::query_data(const dataset::DataQuery& q, metadata_dest_func dest)
{
    // Handle the trivial case of only one dataset
    if (datasets.size() == 1)
        return datasets[0]->query_data(q, dest);

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
    std::shared_ptr<metadata::sort::Compare> sorter = q.sorter;
    if (!sorter)
        sorter = metadata::sort::Compare::parse("");

    bool canceled = false;
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
        if (!canceled)
            if (!dest(readers[minmd_idx].mdbuf.pop()))
                canceled = true;
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
        throw_consistency_error("running metadata queries on multiple datasets", str::join("; ", errors.begin(), errors.end()));

    return canceled;
}

void Merged::query_summary(const Matcher& matcher, Summary& summary)
{
    // Handle the trivial case of only one dataset
    if (datasets.size() == 1)
    {
        datasets[0]->query_summary(matcher, summary);
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
        throw_consistency_error("running summary queries on multiple datasets", str::join("; ", errors.begin(), errors.end()));
}

void Merged::query_bytes(const dataset::ByteQuery& q, NamedFileDescriptor& out)
{
    // Here we must serialize, as we do not know how to merge raw data streams
    //
    // We cannot just wrap query_data because some subdatasets could be
    // remote, and that would mean doing postprocessing on the client side,
    // potentially transferring terabytes of data just to produce a number

    // TODO: data_start_hook may be called more than once here
    // TODO: we might be able to do something smarter, like if we're merging
    // many datasets from the same server we can run it all there; if we're
    // merging all local datasets, wrap queryData; and so on.

    for (auto i: datasets)
        i->query_bytes(q, out);
}

void Merged::query_bytes(const dataset::ByteQuery& q, AbstractOutputFile& out)
{
    // Here we must serialize, as we do not know how to merge raw data streams
    //
    // We cannot just wrap query_data because some subdatasets could be
    // remote, and that would mean doing postprocessing on the client side,
    // potentially transferring terabytes of data just to produce a number

    // TODO: data_start_hook may be called more than once here
    // TODO: we might be able to do something smarter, like if we're merging
    // many datasets from the same server we can run it all there; if we're
    // merging all local datasets, wrap queryData; and so on.

    for (auto i: datasets)
        i->query_bytes(q, out);
}

}
}
