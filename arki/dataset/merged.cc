#include "arki/dataset/merged.h"
#include "arki/core/time.h"
#include "arki/dataset/pool.h"
#include "arki/exceptions.h"
#include "arki/matcher.h"
#include "arki/metadata/sort.h"
#include "arki/query.h"
#include "arki/query/progress.h"
#include "arki/summary.h"
#include "arki/utils/string.h"
#include <atomic>
#include <condition_variable>
#include <cstring>
#include <mutex>
#include <thread>

using namespace std;
using namespace arki::core;
using namespace arki::utils;

namespace arki {
namespace dataset {
namespace merged {

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
    SyncBuffer(const SyncBuffer&)             = delete;
    SyncBuffer(const SyncBuffer&&)            = delete;
    SyncBuffer& operator=(const SyncBuffer&)  = delete;
    SyncBuffer& operator=(const SyncBuffer&&) = delete;

    SyncBuffer() : head(0), tail(0), size(buf_size), m_done(false) {}
    ~SyncBuffer() {}

    void push(std::shared_ptr<Metadata> val)
    {
        std::unique_lock<std::mutex> lock(mutex);
        cond.wait(lock, [this] { return (head + 1) % size != tail; });
        buffer[head] = val;
        head         = (head + 1) % size;
        cond.notify_all();
    }

    std::shared_ptr<Metadata> pop()
    {
        std::lock_guard<std::mutex> lock(mutex);
        if (head == tail)
            throw_consistency_error("removing an element from a SyncBuffer",
                                    "the buffer is empty");
        // Reset the item (this will take care, for example, to dereference
        // refcounted values in the same thread that took them over)
        std::shared_ptr<Metadata> res(std::move(buffer[tail]));
        tail = (tail + 1) % size;
        cond.notify_all();
        return res;
    }

    /**
     * Peek at the first element in the queue. If the queue is empty, wait for
     * an element to appear.
     *
     * Get must be done by the same thread that does pop
     */
    const Metadata* get()
    {
        std::unique_lock<std::mutex> lock(mutex);
        cond.wait(lock, [this] { return head != tail || m_done; });
        if (head == tail && m_done)
            return 0;
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

/**
 * Thread that runs a dataset::Reader for one of the components of the merged
 * dataset
 */
class ReaderThread
{
protected:
    std::thread thread;
    std::atomic_bool all_ok;

public:
    ReaderThread() : all_ok(true) {}
    virtual ~ReaderThread()
    {
        all_ok.store(false);
        if (thread.joinable())
            thread.join();
    }
    virtual void main() = 0;
    void start()
    {
        thread = std::thread([this] { main(); });
    }
    void join() { thread.join(); }
    void stop_producing() { all_ok.store(false); }
};

/**
 * Reader thread for running a query_data
 */
class MetadataReader : public ReaderThread
{
public:
    std::shared_ptr<dataset::Reader> dataset;
    query::Data query;
    string errorbuf;
    SyncBuffer mdbuf;

    MetadataReader() {}

    void main() override
    {
        try
        {
            dataset->query_data(query, [&](std::shared_ptr<Metadata> md) {
                mdbuf.push(md);
                return all_ok.load();
            });
            mdbuf.done();
        }
        catch (std::exception& e)
        {
            mdbuf.done();
            errorbuf = e.what();
        }
    }

    void start(std::shared_ptr<dataset::Reader> dataset,
               const query::Data& query)
    {
        this->dataset = dataset;
        this->query   = query;
        this->query.progress.reset();
        ReaderThread::start();
    }
};

/**
 * Reader thread for running a query_summary
 */
class SummaryReader : public ReaderThread
{
public:
    std::shared_ptr<dataset::Reader> dataset;
    Matcher matcher;
    Summary summary;
    string errorbuf;

    SummaryReader() {}

    void main() override
    {
        try
        {
            dataset->query_summary(matcher, summary);
        }
        catch (std::exception& e)
        {
            errorbuf = e.what();
        }
    }

    void start(std::shared_ptr<dataset::Reader> dataset, const Matcher& matcher)
    {
        this->matcher = matcher;
        this->dataset = dataset;
        ReaderThread::start();
    }
};

/**
 * Pool of dataset MetadataReader threads
 */
class MetadataReaderPool
{
    // Reader threads that we manage
    std::vector<MetadataReader> readers;

    /**
     * Sorted used for merging produced data
     *
     * Note: we can assume that every dataset will give us data already sorted
     * as required by the query, so here we just merge sorted data
     */
    std::shared_ptr<metadata::sort::Compare> sorter;

public:
    MetadataReaderPool(std::vector<std::shared_ptr<dataset::Reader>>& datasets,
                       const query::Data& query)
        : readers(datasets.size()), sorter(query.sorter)
    {
        query::Data subquery = query;
        // Disable progress reporting on the query that we give to each
        // component dataset, to avoid reporting data production twice
        subquery.progress    = nullptr;

        if (!sorter)
            sorter = metadata::sort::Compare::parse("");

        for (size_t i = 0; i < datasets.size(); ++i)
            readers[i].start(datasets[i], subquery);
    }

    /**
     * Get the next element from reader threads, in sorter order
     */
    std::shared_ptr<Metadata> pop()
    {
        // Look in all dataset queues for the metadata with the smallest value
        const Metadata* minmd    = nullptr;
        MetadataReader* selected = nullptr;
        for (auto& reader : readers)
        {
            const Metadata* md = reader.mdbuf.get();
            if (!md)
                continue;
            if (!minmd || sorter->compare(*md, *minmd) < 0)
            {
                minmd    = md;
                selected = &reader;
            }
        }

        // When there's nothing more to read, we exit
        if (minmd == nullptr)
        {
            shutdown();
            return std::shared_ptr<Metadata>();
        }

        // Pop the selected value out of its queue
        return selected->mdbuf.pop();
    }

    void cancel()
    {
        for (auto& r : readers)
            r.stop_producing();
        while (pop())
            ;
    }

    void shutdown()
    {
        std::vector<std::string> errors;
        for (auto& reader : readers)
        {
            try
            {
                reader.join();
            }
            catch (std::exception& e)
            {
                errors.push_back(e.what());
                continue;
            }
            if (!reader.errorbuf.empty())
                errors.push_back(reader.errorbuf);
        }
        if (!errors.empty())
            throw std::runtime_error(
                "problems running metadata queries on multiple datasets: " +
                str::join("; ", errors.begin(), errors.end()));
    }
};

Dataset::Dataset(std::shared_ptr<dataset::Pool> pool)
    : dataset::Dataset(pool->session(), "merged")
{
    pool->foreach_dataset([&](std::shared_ptr<arki::dataset::Dataset> ds) {
        datasets.emplace_back(ds->create_reader());
        return true;
    });
}

std::shared_ptr<dataset::Reader> Dataset::create_reader()
{
    return std::make_shared<Reader>(
        static_pointer_cast<Dataset>(shared_from_this()));
}

Reader::~Reader() {}

std::string Reader::type() const { return "merged"; }

bool Reader::impl_query_data(const query::Data& query, metadata_dest_func dest)
{
    query::TrackProgress track(query.progress);
    dest = track.wrap(dest);

    auto& datasets = dataset().datasets;

    // Handle the trivial case of only one dataset
    if (datasets.size() == 1)
        return track.done(datasets[0]->query_data(query, dest));

    // Start all the readers
    MetadataReaderPool readers(datasets, query);

    // TODO: we need to join or detach the threads, otherwise if we raise an
    // exception before the join further down, we get
    // https://stackoverflow.com/questions/7381757/c-terminate-called-without-an-active-exception

    bool canceled = false;
    while (true)
    {
        auto md = readers.pop();
        if (!md)
            break;

        bool ok;
        try
        {
            ok = dest(md);
        }
        catch (std::exception& e)
        {
            readers.cancel();
            throw;
        }

        if (!ok)
        {
            canceled = true;
            readers.cancel();
        }
    }

    return track.done(canceled);
}

void Reader::impl_query_summary(const Matcher& matcher, Summary& summary)
{
    auto& datasets = dataset().datasets;

    // Handle the trivial case of only one dataset
    if (datasets.size() == 1)
    {
        datasets[0]->query_summary(matcher, summary);
        return;
    }

    // Start all the readers
    vector<SummaryReader> readers(datasets.size());
    for (size_t i = 0; i < datasets.size(); ++i)
        readers[i].start(datasets[i], matcher);

    // Collect all the results
    std::vector<std::string> errors;
    for (auto& reader : readers)
    {
        reader.join();
        if (reader.errorbuf.empty())
            summary.add(reader.summary);
        else
            errors.push_back(reader.errorbuf);
    }
    if (!errors.empty())
        throw_consistency_error("running summary queries on multiple datasets",
                                str::join("; ", errors.begin(), errors.end()));
}

namespace {

class QueryBytesProgress : public query::Progress
{
    std::shared_ptr<query::Progress> wrapped;

public:
    QueryBytesProgress(std::shared_ptr<query::Progress> wrapped)
        : wrapped(wrapped)
    {
        if (wrapped)
            wrapped->start();
    }
    ~QueryBytesProgress() {}
    void start(size_t expected_count = 0, size_t expected_bytes = 0) override {}
    void update(size_t count, size_t bytes) override
    {
        if (wrapped)
            wrapped->update(count, bytes);
    }
    void done() override {}

    void actual_done()
    {
        if (wrapped)
            wrapped->done();
    }
};

} // namespace

void Reader::impl_stream_query_bytes(const query::Bytes& q, StreamOutput& out)
{
    // Here we must serialize, as we do not know how to merge raw data streams
    //
    // We cannot just wrap query_data because some subdatasets could be
    // remote, and that would mean doing postprocessing on the client side,
    // potentially transferring terabytes of data just to produce a number

    // TODO: we might be able to do something smarter, like if we're merging
    // many datasets from the same server we can run it all there; if we're
    // merging all local datasets, wrap queryData; and so on.
    query::Bytes localq(q);
    auto wrapped_progress = std::make_shared<QueryBytesProgress>(q.progress);
    localq.progress       = wrapped_progress;

    for (auto i : dataset().datasets)
        i->query_bytes(localq, out);

    wrapped_progress->actual_done();
}

core::Interval Reader::get_stored_time_interval()
{
    throw std::runtime_error(
        "merged::Reader::get_stored_time_interval not yet implemented");
}

} // namespace merged
} // namespace dataset
} // namespace arki
