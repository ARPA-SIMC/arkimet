#include "arki/dataset/simple/writer.h"
#include "arki/dataset/index/manifest.h"
#include "arki/dataset/segment.h"
#include "arki/dataset/step.h"
#include "arki/dataset/lock.h"
#include "arki/types/source/blob.h"
#include "arki/types/reftime.h"
#include "arki/utils/files.h"
#include "arki/scan/any.h"
#include "arki/nag.h"
#include "arki/utils/sys.h"
#include "arki/utils/string.h"
#include "arki/metadata.h"
#include "arki/metadata/collection.h"
#include "arki/summary.h"
#include <ctime>
#include <cstdio>

using namespace std;
using namespace arki;
using namespace arki::types;
using namespace arki::utils;
using arki::core::Time;

namespace arki {
namespace dataset {
namespace simple {

/// Accumulate metadata and summaries while writing
struct AppendSegment
{
    std::shared_ptr<segment::Writer> segment;
    utils::sys::Path dir;
    std::string basename;
    std::string pathname;
    bool flushed;
    metadata::Collection mds;
    Summary sum;

    AppendSegment(const std::string& pathname, std::shared_ptr<core::Lock> lock)
        : dir(str::dirname(pathname)), basename(str::basename(pathname)), pathname(pathname), flushed(true)
    {
        struct stat st_data;
        if (!dir.fstatat_ifexists(basename.c_str(), st_data))
            return;

        // Read the metadata
        scan::scan(pathname, lock, mds.inserter_func());

        // Read the summary
        if (!mds.empty())
            mds.add_to_summary(sum);
    }

    AppendSegment(std::shared_ptr<segment::Writer> segment, std::shared_ptr<core::Lock> lock)
        : AppendSegment(segment->absname, lock)
    {
        this->segment = segment;
    }

    ~AppendSegment()
    {
        flush();
    }


    void add(const Metadata& md, const types::source::Blob& source)
    {
        using namespace arki::types;

        // Replace the pathname with its basename
        unique_ptr<Metadata> copy(md.clone());
        copy->set_source(Source::createBlobUnlocked(source.format, dir.name(), basename, source.offset, source.size));
        sum.add(*copy);
        mds.acquire(move(copy));
        flushed = false;
    }
    void flush()
    {
        if (flushed) return;
        mds.writeAtomically(pathname + ".metadata");
        sum.writeAtomically(pathname + ".summary");
        //fsync(dir);
    }
};


Writer::Writer(std::shared_ptr<const simple::Config> config)
    : m_config(config), m_mft(0)
{
    // Create the directory if it does not exist
    sys::makedirs(config->path);

    lock = config->append_lock_dataset();

    // If the index is missing, take note not to perform a repack until a
    // check is made
    if (!index::Manifest::exists(config->path))
        files::createDontpackFlagfile(config->path);

    unique_ptr<index::Manifest> mft = index::Manifest::create(config->path, config->lock_policy, config->index_type);
    m_mft = mft.release();
    m_mft->lock = lock;
    m_mft->openRW();

    lock.reset();
}

Writer::~Writer()
{
    flush();
    delete m_mft;
}

std::string Writer::type() const { return "simple"; }

std::unique_ptr<AppendSegment> Writer::file(const Metadata& md, const std::string& format)
{
    auto segment = segmented::Writer::file(md, format);
    return unique_ptr<AppendSegment>(new AppendSegment(segment, lock));
}

WriterAcquireResult Writer::acquire(Metadata& md, ReplaceStrategy replace)
{
    auto age_check = config().check_acquire_age(md);
    if (age_check.first) return age_check.second;

    if (!lock) lock = config().append_lock_dataset();
    m_mft->lock = lock;
    // TODO: refuse if md is before "archive age"
    auto mdbuf = file(md, md.source().format);

    // Try appending
    try {
        const types::source::Blob& new_source = mdbuf->segment->append(md);
        mdbuf->add(md, new_source);
        mdbuf->segment->commit();
        time_t ts = scan::timestamp(mdbuf->pathname);
        if (ts == 0)
            nag::warning("simple dataset acquire: %s timestamp is 0", mdbuf->pathname.c_str());
        m_mft->acquire(mdbuf->segment->relname, ts, mdbuf->sum);
        return ACQ_OK;
    } catch (std::exception& e) {
        fprintf(stderr, "ERROR %s\n", e.what());
        // sqlite will take care of transaction consistency
        md.add_note("Failed to store in dataset '" + config().name + "': " + e.what());
        return ACQ_ERROR;
    }

    // After appending, keep updated info in-memory, and update manifest on
    // flush when the Datafile structures are deallocated
}

void Writer::acquire_batch(std::vector<std::shared_ptr<WriterBatchElement>>& batch, ReplaceStrategy replace)
{
    for (auto& e: batch)
    {
        e->dataset_name.clear();
        e->result = acquire(e->md, replace);
        if (e->result == ACQ_OK)
            e->dataset_name = name();
    }
}

void Writer::remove(Metadata& md)
{
    // Nothing to do
    throw std::runtime_error("cannot remove data from simple dataset: dataset does not support removing items");
}

void Writer::flush()
{
    segmented::Writer::flush();
    m_mft->flush();
    lock.reset();
}

Pending Writer::test_writelock()
{
    return m_mft->test_writelock();
}

void Writer::test_acquire(const ConfigFile& cfg, std::vector<std::shared_ptr<WriterBatchElement>>& batch, std::ostream& out)
{
    std::shared_ptr<const simple::Config> config(new simple::Config(cfg));
    for (auto& e: batch)
    {
        auto age_check = config->check_acquire_age(e->md);
        if (age_check.first)
        {
            e->result = age_check.second;
            if (age_check.second == ACQ_OK)
                e->dataset_name = config->name;
            else
                e->dataset_name.clear();
        } else {
            // Acquire on simple datasets always succeeds except in case of envrionment
            // issues like I/O errors and full disks
            e->result = ACQ_OK;
            e->dataset_name = config->name;
        }
    }
}

}
}
}
