#include "arki/dataset/simple/writer.h"
#include "arki/dataset/index/manifest.h"
#include "arki/dataset/step.h"
#include "arki/dataset/lock.h"
#include "arki/dataset/session.h"
#include "arki/types/source/blob.h"
#include "arki/types/reftime.h"
#include "arki/scan.h"
#include "arki/utils/sys.h"
#include "arki/utils/string.h"
#include "arki/utils/accounting.h"
#include "arki/utils/files.h"
#include "arki/metadata.h"
#include "arki/metadata/collection.h"
#include "arki/summary.h"

using namespace std;
using namespace arki;
using namespace arki::types;
using namespace arki::utils;
using arki::core::Time;

namespace arki {
namespace dataset {
namespace simple {

/// Accumulate metadata and summaries while writing
class AppendSegment
{
public:
    std::shared_ptr<simple::Dataset> dataset;
    std::shared_ptr<dataset::AppendLock> lock;
    std::shared_ptr<segment::data::Writer> segment;
    utils::sys::Path dir;
    std::string basename;
    bool flushed = true;
    metadata::Collection mds;
    Summary sum;

    AppendSegment(std::shared_ptr<simple::Dataset> dataset, std::shared_ptr<dataset::AppendLock> lock, std::shared_ptr<segment::data::Writer> segment)
        : dataset(dataset), lock(lock), segment(segment),
          dir(segment->segment().abspath().parent_path()),
          basename(segment->segment().abspath().filename())
    {
        struct stat st_data;
        if (!dir.fstatat_ifexists(basename.c_str(), st_data))
            return;

        // Read the metadata
        auto reader = segment->data().reader(lock);
        reader->scan(mds.inserter_func());

        // Read the summary
        if (!mds.empty())
            mds.add_to_summary(sum);
    }

    void add(const Metadata& md, const types::source::Blob& source)
    {
        using namespace arki::types;

        // Replace the pathname with its basename
        auto copy(md.clone());
        if (!dataset->smallfiles)
            copy->unset(TYPE_VALUE);
        copy->set_source(Source::createBlobUnlocked(source.format, dir.path(), basename, source.offset, source.size));
        sum.add(*copy);
        mds.acquire(std::move(copy));
        flushed = false;
    }

    WriterAcquireResult acquire(Metadata& md)
    {
        auto mft = index::Manifest::create(dataset, dataset->index_type);
        mft->lock = lock;
        mft->openRW();

        // Try appending
        try {
            const types::source::Blob& new_source = segment->append(md);
            add(md, new_source);
            segment->commit();
            time_t ts = segment->data().timestamp();
            mft->acquire(segment->segment().relpath(), ts, sum);
            mds.writeAtomically(sys::with_suffix(segment->segment().abspath(), ".metadata"));
            sum.writeAtomically(sys::with_suffix(segment->segment().abspath(), ".summary"));
            mft->flush();
            return ACQ_OK;
        } catch (std::exception& e) {
            // sqlite will take care of transaction consistency
            md.add_note("Failed to store in dataset '" + dataset->name() + "': " + e.what());
            return ACQ_ERROR;
        }
    }

    void acquire_batch(WriterBatch& batch)
    {
        auto mft = index::Manifest::create(dataset, dataset->index_type);
        mft->lock = lock;
        mft->openRW();

        try {
            for (auto& e: batch)
            {
                e->dataset_name.clear();
                const types::source::Blob& new_source = segment->append(e->md);
                add(e->md, new_source);
                e->result = ACQ_OK;
                e->dataset_name = dataset->name();
            }
        } catch (std::exception& e) {
            // sqlite will take care of transaction consistency
            batch.set_all_error("Failed to store in dataset '" + dataset->name() + "': " + e.what());
            return;
        }

        segment->commit();
        time_t ts = segment->data().timestamp();
        mft->acquire(segment->segment().relpath(), ts, sum);
        mds.writeAtomically(sys::with_suffix(segment->segment().abspath(), ".metadata"));
        sum.writeAtomically(sys::with_suffix(segment->segment().abspath(), ".summary"));
        mft->flush();
    }
};


Writer::Writer(std::shared_ptr<simple::Dataset> dataset)
    : DatasetAccess(dataset)
{
    // Create the directory if it does not exist
    std::filesystem::create_directories(dataset->path);

    // If the index is missing, take note not to perform a repack until a
    // check is made
    if (!index::Manifest::exists(dataset->path))
        files::createDontpackFlagfile(dataset->path);
}

Writer::~Writer()
{
    flush();
}

std::string Writer::type() const { return "simple"; }

std::unique_ptr<AppendSegment> Writer::file(const segment::data::WriterConfig& writer_config, const Metadata& md, DataFormat format)
{
    auto lock = dataset().append_lock_dataset();
    core::Time time = md.get<types::reftime::Position>()->get_Position();
    auto relpath = sys::with_suffix(dataset().step()(time), "."s + format_name(md.source().format));
    auto writer = dataset().segment_session->segment_writer(writer_config, format, relpath);
    return std::unique_ptr<AppendSegment>(new AppendSegment(m_dataset, lock, writer));
}

std::unique_ptr<AppendSegment> Writer::file(const segment::data::WriterConfig& writer_config, const std::filesystem::path& relpath)
{
    std::filesystem::create_directories((dataset().path / relpath).parent_path());
    auto lock = dataset().append_lock_dataset();
    auto segment = dataset().segment_session->segment_writer(writer_config, scan::Scanner::format_from_filename(relpath), relpath);
    return std::unique_ptr<AppendSegment>(new AppendSegment(m_dataset, lock, segment));
}

WriterAcquireResult Writer::acquire(Metadata& md, const AcquireConfig& cfg)
{
    acct::acquire_single_count.incr();
    auto age_check = dataset().check_acquire_age(md);
    if (age_check.first) return age_check.second;

    segment::data::WriterConfig writer_config;
    writer_config.drop_cached_data_on_commit = cfg.drop_cached_data_on_commit;
    writer_config.eatmydata = dataset().eatmydata;

    auto segment = file(writer_config, md, md.source().format);
    return segment->acquire(md);
}

void Writer::acquire_batch(WriterBatch& batch, const AcquireConfig& cfg)
{
    acct::acquire_batch_count.incr();

    segment::data::WriterConfig writer_config;
    writer_config.drop_cached_data_on_commit = cfg.drop_cached_data_on_commit;
    writer_config.eatmydata = dataset().eatmydata;

    std::map<std::string, WriterBatch> by_segment = batch_by_segment(batch);

    // Import segment by segment
    for (auto& s: by_segment)
    {
        auto segment = file(writer_config, s.first);
        segment->acquire_batch(s.second);
    }
}

void Writer::test_acquire(std::shared_ptr<Session> session, const core::cfg::Section& cfg, WriterBatch& batch)
{
    std::shared_ptr<const simple::Dataset> dataset(new simple::Dataset(session, cfg));
    for (auto& e: batch)
    {
        auto age_check = dataset->check_acquire_age(e->md);
        if (age_check.first)
        {
            e->result = age_check.second;
            if (age_check.second == ACQ_OK)
                e->dataset_name = dataset->name();
            else
                e->dataset_name.clear();
        } else {
            // Acquire on simple datasets always succeeds except in case of envrionment
            // issues like I/O errors and full disks
            e->result = ACQ_OK;
            e->dataset_name = dataset->name();
        }
    }
}

}
}
}
