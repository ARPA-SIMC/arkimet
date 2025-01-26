#include "arki/dataset/simple/writer.h"
#include "arki/dataset/simple/manifest.h"
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

namespace arki::dataset::simple {

/// Accumulate metadata and summaries while writing
class AppendSegment
{
public:
    std::shared_ptr<Segment> segment;
    std::shared_ptr<simple::Dataset> dataset;
    std::shared_ptr<core::AppendLock> lock;
    std::shared_ptr<segment::data::Writer> segment_data_writer;
    utils::sys::Path dir;
    std::string basename;
    metadata::Collection mds;
    Summary sum;

    AppendSegment(std::shared_ptr<Segment> segment, std::shared_ptr<simple::Dataset> dataset, std::shared_ptr<core::AppendLock> lock, std::shared_ptr<segment::data::Writer> segment_data_writer)
        : segment(segment), dataset(dataset), lock(lock), segment_data_writer(segment_data_writer),
          dir(segment->abspath().parent_path()),
          basename(segment->abspath().filename())
    {
        struct stat st_data;
        if (!dir.fstatat_ifexists(basename.c_str(), st_data))
            return;

        // Read the metadata
        auto reader = segment->reader(lock);
        reader->read_all(mds.inserter_func());

        // Read the summary
        if (!mds.empty())
            mds.add_to_summary(sum);
    }

    void add(const Metadata& md, const types::source::Blob& source)
    {
        using namespace arki::types;

        // Replace the pathname with its basename
        auto copy(md.clone());
        if (!segment->session().smallfiles)
            copy->unset(TYPE_VALUE);
        copy->set_source(Source::createBlobUnlocked(source.format, dir.path(), basename, source.offset, source.size));
        sum.add(*copy);
        mds.acquire(std::move(copy));
    }

    void write_metadata()
    {
        auto path_md = segment->abspath_metadata();
        auto path_sum = segment->abspath_summary();

        mds.prepare_for_segment_metadata();
        mds.writeAtomically(path_md);
        sum.writeAtomically(path_sum);

        // Synchronize summary and metadata timestamps.
        // This is not normally needed, as the files are written and flushed in
        // the correct order.
        //
        // When running with filesystem sync disabled (eatmydata or
        // systemd-nspawn --suppress-sync) however there are cases where the
        // summary timestamp ends up one second earlier than the metadata
        // timestamp, which then gets flagged by a dataset check
        auto ts_md = sys::timestamp(path_md);
        sys::touch(path_sum, ts_md);
    }

    bool acquire_batch(metadata::InboundBatch& batch)
    {
        try {
            for (auto& e: batch)
            {
                e->destination.clear();
                const types::source::Blob& new_source = segment_data_writer->append(*e->md);
                add(*e->md, new_source);
                e->result = metadata::Inbound::Result::OK;
                e->destination = dataset->name();
            }
        } catch (std::exception& e) {
            // sqlite will take care of transaction consistency
            batch.set_all_error("Failed to store in dataset '" + dataset->name() + "': " + e.what());
            return false;
        }

        segment_data_writer->commit();
        write_metadata();
        return true;
    }
};


Writer::Writer(std::shared_ptr<simple::Dataset> dataset)
    : DatasetAccess(dataset), manifest(dataset->path, dataset->eatmydata)
{
    // Create the directory if it does not exist
    std::filesystem::create_directories(dataset->path);

    // If the index is missing, take note not to perform a repack until a
    // check is made
    if (!manifest::exists(dataset->path))
        files::createDontpackFlagfile(dataset->path);
}

Writer::~Writer()
{
    flush();
}

std::string Writer::type() const { return "simple"; }

void Writer::invalidate_summary()
{
    std::filesystem::remove(dataset().path / "summary");
}

std::unique_ptr<AppendSegment> Writer::file(const segment::data::WriterConfig& writer_config, const Metadata& md, DataFormat format, std::shared_ptr<core::AppendLock> lock)
{
    core::Time time = md.get<types::reftime::Position>()->get_Position();
    auto relpath = sys::with_suffix(dataset().step()(time), "."s + format_name(md.source().format));
    auto segment = dataset().segment_session->segment_from_relpath_and_format(relpath, format);
    auto writer = dataset().segment_session->segment_data_writer(segment, writer_config);
    return std::unique_ptr<AppendSegment>(new AppendSegment(segment, m_dataset, lock, writer));
}

std::unique_ptr<AppendSegment> Writer::file(const segment::data::WriterConfig& writer_config, const std::filesystem::path& relpath, std::shared_ptr<core::AppendLock> lock)
{
    std::filesystem::create_directories((dataset().path / relpath).parent_path());
    auto segment = dataset().segment_session->segment_from_relpath(relpath);
    auto segment_data_writer = dataset().segment_session->segment_data_writer(segment, writer_config);
    return std::unique_ptr<AppendSegment>(new AppendSegment(segment, m_dataset, lock, segment_data_writer));
}

void Writer::acquire_batch(metadata::InboundBatch& batch, const AcquireConfig& cfg)
{
    acct::acquire_batch_count.incr();

    segment::data::WriterConfig writer_config;
    writer_config.drop_cached_data_on_commit = cfg.drop_cached_data_on_commit;

    std::map<std::string, metadata::InboundBatch> by_segment = batch_by_segment(batch);

    // Import data grouped by segment
    auto lock = dataset().append_lock_dataset();
    manifest.reread();
    bool changed = false;
    for (auto& s: by_segment)
    {
        auto segment = file(writer_config, s.first, lock);
        if (segment->acquire_batch(s.second))
        {
            time_t ts = segment->segment_data_writer->data().timestamp().value();
            manifest.set(segment->segment->relpath(), ts, segment->sum.get_reference_time());
            invalidate_summary();
            changed = true;
        }
    }
    if (changed)
        manifest.flush();
}

void Writer::test_acquire(std::shared_ptr<Session> session, const core::cfg::Section& cfg, metadata::InboundBatch& batch)
{
    std::shared_ptr<const simple::Dataset> dataset(new simple::Dataset(session, cfg));
    for (auto& e: batch)
    {
        auto age_check = dataset->check_acquire_age(*e->md);
        if (age_check.first)
        {
            e->result = age_check.second;
            if (age_check.second == metadata::Inbound::Result::OK)
                e->destination = dataset->name();
            else
                e->destination.clear();
        } else {
            // Acquire on simple datasets always succeeds except in case of envrionment
            // issues like I/O errors and full disks
            e->result = metadata::Inbound::Result::OK;
            e->destination = dataset->name();
        }
    }
}

}
