#include "scan.h"
#include "arki/core/lock.h"
#include "arki/metadata.h"
#include "arki/metadata/collection.h"
#include "arki/metadata/inbound.h"
#include "arki/query.h"
#include "arki/types/source/blob.h"
#include "arki/utils/sys.h"
#include "data.h"
#include "reporter.h"

using namespace arki::utils;

namespace arki::segment::scan {

bool has_data(std::shared_ptr<const Segment> segment)
{
    auto data = Data::create(segment);
    return !data->is_empty();
}

/*
 * Session
 */

std::shared_ptr<segment::Reader>
Session::create_segment_reader(std::shared_ptr<const Segment> segment,
                               std::shared_ptr<const core::ReadLock> lock) const
{
    return std::make_shared<segment::scan::Reader>(segment, lock);
}

std::shared_ptr<segment::Writer>
Session::segment_writer(std::shared_ptr<const Segment> segment,
                        std::shared_ptr<core::AppendLock> lock) const
{
    return std::make_shared<segment::scan::Writer>(segment, lock);
}

std::shared_ptr<segment::Checker>
Session::segment_checker(std::shared_ptr<const Segment> segment,
                         std::shared_ptr<core::CheckLock> lock) const
{
    return std::make_shared<segment::scan::Checker>(segment, lock);
}

/*
 * Reader
 */

Reader::Reader(std::shared_ptr<const Segment> segment,
               std::shared_ptr<const core::ReadLock> lock)
    : segment::Reader(segment, lock), data(segment::Data::create(segment)),
      data_reader(data->reader(lock))
{
}

Reader::~Reader() {}

std::vector<uint8_t> Reader::read(const types::source::Blob& src)
{
    return data_reader->read(src);
}

stream::SendResult Reader::stream(const types::source::Blob& src,
                                  StreamOutput& out)
{
    return data_reader->stream(src, out);
}

bool Reader::read_all(metadata_dest_func dest)
{
    return data_reader->scan_data(dest);
}

bool Reader::query_data(const query::Data& q, metadata_dest_func dest)
{
    arki::metadata::Collection mdbuf;

    data_reader->scan_data([&](auto md) {
        if (q.matcher(*md))
            mdbuf.acquire(md);
        if (not q.with_data)
            md->sourceBlob().unlock();
        return true;
    });

    // Sort and output the rest
    if (q.sorter)
        mdbuf.sort(*q.sorter);

    // pass it to consumer
    return mdbuf.move_to(dest);
}

void Reader::query_summary(const Matcher& matcher, Summary& summary)
{
    data_reader->scan_data([&](auto md) {
        if (matcher(*md))
            summary.add(*md);
        return true;
    });
}

/*
 * Writer
 */

Writer::Writer(std::shared_ptr<const Segment> segment,
               std::shared_ptr<core::AppendLock> lock)
    : segment::Writer(segment, lock), data(Data::create(segment))
{
}
Writer::~Writer() {}

std::shared_ptr<segment::data::Writer>
Writer::get_data_writer(const segment::WriterConfig& config) const
{
    std::filesystem::create_directories(m_segment->abspath().parent_path());
    return data->writer(config);
}

Writer::AcquireResult Writer::acquire(arki::metadata::InboundBatch& batch,
                                      const WriterConfig& config)
{
    auto data_writer = get_data_writer(config);
    try
    {
        for (auto& e : batch)
        {
            e->destination.clear();
            data_writer->append(*e->md);
            e->result      = arki::metadata::Inbound::Result::OK;
            e->destination = config.destination_name;
        }
    }
    catch (std::exception& e)
    {
        // returning here before commit means no action is taken
        batch.set_all_error("Failed to store in '" + config.destination_name +
                            "': " + e.what());
        AcquireResult res;
        res.count_ok      = 0;
        res.count_failed  = batch.size();
        res.segment_mtime = data->timestamp().value_or(0);
        res.data_timespan = core::Interval();
        return res;
    }

    data_writer->commit();

    auto ts = data->timestamp();
    if (!ts)
        throw std::runtime_error(segment().abspath().native() +
                                 ": segment not found after importing");

    AcquireResult res;
    res.count_ok      = batch.size();
    res.count_failed  = 0;
    res.segment_mtime = ts.value();
    res.data_timespan = core::Interval();
    return res;
}

/*
 * Checker
 */

Checker::Checker(std::shared_ptr<const Segment> segment,
                 std::shared_ptr<core::CheckLock> lock)
    : segment::Checker(segment, lock), data(Data::create(segment))
{
}

void Checker::update_data() { data = Data::create(m_segment); }

bool Checker::has_data() const { return data->exists_on_disk(); }
std::optional<time_t> Checker::timestamp() const { return data->timestamp(); }
bool Checker::allows_tar() const { return !data->single_file(); }
bool Checker::allows_zip() const { return !data->single_file(); }
bool Checker::allows_compress() const { return data->single_file(); }

arki::metadata::Collection Checker::scan()
{
    // Rescan the file
    auto data_reader = data->reader(lock);
    arki::metadata::Collection res;
    data_reader->scan_data(res.inserter_func());
    return res;
}

Checker::FsckResult Checker::fsck(segment::Reporter& reporter, bool quick)
{
    Checker::FsckResult res;

    auto data_checker = data->checker();
    auto ts_data      = data->timestamp();
    if (!ts_data)
    {
        reporter.info(segment(), "segment data not found on disk");
        res.state = SEGMENT_MISSING;
        return res;
    }
    res.mtime = ts_data.value();
    res.size  = data->size();

    auto mds = scan();

    if (mds.empty())
    {
        reporter.info(segment(), "the segment is fully deleted");
        res.state += SEGMENT_DELETED;
    }
    else
    {
        // Compute the span of reftimes inside the segment
        mds.sort_segment();
        if (!mds.expand_date_range(res.interval))
        {
            reporter.info(
                segment(),
                "segment contains data without reference time information");
            res.state += SEGMENT_CORRUPTED;
        }
        else
        {
            res.state += data_checker->check(
                [&](const std::string& msg) { reporter.info(segment(), msg); },
                mds, quick);
        }
    }

    return res;
}

bool Checker::scan_data(segment::Reporter& reporter, metadata_dest_func dest)
{
    auto data_checker = data->checker();
    return data_checker->rescan_data(
        [&](const std::string& message) { reporter.info(segment(), message); },
        lock, dest);
}

std::shared_ptr<segment::Fixer> Checker::fixer()
{
    return std::make_shared<Fixer>(
        std::static_pointer_cast<scan::Checker>(shared_from_this()),
        lock->write_lock());
}

/*
 * Fixer
 */

Fixer::Fixer(std::shared_ptr<scan::Checker> checker,
             std::shared_ptr<core::CheckWriteLock> lock)
    : segment::Fixer(checker, lock)
{
}

Fixer::MarkRemovedResult Fixer::mark_removed(const std::set<uint64_t>& offsets)
{
    MarkRemovedResult res;
    // Load current metadata
    auto mds  = checker().scan();
    // Remove matching offsets
    mds       = mds.without_data(offsets);
    auto rres = reorder(mds, segment::RepackConfig());

    res.segment_mtime = rres.segment_mtime;
    // TODO: add a Collection method to compute data timespan without building
    //       a summary
    Summary summary;
    mds.add_to_summary(summary);
    res.data_timespan = summary.get_reference_time();
    return res;
}

Fixer::ReorderResult Fixer::reorder(arki::metadata::Collection& mds,
                                    const segment::RepackConfig& repack_config)
{
    ReorderResult res;
    res.size_pre      = checker().data->size();
    // Write out the data with the new order
    auto data_checker = checker().data->checker();
    auto p_repack     = data_checker->repack(mds, repack_config);
    p_repack.commit();
    res.size_post     = checker().data->size();
    res.segment_mtime = get_data_mtime_after_fix("reorder");
    return res;
}

size_t Fixer::remove(bool with_data)
{
    size_t res = 0;
    if (!with_data)
        return res;
    return res + remove_data();
}

size_t Fixer::remove_data()
{
    auto data_checker = checker().data->checker();
    return data_checker->remove();
}

Fixer::ConvertResult Fixer::tar()
{
    ConvertResult res;
    if (std::filesystem::exists(sys::with_suffix(segment().abspath(), ".tar")))
    {
        auto ts = checker().data->timestamp();
        if (!ts)
        {
            std::stringstream buf;
            buf << segment().abspath()
                << ": tar segment already exists but cannot be accessed";
            throw std::runtime_error(buf.str());
        }
        res.segment_mtime = ts.value();
        return res;
    }
    res.size_pre = checker().data->size();

    auto data_checker = checker().data->checker();

    // Rescan file and sort for repacking
    auto mds = checker().scan();
    mds.sort_segment();

    // Create the .tar segment
    auto new_data_checker = data_checker->tar(mds);
    res.size_post         = new_data_checker->data().size();

    checker().update_data();
    res.segment_mtime = get_data_mtime_after_fix("conversion to tar");

    return res;
}

Fixer::ConvertResult Fixer::zip()
{
    ConvertResult res;
    if (std::filesystem::exists(sys::with_suffix(segment().abspath(), ".zip")))
    {
        auto ts = checker().data->timestamp();
        if (!ts)
        {
            std::stringstream buf;
            buf << segment().abspath()
                << ": zip segment already exists but cannot be accessed";
            throw std::runtime_error(buf.str());
        }
        res.segment_mtime = ts.value();
        return res;
    }
    res.size_pre = checker().data->size();

    auto data_checker = checker().data->checker();

    // Rescan file and sort for repacking
    auto mds = checker().scan();
    mds.sort_segment();

    // Create the .zip segment
    auto new_data_checker = data_checker->zip(mds);
    res.size_post         = new_data_checker->data().size();

    checker().update_data();
    res.segment_mtime = get_data_mtime_after_fix("conversion to zip");

    return res;
}

Fixer::ConvertResult Fixer::compress(unsigned groupsize)
{
    ConvertResult res;
    if (std::filesystem::exists(sys::with_suffix(segment().abspath(), ".gz")) or
        std::filesystem::exists(
            sys::with_suffix(segment().abspath(), ".gz.idx")))
    {
        auto ts = checker().data->timestamp();
        if (!ts)
        {
            std::stringstream buf;
            buf << segment().abspath()
                << ": gz segment already exists but cannot be accessed";
            throw std::runtime_error(buf.str());
        }
        res.segment_mtime = ts.value();
        return res;
    }
    res.size_pre = checker().data->size();

    auto data_checker = checker().data->checker();

    // Rescan file and sort for repacking
    auto mds = checker().scan();
    mds.sort_segment();

    // Create the .zip segment
    auto new_data_checker = data_checker->compress(mds, groupsize);
    res.size_post         = new_data_checker->data().size();

    checker().update_data();
    res.segment_mtime = get_data_mtime_after_fix("conversion to gz");

    return res;
}

void Fixer::reindex(arki::metadata::Collection&)
{
    // Nothing to do, since we do not have an index
}

void Fixer::move(std::shared_ptr<arki::Segment> dest)
{
    auto data_checker = checker().data->checker();
    data_checker->move(dest->session().shared_from_this(), dest->relpath());
}

void Fixer::move_data(std::shared_ptr<arki::Segment> dest)
{
    auto data_checker = checker().data->checker();
    data_checker->move(dest->session().shared_from_this(), dest->relpath());
}

void Fixer::test_mark_all_removed()
{
    // Nothing to do, since we do not have an index
}

void Fixer::test_corrupt_data(unsigned data_idx)
{
    arki::metadata::Collection mds = checker().scan();
    checker().data->checker()->test_corrupt(mds, data_idx);
}

void Fixer::test_truncate_data(unsigned data_idx)
{
    arki::metadata::Collection mds = checker().scan();
    const auto& s                  = mds[data_idx].sourceBlob();
    auto pft                       = checker().data->preserve_mtime();
    checker().data->checker()->test_truncate(s.offset);
}

void Fixer::test_touch_contents(time_t timestamp)
{
    checker().data->checker()->test_touch_contents(timestamp);
}

void Fixer::test_make_overlap(unsigned overlap_size, unsigned data_idx)
{
    auto mds          = checker().scan();
    auto data_checker = checker().data->checker();
    data_checker->test_make_overlap(mds, overlap_size, data_idx);
}

void Fixer::test_make_hole(unsigned hole_size, unsigned data_idx)
{
    auto mds          = checker().scan();
    auto data_checker = checker().data->checker();
    data_checker->test_make_hole(mds, hole_size, data_idx);
}

arki::metadata::Collection
Fixer::test_change_metadata(std::shared_ptr<Metadata> md, unsigned data_idx)
{
    auto pmt = checker().data->preserve_mtime();

    arki::metadata::Collection mds = m_checker->scan();
    md->set_source(
        std::unique_ptr<arki::types::Source>(mds[data_idx].source().clone()));
    md->sourceBlob().unlock();
    mds.replace(data_idx, md);

    reindex(mds);
    return mds;
}

void Fixer::test_swap_data(unsigned d1_idx, unsigned d2_idx,
                           const segment::RepackConfig& repack_config)
{
    arki::metadata::Collection mds = m_checker->scan();
    mds.swap(d1_idx, d2_idx);

    auto pmt = checker().data->preserve_mtime();
    reorder(mds, repack_config);
}

} // namespace arki::segment::scan
