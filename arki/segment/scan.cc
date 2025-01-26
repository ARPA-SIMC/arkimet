#include "scan.h"
#include "data.h"
#include "reporter.h"
#include "arki/core/lock.h"
#include "arki/query.h"
#include "arki/metadata.h"
#include "arki/metadata/collection.h"
#include "arki/types/source/blob.h"
#include "arki/utils/sys.h"

using namespace arki::utils;

namespace arki::segment::scan {

Reader::~Reader()
{
}

bool Reader::read_all(metadata_dest_func dest)
{
    auto reader = m_segment->session().segment_data_reader(m_segment, lock);
    return reader->scan_data(dest);
}

bool Reader::query_data(const query::Data& q, metadata_dest_func dest)
{
    arki::metadata::Collection mdbuf;

    auto reader = m_segment->session().segment_data_reader(m_segment, lock);
    reader->scan_data([&](auto md) {
        if (q.matcher(*md))
            mdbuf.acquire(md);
        if (not q.with_data)
            md->sourceBlob().unlock();
        return true;
    });

    // Sort and output the rest
    if (q.sorter) mdbuf.sort(*q.sorter);

    // pass it to consumer
    return mdbuf.move_to(dest);
}

void Reader::query_summary(const Matcher& matcher, Summary& summary)
{
    auto reader = m_segment->session().segment_data_reader(m_segment, lock);
    reader->scan_data([&](auto md) {
        if (matcher(*md))
            summary.add(*md);
        return true;
    });
}


Writer::~Writer()
{
}

Writer::AcquireResult Writer::acquire(arki::metadata::InboundBatch& batch, const WriterConfig& config)
{
    throw std::runtime_error("acquiring in scan segments is not yet implemented");
}


arki::metadata::Collection Checker::scan()
{
    // Rescan the file
    auto data_reader = m_data->reader(lock);
    arki::metadata::Collection res;
    data_reader->scan_data(res.inserter_func());
    return res;
}

Checker::FsckResult Checker::fsck(segment::Reporter& reporter, bool quick)
{
    Checker::FsckResult res;

    auto data_checker = m_data->checker();
    auto ts_data = m_data->timestamp();
    if (!ts_data)
    {
        reporter.info(segment(), "segment data not found on disk");
        res.state = SEGMENT_MISSING;
        return res;
    }
    res.mtime = ts_data.value();
    res.size = data().size();

    auto mds = scan();

    if (mds.empty())
    {
        reporter.info(segment(), "the segment is fully deleted");
        res.state += SEGMENT_DELETED;
    } else {
        // Compute the span of reftimes inside the segment
        mds.sort_segment();
        if (!mds.expand_date_range(res.interval))
        {
            reporter.info(segment(), "segment contains data without reference time information");
            res.state += SEGMENT_CORRUPTED;
        } else {
            res.state += data_checker->check([&](const std::string& msg) { reporter.info(segment(), msg); }, mds, quick);
        }
    }

    return res;
}

std::shared_ptr<segment::Fixer> Checker::fixer()
{
    return std::make_shared<Fixer>(shared_from_this(), lock->write_lock());
}

Fixer::MarkRemovedResult Fixer::mark_removed(const std::set<uint64_t>& offsets)
{
    MarkRemovedResult res;
    // Load current metadata
    auto mds = checker().scan();
    // Remove matching offsets
    mds = mds.without_data(offsets);
    auto rres = reorder(mds, segment::data::RepackConfig());

    res.segment_mtime = rres.segment_mtime;
    // TODO: add a Collection method to compute data timespan without building
    //       a summary
    Summary summary;
    mds.add_to_summary(summary);
    res.data_timespan = summary.get_reference_time();
    return res;
}

Fixer::ReorderResult Fixer::reorder(arki::metadata::Collection& mds, const segment::data::RepackConfig& repack_config)
{
    ReorderResult res;
    res.size_pre = data().size();
    // Write out the data with the new order
    auto data_checker = data().checker();
    auto p_repack = data_checker->repack(mds, repack_config);
    p_repack.commit();
    res.size_post = data().size();
    res.segment_mtime = get_data_mtime_after_fix("reorder");
    return res;
}

size_t Fixer::remove(bool with_data)
{
    size_t res = 0;
    if (!with_data)
        return res;
    auto data_checker = data().checker();
    return res + data_checker->remove();
}

Fixer::ConvertResult Fixer::tar()
{
    ConvertResult res;
    if (std::filesystem::exists(sys::with_suffix(segment().abspath(), ".tar")))
    {
        auto ts = data().timestamp();
        if (!ts)
        {
            std::stringstream buf;
            buf << segment().abspath() << ": tar segment already exists but cannot be accessed";
            throw std::runtime_error(buf.str());
        }
        res.segment_mtime = ts.value();
        return res;
    }
    res.size_pre = data().size();

    auto data_checker = data().checker();

    // Rescan file and sort for repacking
    auto mds = checker().scan();
    mds.sort_segment();

    // Create the .tar segment
    auto new_data_checker = data_checker->tar(mds);
    res.size_post = new_data_checker->data().size();

    checker().update_data();
    res.segment_mtime = get_data_mtime_after_fix("conversion to tar");

    return res;
}

Fixer::ConvertResult Fixer::zip()
{
    ConvertResult res;
    if (std::filesystem::exists(sys::with_suffix(segment().abspath(), ".zip")))
    {
        auto ts = data().timestamp();
        if (!ts)
        {
            std::stringstream buf;
            buf << segment().abspath() << ": zip segment already exists but cannot be accessed";
            throw std::runtime_error(buf.str());
        }
        res.segment_mtime = ts.value();
        return res;
    }
    res.size_pre = data().size();

    auto data_checker = data().checker();

    // Rescan file and sort for repacking
    auto mds = checker().scan();
    mds.sort_segment();

    // Create the .zip segment
    auto new_data_checker = data_checker->zip(mds);
    res.size_post = new_data_checker->data().size();

    checker().update_data();
    res.segment_mtime = get_data_mtime_after_fix("conversion to zip");

    return res;
}

Fixer::ConvertResult Fixer::compress(unsigned groupsize)
{
    ConvertResult res;
    if (std::filesystem::exists(sys::with_suffix(segment().abspath(), ".gz"))
                or std::filesystem::exists(sys::with_suffix(segment().abspath(), ".gz.idx")))
    {
        auto ts = data().timestamp();
        if (!ts)
        {
            std::stringstream buf;
            buf << segment().abspath() << ": gz segment already exists but cannot be accessed";
            throw std::runtime_error(buf.str());
        }
        res.segment_mtime = ts.value();
        return res;
    }
    res.size_pre = data().size();

    auto data_checker = data().checker();

    // Rescan file and sort for repacking
    auto mds = checker().scan();
    mds.sort_segment();

    // Create the .zip segment
    auto new_data_checker = data_checker->compress(mds, groupsize);
    res.size_post = new_data_checker->data().size();

    checker().update_data();
    res.segment_mtime = get_data_mtime_after_fix("conversion to gz");

    return res;
}

void Fixer::reindex(arki::metadata::Collection&)
{
    // Nothing to do, since we do not have an index
}

void Fixer::test_mark_all_removed()
{
    // Nothing to do, since we do not have an index
}

void Fixer::test_make_overlap(unsigned overlap_size, unsigned data_idx)
{
    auto mds = checker().scan();
    auto data_checker = data().checker();
    data_checker->test_make_overlap(mds, overlap_size, data_idx);
}

void Fixer::test_make_hole(unsigned hole_size, unsigned data_idx)
{
    auto mds = checker().scan();
    auto data_checker = data().checker();
    data_checker->test_make_hole(mds, hole_size, data_idx);
}

}
