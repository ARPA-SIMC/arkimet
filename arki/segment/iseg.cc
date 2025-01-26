#include "iseg.h"
#include "iseg/session.h"
#include "iseg/index.h"
#include "reporter.h"
#include "data.h"
#include "arki/types/source/blob.h"
#include "arki/metadata.h"
#include "arki/metadata/collection.h"
#include "arki/metadata/inbound.h"
#include "arki/scan.h"
#include "arki/nag.h"
#include "arki/utils/sys.h"

using namespace arki::utils;

namespace arki::segment::iseg {

std::shared_ptr<RIndex> Segment::read_index(std::shared_ptr<const core::ReadLock> lock) const
{
    return std::make_shared<RIndex>(std::static_pointer_cast<const iseg::Segment>(shared_from_this()), lock);
}

std::shared_ptr<AIndex> Segment::append_index(std::shared_ptr<core::AppendLock> lock) const
{
    return std::make_shared<AIndex>(std::static_pointer_cast<const iseg::Segment>(shared_from_this()), lock);
}

std::shared_ptr<CIndex> Segment::check_index(std::shared_ptr<core::CheckLock> lock) const
{
    return std::make_shared<CIndex>(std::static_pointer_cast<const iseg::Segment>(shared_from_this()), lock);
}

Reader::Reader(std::shared_ptr<const iseg::Segment> segment, std::shared_ptr<const core::ReadLock> lock)
    : segment::Reader(segment, lock),
      m_index(segment->read_index(lock))
{
}

bool Reader::read_all(metadata_dest_func dest)
{
    auto reader = m_segment->session().segment_data_reader(m_segment, lock);
    return m_index->scan([&](auto md) {
        md->sourceBlob().lock(reader);
        return dest(md);
    });
}

bool Reader::query_data(const query::Data& q, metadata_dest_func dest)
{
    auto mdbuf = m_index->query_data(q.matcher);

    // TODO: if using smallfiles there is no need to lock the source
    if (q.with_data)
    {
        auto reader = m_segment->session().segment_data_reader(m_segment, lock);
        for (auto& md: mdbuf)
            md->sourceBlob().lock(reader);
    }

    // Sort and output the rest
    if (q.sorter) mdbuf.sort(*q.sorter);

    // pass it to consumer
    return mdbuf.move_to(dest);
}

void Reader::query_summary(const Matcher& matcher, Summary& summary)
{
    m_index->query_summary_from_db(matcher, summary);
}


Writer::Writer(std::shared_ptr<const Segment> segment, std::shared_ptr<core::AppendLock> lock)
    : segment::Writer(segment, lock), index(segment->append_index(lock))
{
}

Writer::~Writer()
{
}

Writer::AcquireResult Writer::acquire_batch_replace_never(arki::metadata::InboundBatch& batch, const WriterConfig& config)
{
    AcquireResult res;
    auto data_writer = segment().session().segment_data_writer(m_segment, config);
    core::Pending p_idx = index->begin_transaction();

    try {
        for (auto& e: batch)
        {
            e->destination.clear();

            if (std::unique_ptr<types::source::Blob> old = index->index(*e->md, data_writer->next_offset()))
            {
                e->md->add_note("Failed to store in '" + config.destination_name + "' because the data already exists in " + segment().relpath().native() + ":" + std::to_string(old->offset));
                e->result = arki::metadata::Inbound::Result::DUPLICATE;
                ++res.count_failed;
            } else {
                data_writer->append(*e->md);
                e->result = arki::metadata::Inbound::Result::OK;
                e->destination = config.destination_name;
                ++res.count_ok;
            }
        }
    } catch (std::exception& e) {
        // sqlite will take care of transaction consistency
        batch.set_all_error("Failed to store in '" + config.destination_name + "': " + e.what());
        res.count_ok = 0;
        res.count_failed = batch.size();
        return res;
    }

    data_writer->commit();
    p_idx.commit();
    return res;
}

Writer::AcquireResult Writer::acquire_batch_replace_always(arki::metadata::InboundBatch& batch, const WriterConfig& config)
{
    AcquireResult res;
    auto data_writer = segment().session().segment_data_writer(m_segment, config);
    core::Pending p_idx = index->begin_transaction();

    try {
        for (auto& e: batch)
        {
            e->destination.clear();
            index->replace(*e->md, data_writer->next_offset());
            data_writer->append(*e->md);
            e->result = arki::metadata::Inbound::Result::OK;
            e->destination = config.destination_name;
            ++res.count_ok;
        }
    } catch (std::exception& e) {
        // sqlite will take care of transaction consistency
        batch.set_all_error("Failed to store in '" + config.destination_name + "': " + e.what());
        res.count_ok = 0;
        res.count_failed = batch.size();
        return res;
    }

    data_writer->commit();
    p_idx.commit();
    return res;
}

Writer::AcquireResult Writer::acquire_batch_replace_higher_usn(arki::metadata::InboundBatch& batch, const WriterConfig& config)
{
    AcquireResult res;
    auto data_writer = segment().session().segment_data_writer(m_segment, config);
    core::Pending p_idx = index->begin_transaction();

    try {
        for (auto& e: batch)
        {
            e->destination.clear();

            // Try to acquire without replacing
            if (std::unique_ptr<types::source::Blob> old = index->index(*e->md, data_writer->next_offset()))
            {
                // Duplicate detected

                // Read the update sequence number of the new BUFR
                int new_usn;
                if (!arki::scan::Scanner::update_sequence_number(*e->md, new_usn))
                {
                    e->md->add_note("Failed to store in '" + config.destination_name + "' because the dataset already has the data in " + segment().relpath().native() + ":" + std::to_string(old->offset) + " and there is no Update Sequence Number to compare");
                    e->result = arki::metadata::Inbound::Result::DUPLICATE;
                    ++res.count_failed;
                    continue;
                }

                // Read the update sequence number of the old BUFR
                auto reader = segment().data_reader(lock);
                old->lock(reader);
                int old_usn;
                if (!arki::scan::Scanner::update_sequence_number(*old, old_usn))
                {
                    e->md->add_note("Failed to store in '" + config.destination_name + "': a similar element exists, the new element has an Update Sequence Number but the old one does not, so they cannot be compared");
                    e->result = arki::metadata::Inbound::Result::ERROR;
                    ++res.count_failed;
                    continue;
                }

                // If the new element has no higher Update Sequence Number, report a duplicate
                if (old_usn > new_usn)
                {
                    e->md->add_note("Failed to store in '" + config.destination_name + "' because the dataset already has the data in " + segment().relpath().native() + ":" + std::to_string(old->offset) + " with a higher Update Sequence Number");
                    e->result = arki::metadata::Inbound::Result::DUPLICATE;
                    ++res.count_failed;
                    continue;
                }

                // Replace, reusing the pending datafile transaction from earlier
                index->replace(*e->md, data_writer->next_offset());
            }
            data_writer->append(*e->md);
            e->result = arki::metadata::Inbound::Result::OK;
            e->destination = config.destination_name;
            ++res.count_ok;
        }
    } catch (std::exception& e) {
        // sqlite will take care of transaction consistency
        batch.set_all_error("Failed to store in dataset '" + config.destination_name + "': " + e.what());
        res.count_ok = 0;
        res.count_failed = batch.size();
        return res;
    }

    data_writer->commit();
    p_idx.commit();
    return res;
}

Writer::AcquireResult Writer::acquire(arki::metadata::InboundBatch& batch, const WriterConfig& config)
{
    AcquireResult res;
    switch (config.replace_strategy)
    {
        case ReplaceStrategy::DEFAULT:
        case ReplaceStrategy::NEVER:
            res = acquire_batch_replace_never(batch, config);
            break;
        case ReplaceStrategy::ALWAYS:
            res = acquire_batch_replace_always(batch, config);
            break;
        case ReplaceStrategy::HIGHER_USN:
            res = acquire_batch_replace_higher_usn(batch, config);
            break;
        default:
        {
            std::stringstream buf;
            buf << "programming error: unsupported replace value " << config.replace_strategy << " for " << config.destination_name;
            throw std::runtime_error(buf.str());
        }
    }

    res.segment_mtime = segment().data()->timestamp().value_or(0);
    res.data_timespan = index->query_data_timespan();
    return res;
}


Checker::Checker(std::shared_ptr<const Segment> segment, std::shared_ptr<core::CheckLock> lock)
    : segment::Checker(segment, lock)
{
}

CIndex& Checker::index()
{
    if (!m_index)
        m_index = std::static_pointer_cast<const Segment>(m_segment)->check_index(lock);
    return *m_index;
}

arki::metadata::Collection Checker::scan()
{
    auto reader = segment().session().segment_data_reader(m_segment, lock);

    arki::metadata::Collection res;
    index().scan([&](auto md) {
        md->sourceBlob().lock(reader);
        res.acquire(md);
        return true;
    }, "offset");
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

    if (!std::filesystem::exists(segment().abspath_iseg_index()))
    {
        if (data().is_empty())
        {
            reporter.info(segment(), "empty segment found on disk with no associated index");
            res.state = SEGMENT_DELETED;
        } else {
            reporter.info(segment(), "segment found on disk with no associated index");
            res.state = SEGMENT_UNALIGNED;
        }
        return res;
    }

#if 0
    /**
     * Although iseg could detect if the data of a segment is newer than its
     * index, the timestamp of the index is updated by various kinds of sqlite
     * operations, making the test rather useless, because it's likely that the
     * index timestamp would get updated before the mismatch is detected.
     */
    string abspath = str::joinpath(dataset().path, relpath);
    if (sys::timestamp(abspath) > sys::timestamp(abspath + ".index"))
    {
        segments_state.insert(make_pair(relpath, segmented::SegmentState(segment::SEGMENT_UNALIGNED)));
        return;
    }
#endif

    auto mds = scan();

    if (mds.empty())
    {
        reporter.info(segment(), "index reports that the segment is fully deleted");
        res.state += SEGMENT_DELETED;
    } else {
        // Compute the span of reftimes inside the segment
        mds.sort_segment();
        if (!mds.expand_date_range(res.interval))
        {
            reporter.info(segment(), "index contains data for this segment but no reference time information");
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

    auto& index = checker().index();
    auto pending_del = index.begin_transaction();

    for (const auto& offset: offsets)
        index.remove(offset);

    pending_del.commit();

    res.segment_mtime = get_data_mtime_after_fix("removal in metadata");
    res.data_timespan = index.query_data_timespan();
    return res;
}

Fixer::ReorderResult Fixer::reorder(arki::metadata::Collection& mds, const segment::data::RepackConfig& repack_config)
{
    ReorderResult res;
    auto& index = checker().index();
    auto pending_index = index.begin_transaction();

    // Make a copy of the file with the data in it ordered as mds is ordered,
    // and update the offsets in the index
    auto data_checker = data().checker();
    auto pending_repack = data_checker->repack(mds, repack_config);

    // Reindex mds
    index.reset();
    for (const auto& md: mds)
    {
        const auto& source = md->sourceBlob();
        if (index.index(*md, source.offset))
            throw std::runtime_error("duplicate detected while reordering segment");
    }
    res.size_pre = data().size();

    // Commit the changes in the file system
    pending_repack.commit();

    // Commit the changes in the database
    pending_index.commit();

    index.vacuum();

    res.segment_mtime = get_data_mtime_after_fix("reorder");
    res.size_post = data().size();
    return res;
}

size_t Fixer::remove(bool with_data)
{
    size_t res = 0;
    res += remove_ifexists(segment().abspath_iseg_index());

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

    auto& index = checker().index();
    auto data_checker = data().checker();
    auto pending_index = index.begin_transaction();

    // Rescan file and sort for repacking
    auto mds = checker().scan();
    mds.sort_segment();

    // Create the .tar segment
    auto new_data_checker = data_checker->tar(mds);
    res.size_post = new_data_checker->data().size();

    // Reindex the new metadata
    index.reindex(mds);

    // Commit the changes in the database
    pending_index.commit();

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

    auto& index = checker().index();
    auto data_checker = data().checker();
    auto pending_index = index.begin_transaction();

    // Rescan file and sort for repacking
    auto mds = checker().scan();
    mds.sort_segment();

    // Create the .zip segment
    auto new_data_checker = data_checker->zip(mds);
    res.size_post = new_data_checker->data().size();

    // Reindex the new metadata
    index.reindex(mds);

    // Commit the changes in the database
    pending_index.commit();

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

    auto& index = checker().index();
    auto data_checker = data().checker();
    auto pending_index = index.begin_transaction();

    // Rescan file and sort for repacking
    auto mds = checker().scan();
    mds.sort_segment();

    // Create the .zip segment
    auto new_data_checker = data_checker->compress(mds, groupsize);
    res.size_post = new_data_checker->data().size();

    // Reindex the new metadata
    index.reindex(mds);

    // Commit the changes in the database
    pending_index.commit();

    checker().update_data();
    res.segment_mtime = get_data_mtime_after_fix("conversion to gz");
    return res;
}

void Fixer::reindex(arki::metadata::Collection& mds)
{
    auto& index = checker().index();
    auto pending_index = index.begin_transaction();
    index.reindex(mds);
    pending_index.commit();
    index.vacuum();
}

void Fixer::move(std::shared_ptr<arki::Segment> dest)
{
    segment::Fixer::move(dest);
    sys::rename_ifexists(segment().abspath_iseg_index(), dest->abspath_iseg_index());
}

void Fixer::test_touch_contents(time_t timestamp)
{
    segment::Fixer::test_touch_contents(timestamp);
    sys::touch_ifexists(segment().abspath_iseg_index(), timestamp);
}

void Fixer::test_mark_all_removed()
{
    auto& index = checker().index();
    index.reset();
}

void Fixer::test_make_overlap(unsigned overlap_size, unsigned data_idx)
{
    auto mds = checker().scan();
    auto data_checker = data().checker();
    data_checker->test_make_overlap(mds, overlap_size, data_idx);
    checker().index().test_make_overlap(overlap_size, data_idx);
}

void Fixer::test_make_hole(unsigned hole_size, unsigned data_idx)
{
    auto mds = checker().scan();
    auto data_checker = data().checker();
    data_checker->test_make_hole(mds, hole_size, data_idx);
    checker().index().test_make_hole(hole_size, data_idx);
}

}
