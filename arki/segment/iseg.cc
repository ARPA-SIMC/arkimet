#include "iseg.h"
#include "iseg/session.h"
#include "iseg/index.h"
#include "data.h"
#include "arki/types/source/blob.h"
#include "arki/metadata.h"
#include "arki/metadata/collection.h"
#include "arki/nag.h"
#include "arki/utils/sys.h"

using namespace arki::utils;

namespace arki::segment::iseg {

std::shared_ptr<RIndex> Segment::read_index(std::shared_ptr<const core::ReadLock> lock) const
{
    return std::static_pointer_cast<const iseg::Session>(m_session)->read_index(shared_from_this(), lock);
}

std::shared_ptr<CIndex> Segment::check_index(std::shared_ptr<core::CheckLock> lock) const
{
    return std::static_pointer_cast<const iseg::Session>(m_session)->check_index(shared_from_this(), lock);
}

Reader::Reader(std::shared_ptr<const iseg::Segment> segment, std::shared_ptr<const core::ReadLock> lock)
    : segment::Reader(segment, lock),
      m_index(segment->read_index(lock))
{
}

bool Reader::read_all(metadata_dest_func dest)
{
    return m_index->scan(dest);
}

bool Reader::query_data(const query::Data& q, metadata_dest_func dest)
{
    std::shared_ptr<arki::segment::data::Reader> reader;
    if (q.with_data)
        reader = m_segment->session().segment_data_reader(m_segment, lock);

    auto mdbuf = m_index->query_data(q.matcher, reader);

    // Sort and output the rest
    if (q.sorter) mdbuf.sort(*q.sorter);

    // pass it to consumer
    return mdbuf.move_to(dest);
}

void Reader::query_summary(const Matcher& matcher, Summary& summary)
{
    m_index->query_summary_from_db(matcher, summary);
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
    arki::metadata::Collection res;
    index().scan(res.inserter_func(), "offset");
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
    auto data_checker = data().checker(false);
    auto pending_repack = data_checker->repack(mds, repack_config);

    // Reindex mds
    index.reset();
    for (const auto& md: mds)
    {
        const auto& source = md->sourceBlob();
        if (index.index(*md, source.offset))
            throw std::runtime_error("duplicate detected while reordering segment");
    }

    res.size_pre = data_checker->size();

    // Commit the changes in the file system
    pending_repack.commit();

    // Commit the changes in the database
    pending_index.commit();

    res.size_post = data_checker->size();
    res.segment_mtime = get_data_mtime_after_fix("reorder");
    return res;
}

size_t Fixer::remove(bool with_data)
{
    size_t res = 0;
    res += remove_ifexists(segment().abspath_iseg_index());

    if (!with_data)
        return res;

    auto data_checker = data().checker(false);
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

    auto& index = checker().index();
    auto data_checker = data().checker(false);
    res.size_pre = data_checker->size();

    auto pending_index = index.begin_transaction();

    // Rescan file and sort for repacking
    auto mds = checker().scan();
    mds.sort();

    // Create the .tar segment
    auto new_data_checker = data_checker->tar(mds);
    res.size_post = new_data_checker->size();

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

    auto& index = checker().index();
    auto data_checker = data().checker(false);
    res.size_pre = data_checker->size();

    auto pending_index = index.begin_transaction();

    // Rescan file and sort for repacking
    auto mds = checker().scan();
    mds.sort();

    // Create the .zip segment
    auto new_data_checker = data_checker->zip(mds);
    res.size_post = new_data_checker->size();

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

    auto& index = checker().index();
    auto data_checker = data().checker(false);
    res.size_pre = data_checker->size();

    auto pending_index = index.begin_transaction();

    // Rescan file and sort for repacking
    auto mds = checker().scan();
    mds.sort();

    // Create the .zip segment
    auto new_data_checker = data_checker->compress(mds, groupsize);
    res.size_post = new_data_checker->size();

    // Reindex the new metadata
    index.reindex(mds);

    // Commit the changes in the database
    pending_index.commit();

    checker().update_data();
    res.segment_mtime = get_data_mtime_after_fix("conversion to gz");
    return res;
}

}
