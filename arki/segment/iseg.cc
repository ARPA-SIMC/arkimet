#include "iseg.h"
#include "iseg/session.h"
#include "iseg/index.h"
#include "data.h"
#include "arki/metadata/collection.h"
#include "arki/nag.h"

namespace arki::segment::iseg {

std::shared_ptr<RIndex> Segment::read_index(std::shared_ptr<const core::ReadLock> lock) const
{
    return std::make_shared<RIndex>(std::static_pointer_cast<const iseg::Session>(m_session), relpath(), lock);
}

std::shared_ptr<CIndex> Segment::check_index(std::shared_ptr<core::CheckLock> lock) const
{
    return std::make_shared<CIndex>(std::static_pointer_cast<const iseg::Session>(m_session), relpath(), lock);
}

Reader::Reader(std::shared_ptr<const iseg::Segment> segment, std::shared_ptr<const core::ReadLock> lock)
    : segment::Reader(segment, lock),
      m_index(segment->read_index(lock))
{
}

bool Reader::query_data(const query::Data& q, metadata_dest_func dest)
{
    std::shared_ptr<arki::segment::data::Reader> reader;
    if (q.with_data)
        reader = m_segment->session().segment_data_reader(m_segment->format(), m_segment->relpath(), lock);

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


}
