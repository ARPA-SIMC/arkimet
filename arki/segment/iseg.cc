#include "iseg.h"
#include "iseg/session.h"
#include "iseg/index.h"

namespace arki::segment::iseg {

std::shared_ptr<RIndex> Segment::read_index(std::shared_ptr<core::ReadLock> lock) const
{
    return std::make_shared<RIndex>(std::static_pointer_cast<const iseg::Session>(m_session), relpath(), lock);
}

std::shared_ptr<segment::Reader> Segment::reader(std::shared_ptr<core::ReadLock> lock) const
{
    return std::make_shared<Reader>(std::static_pointer_cast<const iseg::Segment>(shared_from_this()), lock);
}

Reader::Reader(std::shared_ptr<const iseg::Segment> segment, std::shared_ptr<core::ReadLock> lock)
    : segment::Reader(segment, lock),
      m_index(segment->read_index(lock))
{
}

bool Reader::query_data(const query::Data& q, metadata_dest_func dest)
{
    return m_index->query_data(q, dest);
}

}
