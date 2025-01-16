#ifndef ARKI_SEGMENT_ISEG_H
#define ARKI_SEGMENT_ISEG_H

#include <arki/segment.h>
#include <arki/segment/iseg/fwd.h>

namespace arki::segment::iseg {

class Segment: public arki::Segment
{
public:
    using arki::Segment::Segment;

    std::shared_ptr<RIndex> read_index(std::shared_ptr<const core::ReadLock> lock) const;
};

class Reader : public segment::Reader
{
    std::shared_ptr<RIndex> m_index;

public:
    Reader(std::shared_ptr<const iseg::Segment> segment, std::shared_ptr<const core::ReadLock> lock);

    bool query_data(const query::Data& q, metadata_dest_func dest) override;
    void query_summary(const Matcher& matcher, Summary& summary) override;
    // core::Interval get_stored_time_interval() override;
};

}

#endif
