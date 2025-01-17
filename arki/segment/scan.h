#ifndef ARKI_SEGMENT_SCAN_H
#define ARKI_SEGMENT_SCAN_H

#include <arki/types/fwd.h>
#include <arki/segment.h>

namespace arki::segment::scan {

class Reader : public segment::Reader
{
public:
    using segment::Reader::Reader;
    ~Reader();

    bool read_all(metadata_dest_func dest) override;
    bool query_data(const query::Data& q, metadata_dest_func dest) override;
    void query_summary(const Matcher& matcher, Summary& summary) override;
};

}

#endif
