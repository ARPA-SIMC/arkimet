#ifndef ARKI_SEGMENT_METADATA_H
#define ARKI_SEGMENT_METADATA_H

#include <arki/types/fwd.h>
#include <arki/segment/fwd.h>
#include <arki/segment.h>

namespace arki::segment::metadata {

class Index
{
    const Segment& segment;
    std::filesystem::path md_path;

public:
    explicit Index(const Segment& segment, const std::filesystem::path& md_path);

    arki::metadata::Collection query_data(const Matcher& matcher, std::shared_ptr<arki::segment::data::Reader> reader);
    void query_summary(const Matcher& matcher, Summary& summary);
};

class Reader : public segment::Reader
{
    Index index;

public:
    Reader(std::shared_ptr<const Segment> segment, std::shared_ptr<const core::ReadLock> lock);

    bool query_data(const query::Data& q, metadata_dest_func dest) override;
    void query_summary(const Matcher& matcher, Summary& summary) override;
};

class Checker : public segment::Checker
{
public:
    using segment::Checker::Checker;
};

}

#endif
