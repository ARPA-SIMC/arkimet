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

class Checker : public segment::Checker
{
public:
    using segment::Checker::Checker;

    arki::metadata::Collection scan() override;

    std::shared_ptr<segment::Fixer> fixer() override;
};

class Fixer : public segment::Fixer
{
public:
    using segment::Fixer::Fixer;

    MarkRemovedResult mark_removed(const std::set<uint64_t>& offsets) override;
    ReorderResult reorder(arki::metadata::Collection& mds, const segment::data::RepackConfig& repack_config) override;
    size_t remove(bool with_data) override;
    ConvertResult tar() override;
    ConvertResult zip() override;
    ConvertResult compress(unsigned groupsize) override;
    void reindex(arki::metadata::Collection& mds) override;
};

}

#endif
