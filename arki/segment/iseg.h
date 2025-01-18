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
    std::shared_ptr<CIndex> check_index(std::shared_ptr<core::CheckLock> lock) const;
};

class Reader : public segment::Reader
{
    std::shared_ptr<RIndex> m_index;

public:
    Reader(std::shared_ptr<const iseg::Segment> segment, std::shared_ptr<const core::ReadLock> lock);

    bool read_all(metadata_dest_func dest) override;
    bool query_data(const query::Data& q, metadata_dest_func dest) override;
    void query_summary(const Matcher& matcher, Summary& summary) override;
    // core::Interval get_stored_time_interval() override;
};

class Checker : public segment::Checker
{
    std::shared_ptr<CIndex> m_index;

public:
    Checker(std::shared_ptr<const Segment> segment, std::shared_ptr<core::CheckLock> lock);

    CIndex& index();
    arki::metadata::Collection scan() override;

    std::shared_ptr<segment::Fixer> fixer() override;
};

class Fixer : public segment::Fixer
{
public:
    using segment::Fixer::Fixer;

    const Checker& checker() const { return *static_cast<const Checker*>(m_checker.get()); }
    Checker& checker() { return *static_cast<Checker*>(m_checker.get()); }

    void mark_removed(const std::set<uint64_t>& offsets) override;
    ReorderResult reorder(arki::metadata::Collection& mds, const segment::data::RepackConfig& repack_config) override;
    size_t remove(bool with_data) override;
    ConvertResult tar() override;
    ConvertResult zip() override;
    ConvertResult compress(unsigned groupsize) override;
};

}

#endif
