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
    std::shared_ptr<AIndex> append_index(std::shared_ptr<core::AppendLock> lock) const;
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

class Writer : public segment::Writer
{
    std::shared_ptr<AIndex> index;

    AcquireResult acquire_batch_replace_never(arki::metadata::InboundBatch& batch, const WriterConfig& config);
    AcquireResult acquire_batch_replace_always(arki::metadata::InboundBatch& batch, const WriterConfig& config);
    AcquireResult acquire_batch_replace_higher_usn(arki::metadata::InboundBatch& batch, const WriterConfig& config);

public:
    Writer(std::shared_ptr<const Segment> segment, std::shared_ptr<core::AppendLock> lock);
    ~Writer();

    AcquireResult acquire(arki::metadata::InboundBatch& batch, const WriterConfig& config) override;
};

class Checker : public segment::Checker
{
    std::shared_ptr<CIndex> m_index;

public:
    Checker(std::shared_ptr<const Segment> segment, std::shared_ptr<core::CheckLock> lock);

    CIndex& index();
    arki::metadata::Collection scan() override;
    FsckResult fsck(segment::Reporter& reporter, bool quick=true) override;

    std::shared_ptr<segment::Fixer> fixer() override;
};

class Fixer : public segment::Fixer
{
public:
    using segment::Fixer::Fixer;

    const Checker& checker() const { return *static_cast<const Checker*>(m_checker.get()); }
    Checker& checker() { return *static_cast<Checker*>(m_checker.get()); }

    MarkRemovedResult mark_removed(const std::set<uint64_t>& offsets) override;
    ReorderResult reorder(arki::metadata::Collection& mds, const segment::data::RepackConfig& repack_config) override;
    size_t remove(bool with_data) override;
    ConvertResult tar() override;
    ConvertResult zip() override;
    ConvertResult compress(unsigned groupsize) override;
    void reindex(arki::metadata::Collection& mds) override;
    void move(std::shared_ptr<arki::Segment> dest) override;
    void test_touch_contents(time_t timestamp) override;
    void test_mark_all_removed() override;
    void test_make_overlap(unsigned overlap_size, unsigned data_idx=1) override;
    void test_make_hole(unsigned hole_size, unsigned data_idx=0) override;
};

}

#endif
