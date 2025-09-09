#ifndef ARKI_SEGMENT_SCAN_H
#define ARKI_SEGMENT_SCAN_H

#include <arki/segment.h>
#include <arki/types/fwd.h>

namespace arki::segment::scan {

class Reader : public segment::Reader
{
    std::shared_ptr<segment::data::Reader> data_reader;

public:
    Reader(std::shared_ptr<const Segment> segment,
           std::shared_ptr<const core::ReadLock> lock);
    ~Reader();

    std::vector<uint8_t> read(const types::source::Blob& src) override;
    stream::SendResult stream(const types::source::Blob& src,
                              StreamOutput& out) override;
    bool read_all(metadata_dest_func dest) override;
    bool query_data(const query::Data& q, metadata_dest_func dest) override;
    void query_summary(const Matcher& matcher, Summary& summary) override;
};

class Writer : public segment::Writer
{
public:
    using segment::Writer::Writer;
    ~Writer();

    AcquireResult acquire(arki::metadata::InboundBatch& batch,
                          const WriterConfig& config) override;
};

class Checker : public segment::Checker
{
public:
    using segment::Checker::Checker;

    arki::metadata::Collection scan() override;
    FsckResult fsck(segment::Reporter& reporter, bool quick = true) override;

    std::shared_ptr<segment::Fixer> fixer() override;
};

class Fixer : public segment::Fixer
{
public:
    using segment::Fixer::Fixer;

    MarkRemovedResult mark_removed(const std::set<uint64_t>& offsets) override;
    ReorderResult reorder(arki::metadata::Collection& mds,
                          const segment::RepackConfig& repack_config) override;
    size_t remove(bool with_data) override;
    ConvertResult tar() override;
    ConvertResult zip() override;
    ConvertResult compress(unsigned groupsize) override;
    void reindex(arki::metadata::Collection& mds) override;
    void test_mark_all_removed() override;
    void test_make_overlap(unsigned overlap_size,
                           unsigned data_idx = 1) override;
    void test_make_hole(unsigned hole_size, unsigned data_idx = 0) override;
};

} // namespace arki::segment::scan

#endif
