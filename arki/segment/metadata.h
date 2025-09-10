#ifndef ARKI_SEGMENT_METADATA_H
#define ARKI_SEGMENT_METADATA_H

#include <arki/metadata/collection.h>
#include <arki/segment.h>
#include <arki/segment/fwd.h>
#include <arki/types/fwd.h>

namespace arki::segment::metadata {

class Index
{
    const Segment& segment;
    std::filesystem::path md_path;

public:
    explicit Index(const Segment& segment);

    bool read_all(std::shared_ptr<arki::segment::Reader> reader,
                  metadata_dest_func dest);
    arki::metadata::Collection
    query_data(const Matcher& matcher,
               std::shared_ptr<arki::segment::Reader> reader);
    void query_summary(const Matcher& matcher, Summary& summary);
};

class Reader : public segment::Reader
{
    std::shared_ptr<segment::data::Reader> data_reader;
    Index index;

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
protected:
    arki::metadata::Collection mds;
    Summary sum;

    std::shared_ptr<segment::data::Writer>
    get_data_writer(const segment::WriterConfig& config) const;

    void add(const Metadata& md, const types::source::Blob& source);
    void write_metadata();

public:
    Writer(std::shared_ptr<const Segment> segment,
           std::shared_ptr<core::AppendLock> lock);
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
    bool scan_data(segment::Reporter& reporter,
                   metadata_dest_func dest) override;
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
    size_t remove_data() override;
    ConvertResult tar() override;
    ConvertResult zip() override;
    ConvertResult compress(unsigned groupsize) override;
    void reindex(arki::metadata::Collection& mds) override;
    void move(std::shared_ptr<arki::Segment> dest) override;
    void move_data(std::shared_ptr<arki::Segment> dest) override;
    void test_touch_contents(time_t timestamp) override;
    void test_mark_all_removed() override;
    void test_make_overlap(unsigned overlap_size,
                           unsigned data_idx = 1) override;
    void test_make_hole(unsigned hole_size, unsigned data_idx = 0) override;
};

} // namespace arki::segment::metadata

#endif
