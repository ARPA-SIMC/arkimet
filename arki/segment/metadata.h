#ifndef ARKI_SEGMENT_METADATA_H
#define ARKI_SEGMENT_METADATA_H

#include <arki/metadata/collection.h>
#include <arki/segment.h>
#include <arki/segment/fwd.h>
#include <arki/types/fwd.h>

namespace arki::segment::metadata {

/**
 * Segment session that only creates metadata segments
 */
struct Session : public segment::Session
{
protected:
    std::shared_ptr<segment::Reader> create_segment_reader(
        std::shared_ptr<const Segment> segment,
        std::shared_ptr<const core::ReadLock> lock) const override;

public:
    using segment::Session::Session;

    std::shared_ptr<segment::Writer>
    segment_writer(std::shared_ptr<const Segment> segment,
                   std::shared_ptr<core::AppendLock> lock) const override;
    std::shared_ptr<segment::Checker>
    segment_checker(std::shared_ptr<const Segment> segment,
                    std::shared_ptr<core::CheckLock> lock) const override;
};

/**
 * Check if the segment has a valid associated metadata
 */
bool has_valid_metadata(std::shared_ptr<const Segment> segment);

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
    std::shared_ptr<segment::Data> data;
    std::shared_ptr<segment::data::Reader> data_reader;
    Index index;

public:
    Reader(std::shared_ptr<const Segment> segment,
           std::shared_ptr<const core::ReadLock> lock);
    ~Reader();

    bool has_changed() const override;

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
    std::shared_ptr<segment::Data> data;

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
    std::shared_ptr<segment::Data> data;

    /**
     * Redo detection of the data accessor.
     *
     * Call this, for example, after converting the segment to a different
     * format.
     */
    void update_data();

public:
    Checker(std::shared_ptr<const Segment> segment,
            std::shared_ptr<core::CheckLock> lock);

    bool has_data() const override;
    std::optional<time_t> timestamp() const override;
    bool allows_tar() const override;
    bool allows_zip() const override;
    bool allows_compress() const override;
    arki::metadata::Collection scan() override;
    FsckResult fsck(segment::Reporter& reporter, bool quick = true) override;
    bool scan_data(segment::Reporter& reporter,
                   metadata_dest_func dest) override;
    std::shared_ptr<segment::Fixer> fixer() override;

    friend class Fixer;
};

class Fixer : public segment::Fixer
{
public:
    Fixer(std::shared_ptr<Checker> checker,
          std::shared_ptr<core::CheckWriteLock> lock);

    const metadata::Checker& checker() const
    {
        return *std::static_pointer_cast<const metadata::Checker>(m_checker);
    }
    metadata::Checker& checker()
    {
        return *std::static_pointer_cast<metadata::Checker>(m_checker);
    }

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
    void test_mark_all_removed() override;
    void test_corrupt_data(unsigned data_idx) override;
    void test_truncate_data(unsigned data_idx) override;
    void test_touch_contents(time_t timestamp) override;
    void test_make_overlap(unsigned overlap_size,
                           unsigned data_idx = 1) override;
    void test_make_hole(unsigned hole_size, unsigned data_idx = 0) override;
    arki::metadata::Collection
    test_change_metadata(std::shared_ptr<Metadata> md,
                         unsigned data_idx = 0) override;
    void test_swap_data(unsigned d1_idx, unsigned d2_idx,
                        const segment::RepackConfig& repack_config) override;
};

} // namespace arki::segment::metadata

#endif
