#ifndef ARKI_SEGMENT_FD_H
#define ARKI_SEGMENT_FD_H

#include <arki/core/file.h>
#include <arki/metadata/fwd.h>
#include <arki/segment/data.h>
#include <arki/segment/data/base.h>
#include <string>
#include <vector>

namespace arki::segment::data {
namespace fd {

/**
 * Customize in subclasses to add format-specific I/O
 */
class File : public core::File
{
public:
    using core::File::File;

    void fdtruncate_nothrow(off_t pos) noexcept;
    virtual size_t write_data(const arki::metadata::Data& data) = 0;
    virtual void test_add_padding(size_t size)                  = 0;
};

class Data : public arki::segment::Data
{
public:
    using arki::segment::Data::Data;

    std::optional<time_t> timestamp() const override;
    bool exists_on_disk() const override;
    bool is_empty() const override;
    size_t size() const override;
    utils::files::PreserveFileTimes preserve_mtime() override;
    static bool can_store(DataFormat format);

    static std::shared_ptr<segment::Data>
    detect_data(std::shared_ptr<const Segment> segment, bool mock_data);
};

/// Base class for unix fd based read/write functions
template <typename Data> class Reader : public BaseReader<Data>
{
public:
    core::File fd;

    Reader(std::shared_ptr<const Data> data,
           std::shared_ptr<const core::ReadLock> lock);

    bool scan_data(metadata_dest_func dest) override;
    std::vector<uint8_t> read(const types::source::Blob& src) override;
    stream::SendResult stream(const types::source::Blob& src,
                              StreamOutput& out) override;
};

template <typename Data, typename File> class Writer : public BaseWriter<Data>
{
public:
    using BaseWriter<Data>::segment;

    File fd;
    struct timespec initial_mtime;
    off_t initial_size;
    off_t current_pos;
    std::vector<segment::data::Writer::PendingMetadata> pending;

    Writer(const segment::WriterConfig& config,
           std::shared_ptr<const Data> data, int mode = 0);
    ~Writer();

    size_t next_offset() const override;
    const types::source::Blob& append(Metadata& md) override;

    void commit() override;
    void rollback() override;
    void rollback_nothrow() noexcept override;
};

template <typename Data, typename File> class Checker : public BaseChecker<Data>
{
protected:
    void move_data(std::shared_ptr<const Segment> new_segment) override;

public:
    explicit Checker(std::shared_ptr<const Data> data);

    bool rescan_data(std::function<void(const std::string&)> reporter,
                     std::shared_ptr<const core::ReadLock> lock,
                     metadata_dest_func dest) override;
    State check(std::function<void(const std::string&)> reporter,
                const arki::metadata::Collection& mds,
                bool quick = true) override;
    core::Pending repack(arki::metadata::Collection& mds,
                         const RepackConfig& cfg = RepackConfig()) override;
    size_t remove() override;

    void test_truncate(size_t offset) override;
    void test_make_hole(arki::metadata::Collection& mds, unsigned hole_size,
                        unsigned data_idx) override;
    void test_make_overlap(arki::metadata::Collection& mds,
                           unsigned overlap_size, unsigned data_idx) override;
    void test_corrupt(const arki::metadata::Collection& mds,
                      unsigned data_idx) override;
    void test_touch_contents(time_t timestamp) override;
};

} // namespace fd

namespace single {

class Data : public fd::Data
{
public:
    using fd::Data::Data;

    const char* type() const override;
    bool single_file() const override;
    std::shared_ptr<segment::data::Reader>
    reader(std::shared_ptr<const core::ReadLock> lock) const override;
    std::shared_ptr<segment::data::Writer>
    writer(const segment::WriterConfig& config) const override;
    std::shared_ptr<segment::data::Checker> checker() const override;
    void create_segment(arki::metadata::Collection& mds,
                        const RepackConfig& cfg = RepackConfig()) override
    {
        throw std::runtime_error(
            "segment::data::single::create_segment not yet implemented");
    }
    size_t next_offset(size_t offset, size_t size) const override
    {
        throw std::runtime_error("this segment can only hold one data item");
    }

    static const unsigned padding = 0;
};

class Reader : public fd::Reader<Data>
{
public:
    using fd::Reader<Data>::Reader;
};

} // namespace single

namespace concat {

class File : public fd::File
{
public:
    using fd::File::File;
    size_t write_data(const arki::metadata::Data& buf) override;
    void test_add_padding(size_t size) override;
};

class HoleFile : public fd::File
{
public:
    using fd::File::File;
    size_t write_data(const arki::metadata::Data& buf) override;
    void test_add_padding(size_t size) override;
};

class Data : public fd::Data
{
public:
    using fd::Data::Data;

    const char* type() const override;
    bool single_file() const override;
    size_t next_offset(size_t offset, size_t size) const override
    {
        return offset + size;
    }
    std::shared_ptr<segment::data::Reader>
    reader(std::shared_ptr<const core::ReadLock> lock) const override;
    std::shared_ptr<segment::data::Writer>
    writer(const segment::WriterConfig& config) const override;
    std::shared_ptr<segment::data::Checker> checker() const override;
    void create_segment(arki::metadata::Collection& mds,
                        const RepackConfig& cfg = RepackConfig()) override
    {
        create(*m_segment, mds, cfg);
    }

    static std::shared_ptr<Checker>
    create(const Segment& segment, arki::metadata::Collection& mds,
           const RepackConfig& cfg = RepackConfig());

    static const unsigned padding = 0;
};

class HoleData : public Data
{
public:
    using Data::Data;

    std::shared_ptr<segment::data::Writer>
    writer(const segment::WriterConfig& config) const override;
    std::shared_ptr<segment::data::Checker> checker() const override;
};

class Reader : public fd::Reader<Data>
{
public:
    using fd::Reader<Data>::Reader;
};

class Writer : public fd::Writer<Data, File>
{
public:
    using fd::Writer<Data, File>::Writer;
};

class Checker : public fd::Checker<Data, File>
{
public:
    using fd::Checker<Data, File>::Checker;
    core::Pending repack(arki::metadata::Collection& mds,
                         const RepackConfig& cfg = RepackConfig()) override;
};

class HoleWriter : public fd::Writer<Data, HoleFile>
{
public:
    using fd::Writer<Data, HoleFile>::Writer;
};

class HoleChecker : public fd::Checker<Data, HoleFile>
{
public:
    using fd::Checker<Data, HoleFile>::Checker;
    core::Pending repack(arki::metadata::Collection& mds,
                         const RepackConfig& cfg = RepackConfig()) override;
};

} // namespace concat

namespace lines {

class File : public concat::File
{
public:
    using concat::File::File;

    void test_add_padding(size_t size) override;
};

class Data : public fd::Data
{
public:
    using fd::Data::Data;

    const char* type() const override;
    bool single_file() const override;
    size_t next_offset(size_t offset, size_t size) const override
    {
        return offset + size + padding;
    }
    std::shared_ptr<segment::data::Reader>
    reader(std::shared_ptr<const core::ReadLock> lock) const override;
    std::shared_ptr<segment::data::Writer>
    writer(const segment::WriterConfig& config) const override;
    std::shared_ptr<segment::data::Checker> checker() const override;
    void create_segment(arki::metadata::Collection& mds,
                        const RepackConfig& cfg = RepackConfig()) override
    {
        create(*m_segment, mds, cfg);
    }

    static std::shared_ptr<Checker>
    create(const Segment& segment, arki::metadata::Collection& mds,
           const RepackConfig& cfg = RepackConfig());

    static const unsigned padding = 1;
};

class Reader : public fd::Reader<Data>
{
public:
    using fd::Reader<Data>::Reader;
};

class Writer : public fd::Writer<Data, File>
{
public:
    using fd::Writer<Data, File>::Writer;
};

class Checker : public fd::Checker<Data, File>
{
public:
    using fd::Checker<Data, File>::Checker;
};

} // namespace lines

} // namespace arki::segment::data
#endif
