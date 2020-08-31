#ifndef ARKI_SEGMENT_FD_H
#define ARKI_SEGMENT_FD_H

#include <arki/segment.h>
#include <arki/segment/base.h>
#include <arki/segment/common.h>
#include <arki/core/file.h>
#include <arki/metadata/fwd.h>
#include <string>
#include <vector>

namespace arki {
namespace segment {
namespace fd {

/**
 * Customize in subclasses to add format-specific I/O
 */
struct File : public core::File
{
    using core::File::File;

    void fdtruncate_nothrow(off_t pos) noexcept;
    virtual size_t write_data(const metadata::Data& data) = 0;
    virtual void test_add_padding(size_t size) = 0;
};


struct Segment : public arki::Segment
{
    using arki::Segment::Segment;

    time_t timestamp() const override;
    static bool can_store(const std::string& format);
};


/// Base class for unix fd based read/write functions
template<typename Segment>
struct Reader : public segment::BaseReader<Segment>
{
    core::File fd;

    Reader(const std::string& format, const std::string& root, const std::string& relpath, const std::string& abspath, std::shared_ptr<core::Lock> lock);

    bool scan_data(metadata_dest_func dest) override;
    std::vector<uint8_t> read(const types::source::Blob& src) override;
    size_t stream(const types::source::Blob& src, core::NamedFileDescriptor& out) override;
};

template<typename Segment, typename File>
struct Writer : public segment::BaseWriter<Segment>
{
    File fd;
    struct timespec initial_mtime;
    off_t initial_size;
    off_t current_pos;
    std::vector<segment::Writer::PendingMetadata> pending;

    Writer(const WriterConfig& config, const std::string& format, const std::string& root, const std::string& relpath, const std::string& abspath, int mode=0);
    ~Writer();

    size_t next_offset() const override;
    const types::source::Blob& append(Metadata& md) override;

    void commit() override;
    void rollback() override;
    void rollback_nothrow() noexcept override;
};


template<typename Segment, typename File>
class Checker : public segment::BaseChecker<Segment>
{
protected:
    void move_data(const std::string& new_root, const std::string& new_relpath, const std::string& new_abspath) override;

public:
    Checker(const std::string& format, const std::string& root, const std::string& relpath, const std::string& abspath);

    bool exists_on_disk() override;
    bool is_empty() override;
    size_t size() override;

    bool rescan_data(std::function<void(const std::string&)> reporter, std::shared_ptr<core::Lock> lock, metadata_dest_func dest) override;
    State check(std::function<void(const std::string&)> reporter, const metadata::Collection& mds, bool quick=true) override;
    core::Pending repack(const std::string& rootdir, metadata::Collection& mds, const RepackConfig& cfg=RepackConfig()) override;
    size_t remove() override;

    void test_truncate(size_t offset) override;
    void test_make_hole(metadata::Collection& mds, unsigned hole_size, unsigned data_idx) override;
    void test_make_overlap(metadata::Collection& mds, unsigned overlap_size, unsigned data_idx) override;
    void test_corrupt(const metadata::Collection& mds, unsigned data_idx) override;
};

}

namespace concat {

struct File : public fd::File
{
    using fd::File::File;
    size_t write_data(const metadata::Data& buf) override;
    void test_add_padding(size_t size) override;
};

struct HoleFile : public fd::File
{
    using fd::File::File;
    size_t write_data(const metadata::Data& buf) override;
    void test_add_padding(size_t size) override;
};


struct Segment : public fd::Segment
{
    using fd::Segment::Segment;

    const char* type() const override;
    bool single_file() const override;
    std::shared_ptr<segment::Reader> reader(std::shared_ptr<core::Lock> lock) const override;
    std::shared_ptr<segment::Checker> checker() const override;

    static std::shared_ptr<segment::Writer> make_writer(const WriterConfig& config, const std::string& format, const std::string& rootdir, const std::string& relpath, const std::string& abspath);
    static std::shared_ptr<segment::Checker> make_checker(const std::string& format, const std::string& rootdir, const std::string& relpath, const std::string& abspath);
    static std::shared_ptr<segment::Checker> create(const std::string& format, const std::string& rootdir, const std::string& relpath, const std::string& abspath, metadata::Collection& mds, const RepackConfig& cfg=RepackConfig());
    static bool can_store(const std::string& format);

    static const unsigned padding = 0;
};


struct HoleSegment : public Segment
{
    using Segment::Segment;

    const char* type() const override;
    bool single_file() const override;
    std::shared_ptr<segment::Reader> reader(std::shared_ptr<core::Lock> lock) const override;
    std::shared_ptr<segment::Checker> checker() const override;
    static std::shared_ptr<segment::Writer> make_writer(const WriterConfig& config, const std::string& format, const std::string& rootdir, const std::string& relpath, const std::string& abspath);
    static std::shared_ptr<segment::Checker> make_checker(const std::string& format, const std::string& rootdir, const std::string& relpath, const std::string& abspath);
};


struct Reader : public fd::Reader<Segment>
{
    using fd::Reader<Segment>::Reader;
};

struct Writer : public fd::Writer<Segment, File>
{
    using fd::Writer<Segment, File>::Writer;
};

struct Checker : public fd::Checker<Segment, File>
{
    using fd::Checker<Segment, File>::Checker;
};

struct HoleWriter : public fd::Writer<HoleSegment, HoleFile>
{
    using fd::Writer<HoleSegment, HoleFile>::Writer;
};

class HoleChecker : public fd::Checker<HoleSegment, HoleFile>
{
public:
    using fd::Checker<HoleSegment, HoleFile>::Checker;
    core::Pending repack(const std::string& rootdir, metadata::Collection& mds, const RepackConfig& cfg=RepackConfig()) override;
};

}

namespace lines {

struct File : public concat::File
{
    using concat::File::File;

    void test_add_padding(size_t size) override;
};

struct Segment : public fd::Segment
{
    using fd::Segment::Segment;

    const char* type() const override;
    bool single_file() const override;
    std::shared_ptr<segment::Reader> reader(std::shared_ptr<core::Lock> lock) const override;
    std::shared_ptr<segment::Checker> checker() const override;

    static std::shared_ptr<Writer> make_writer(const WriterConfig& config, const std::string& format, const std::string& rootdir, const std::string& relpath, const std::string& abspath);
    static std::shared_ptr<Checker> make_checker(const std::string& format, const std::string& rootdir, const std::string& relpath, const std::string& abspath);
    static std::shared_ptr<Checker> create(const std::string& format, const std::string& rootdir, const std::string& relpath, const std::string& abspath, metadata::Collection& mds, const RepackConfig& cfg=RepackConfig());
    static bool can_store(const std::string& format);

    static const unsigned padding = 1;
};

struct Reader : public fd::Reader<Segment>
{
    using fd::Reader<Segment>::Reader;
};

struct Writer : public fd::Writer<Segment, File>
{
    using fd::Writer<Segment, File>::Writer;
};

struct Checker : public fd::Checker<Segment, File>
{
    using fd::Checker<Segment, File>::Checker;
};

}

}
}
#endif

