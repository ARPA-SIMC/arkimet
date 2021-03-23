#ifndef ARKI_SEGMENT_FD_H
#define ARKI_SEGMENT_FD_H

#include <arki/segment.h>
#include <arki/segment/base.h>
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
class File : public core::File
{
public:
    using core::File::File;

    void fdtruncate_nothrow(off_t pos) noexcept;
    virtual size_t write_data(const metadata::Data& data) = 0;
    virtual void test_add_padding(size_t size) = 0;
};


class Segment : public arki::Segment
{
public:
    using arki::Segment::Segment;

    time_t timestamp() const override;
    static bool can_store(const std::string& format);
};


/// Base class for unix fd based read/write functions
template<typename Segment>
class Reader : public segment::BaseReader<Segment>
{
public:
    core::File fd;

    Reader(const std::string& format, const std::string& root, const std::string& relpath, const std::string& abspath, std::shared_ptr<core::Lock> lock);

    bool scan_data(metadata_dest_func dest) override;
    std::vector<uint8_t> read(const types::source::Blob& src) override;
    size_t stream(const types::source::Blob& src, core::StreamOutput& out) override;
};

template<typename Segment, typename File>
class Writer : public segment::BaseWriter<Segment>
{
public:
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

class File : public fd::File
{
public:
    using fd::File::File;
    size_t write_data(const metadata::Data& buf) override;
    void test_add_padding(size_t size) override;
};

class HoleFile : public fd::File
{
public:
    using fd::File::File;
    size_t write_data(const metadata::Data& buf) override;
    void test_add_padding(size_t size) override;
};


class Segment : public fd::Segment
{
public:
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


class HoleSegment : public Segment
{
public:
    using Segment::Segment;

    const char* type() const override;
    bool single_file() const override;
    std::shared_ptr<segment::Reader> reader(std::shared_ptr<core::Lock> lock) const override;
    std::shared_ptr<segment::Checker> checker() const override;
    static std::shared_ptr<segment::Writer> make_writer(const WriterConfig& config, const std::string& format, const std::string& rootdir, const std::string& relpath, const std::string& abspath);
    static std::shared_ptr<segment::Checker> make_checker(const std::string& format, const std::string& rootdir, const std::string& relpath, const std::string& abspath);
};


class Reader : public fd::Reader<Segment>
{
public:
    using fd::Reader<Segment>::Reader;
};

class Writer : public fd::Writer<Segment, File>
{
public:
    using fd::Writer<Segment, File>::Writer;
};

class Checker : public fd::Checker<Segment, File>
{
public:
    using fd::Checker<Segment, File>::Checker;
};

class HoleWriter : public fd::Writer<HoleSegment, HoleFile>
{
public:
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

class File : public concat::File
{
public:
    using concat::File::File;

    void test_add_padding(size_t size) override;
};

class Segment : public fd::Segment
{
public:
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

class Reader : public fd::Reader<Segment>
{
public:
    using fd::Reader<Segment>::Reader;
};

class Writer : public fd::Writer<Segment, File>
{
public:
    using fd::Writer<Segment, File>::Writer;
};

class Checker : public fd::Checker<Segment, File>
{
public:
    using fd::Checker<Segment, File>::Checker;
};

}

}
}
#endif

