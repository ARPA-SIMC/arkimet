#ifndef ARKI_SEGMENT_FD_H
#define ARKI_SEGMENT_FD_H

#include <arki/segment/data.h>
#include <arki/segment/data/base.h>
#include <arki/core/file.h>
#include <arki/metadata/fwd.h>
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
    virtual size_t write_data(const metadata::Data& data) = 0;
    virtual void test_add_padding(size_t size) = 0;
};


class Data : public arki::segment::Data
{
public:
    using arki::segment::Data::Data;

    time_t timestamp() const override;
    static bool can_store(const std::string& format);
};


/// Base class for unix fd based read/write functions
template<typename Data>
class Reader : public BaseReader<Data>
{
public:
    core::File fd;

    Reader(std::shared_ptr<const Data> data, std::shared_ptr<core::Lock> lock);

    bool scan_data(metadata_dest_func dest) override;
    std::vector<uint8_t> read(const types::source::Blob& src) override;
    stream::SendResult stream(const types::source::Blob& src, StreamOutput& out) override;
};

template<typename Data, typename File>
class Writer : public BaseWriter<Data>
{
public:
    using BaseWriter<Data>::segment;

    File fd;
    struct timespec initial_mtime;
    off_t initial_size;
    off_t current_pos;
    std::vector<segment::data::Writer::PendingMetadata> pending;

    Writer(const data::WriterConfig& config, std::shared_ptr<const Data> data, int mode=0);
    ~Writer();

    size_t next_offset() const override;
    const types::source::Blob& append(Metadata& md) override;

    void commit() override;
    void rollback() override;
    void rollback_nothrow() noexcept override;
};


template<typename Data, typename File>
class Checker : public BaseChecker<Data>
{
protected:
    void move_data(const std::filesystem::path& new_root, const std::filesystem::path& new_relpath, const std::filesystem::path& new_abspath) override;

public:
    explicit Checker(std::shared_ptr<const Data> data);

    bool exists_on_disk() override;
    bool is_empty() override;
    size_t size() override;

    bool rescan_data(std::function<void(const std::string&)> reporter, std::shared_ptr<core::Lock> lock, metadata_dest_func dest) override;
    State check(std::function<void(const std::string&)> reporter, const metadata::Collection& mds, bool quick=true) override;
    core::Pending repack(const std::filesystem::path& rootdir, metadata::Collection& mds, const data::RepackConfig& cfg=data::RepackConfig()) override;
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


class Data : public fd::Data
{
public:
    using fd::Data::Data;

    const char* type() const override;
    bool single_file() const override;
    std::shared_ptr<segment::data::Reader> reader(std::shared_ptr<core::Lock> lock) const override;
    std::shared_ptr<segment::data::Writer> writer(const data::WriterConfig& config, bool mock_data) const override;
    std::shared_ptr<segment::data::Checker> checker(bool mock_data) const override;

    static std::shared_ptr<Checker> create(const Segment& segment, metadata::Collection& mds, const RepackConfig& cfg=RepackConfig());
    static bool can_store(const std::string& format);

    static const unsigned padding = 0;
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

class HoleWriter : public fd::Writer<Data, HoleFile>
{
public:
    using fd::Writer<Data, HoleFile>::Writer;
};

class HoleChecker : public fd::Checker<Data, HoleFile>
{
public:
    using fd::Checker<Data, HoleFile>::Checker;
    core::Pending repack(const std::filesystem::path& rootdir, metadata::Collection& mds, const data::RepackConfig& cfg=data::RepackConfig()) override;
};

}

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
    std::shared_ptr<segment::data::Reader> reader(std::shared_ptr<core::Lock> lock) const override;
    std::shared_ptr<segment::data::Writer> writer(const data::WriterConfig& config, bool mock_data) const override;
    std::shared_ptr<segment::data::Checker> checker(bool mock_data) const override;

    static std::shared_ptr<Checker> create(const Segment& segment, metadata::Collection& mds, const RepackConfig& cfg=RepackConfig());
    static bool can_store(const std::string& format);

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

}

}
#endif

