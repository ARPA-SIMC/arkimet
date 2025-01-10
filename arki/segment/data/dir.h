#ifndef ARKI_SEGMENT_DIR_H
#define ARKI_SEGMENT_DIR_H

#include <arki/defs.h>
#include <arki/segment/data.h>
#include <arki/segment/data/base.h>
#include <arki/segment/data/seqfile.h>
#include <arki/core/file.h>
#include <arki/utils/sys.h>
#include <vector>
#include <map>

namespace arki {
class Metadata;

namespace segment::data::dir {

class Data : public arki::segment::Data
{
public:
    using arki::segment::Data::Data;

    const char* type() const override;
    bool single_file() const override;
    time_t timestamp() const override;

    std::shared_ptr<segment::data::Reader> reader(std::shared_ptr<core::Lock> lock) const override;
    std::shared_ptr<segment::data::Writer> writer(const data::WriterConfig& config, bool mock_data) const override;
    std::shared_ptr<segment::data::Checker> checker(bool mock_data) const override;

    static std::shared_ptr<Checker> create(const Segment& segment, metadata::Collection& mds, const RepackConfig& cfg=RepackConfig());
    static bool can_store(DataFormat format);
};


class Reader : public data::BaseReader<Data>
{
public:
    utils::sys::Path dirfd;

    Reader(std::shared_ptr<const Data> data, std::shared_ptr<core::Lock> lock);

    bool scan_data(metadata_dest_func dest) override;
    utils::sys::File open_src(const types::source::Blob& src);
    std::vector<uint8_t> read(const types::source::Blob& src) override;
    stream::SendResult stream(const types::source::Blob& src, StreamOutput& out) override;
};


template<typename Data>
class BaseWriter : public data::BaseWriter<Data>
{
public:
    SequenceFile seqfile;
    std::vector<std::filesystem::path> written;
    std::vector<data::Writer::PendingMetadata> pending;
    size_t current_pos;

    BaseWriter(const WriterConfig& config, std::shared_ptr<const Data> data);
    ~BaseWriter();

    virtual void write_file(Metadata& md, core::NamedFileDescriptor& fd) = 0;

    size_t next_offset() const override;
    const types::source::Blob& append(Metadata& md) override;

    void commit() override;
    void rollback() override;
    void rollback_nothrow() noexcept override;
};


class Writer : public BaseWriter<Data>
{
public:
    using BaseWriter<Data>::BaseWriter;
    void write_file(Metadata& md, core::NamedFileDescriptor& fd) override;
};

class HoleWriter: public BaseWriter<Data>
{
public:
    using BaseWriter<Data>::BaseWriter;
    void write_file(Metadata& md, core::NamedFileDescriptor& fd) override;
};


template<typename Data>
class BaseChecker : public data::BaseChecker<Data>
{
public:
    using data::BaseChecker<Data>::BaseChecker;

    /// Call f for each nnnnnn.format file in the directory segment, passing the file name
    void foreach_datafile(std::function<void(const char*)> f);
    void validate(Metadata& md, const scan::Validator& v);
    void move_data(const std::filesystem::path& new_root, const std::filesystem::path& new_relpath, const std::filesystem::path& new_abspath) override;
    bool exists_on_disk() override;
    bool is_empty() override;
    size_t size() override;

    bool rescan_data(std::function<void(const std::string&)> reporter, std::shared_ptr<core::Lock> lock, metadata_dest_func dest) override;
    State check(std::function<void(const std::string&)> reporter, const metadata::Collection& mds, bool quick=true) override;
    size_t remove() override;
    core::Pending repack(const std::filesystem::path& rootdir, metadata::Collection& mds, const RepackConfig& cfg=RepackConfig()) override;

    void test_truncate(size_t offset) override;
    void test_make_hole(metadata::Collection& mds, unsigned hole_size, unsigned data_idx) override;
    void test_make_overlap(metadata::Collection& mds, unsigned overlap_size, unsigned data_idx) override;
    void test_corrupt(const metadata::Collection& mds, unsigned data_idx) override;
};


class Checker : public BaseChecker<Data>
{
public:
    using BaseChecker<Data>::BaseChecker;
};


/**
 * Same as Writer, but uses ftruncate to write dummy empty file with the
 * original size but that take up no blocks in the file system. This is used
 * only for testing.
 */
class HoleChecker : public BaseChecker<Data>
{
public:
    using BaseChecker<Data>::BaseChecker;
    State check(std::function<void(const std::string&)> reporter, const metadata::Collection& mds, bool quick=true) override;
};


class ScannerData
{
public:
    std::filesystem::path fname;
    size_t size;

    ScannerData(const std::filesystem::path& fname, size_t size)
        : fname(fname), size(size)
    {
    }
};


class Scanner
{
public:
    /// Segment pointing to the directory
    const Segment& segment;

    /// File size by offset
    std::map<size_t, ScannerData> on_disk;

    /// Maximum sequence found on disk
    size_t max_sequence = 0;

    explicit Scanner(const Segment& segment);

    /// Fill on_disk and max_sequence with the data found on disk
    void list_files();

    /// Scan the data found in on_disk sending results to dest
    bool scan(std::shared_ptr<data::Reader> reader, metadata_dest_func dest);

    /// Scan the data found in on_disk sending results to dest, reporting scanning errors to the reporter
    bool scan(std::function<void(const std::string&)> reporter, std::shared_ptr<data::Reader> reader, metadata_dest_func dest);
};

}
}
#endif
