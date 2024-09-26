#ifndef ARKI_SEGMENT_DIR_H
#define ARKI_SEGMENT_DIR_H

#include <arki/defs.h>
#include <arki/segment.h>
#include <arki/segment/base.h>
#include <arki/segment/seqfile.h>
#include <arki/core/file.h>
#include <arki/utils/sys.h>
#include <vector>
#include <map>

namespace arki {
class Metadata;

namespace segment {
namespace dir {

class Segment : public arki::Segment
{
public:
    using arki::Segment::Segment;

    const char* type() const override;
    bool single_file() const override;
    time_t timestamp() const override;
    std::shared_ptr<segment::Reader> reader(std::shared_ptr<core::Lock> lock) const override;
    std::shared_ptr<segment::Checker> checker() const override;
    static std::shared_ptr<Checker> make_checker(const std::string& format, const std::filesystem::path& rootdir, const std::filesystem::path& relpath, const std::filesystem::path& abspath);
    static std::shared_ptr<Checker> create(const std::string& format, const std::filesystem::path& rootdir, const std::filesystem::path& relpath, const std::filesystem::path& abspath, metadata::Collection& mds, const RepackConfig& cfg=RepackConfig());
    static bool can_store(const std::string& format);
};


class HoleSegment : public Segment
{
public:
    using Segment::Segment;

    const char* type() const override;
};


class Reader : public segment::BaseReader<Segment>
{
public:
    utils::sys::Path dirfd;

    Reader(const std::string& format, const std::filesystem::path& root, const std::filesystem::path& relpath, const std::filesystem::path& abspath, std::shared_ptr<core::Lock> lock);

    bool scan_data(metadata_dest_func dest) override;
    utils::sys::File open_src(const types::source::Blob& src);
    std::vector<uint8_t> read(const types::source::Blob& src) override;
    stream::SendResult stream(const types::source::Blob& src, StreamOutput& out) override;
};


template<typename Segment>
class BaseWriter : public segment::BaseWriter<Segment>
{
public:
    SequenceFile seqfile;
    std::vector<std::filesystem::path> written;
    std::vector<segment::Writer::PendingMetadata> pending;
    size_t current_pos;

    BaseWriter(const WriterConfig& config, const std::string& format, const std::filesystem::path& root, const std::filesystem::path& relpath, const std::filesystem::path& abspath);
    ~BaseWriter();

    virtual void write_file(Metadata& md, core::NamedFileDescriptor& fd) = 0;

    size_t next_offset() const override;
    const types::source::Blob& append(Metadata& md) override;

    void commit() override;
    void rollback() override;
    void rollback_nothrow() noexcept override;
};


class Writer : public BaseWriter<Segment>
{
public:
    using BaseWriter<Segment>::BaseWriter;
    void write_file(Metadata& md, core::NamedFileDescriptor& fd) override;
};

class HoleWriter: public BaseWriter<HoleSegment>
{
public:
    using BaseWriter<HoleSegment>::BaseWriter;
    void write_file(Metadata& md, core::NamedFileDescriptor& fd) override;
};


template<typename Segment>
class BaseChecker : public segment::BaseChecker<Segment>
{
public:
    using segment::BaseChecker<Segment>::BaseChecker;

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


class Checker : public BaseChecker<Segment>
{
public:
    using BaseChecker<Segment>::BaseChecker;
};


/**
 * Same as Writer, but uses ftruncate to write dummy empty file with the
 * original size but that take up no blocks in the file system. This is used
 * only for testing.
 */
class HoleChecker : public BaseChecker<HoleSegment>
{
public:
    using BaseChecker<HoleSegment>::BaseChecker;
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
    /// Format of the data to scan
    std::string format;

    /// Pathname to the directory to scan
    std::filesystem::path abspath;

    /// File size by offset
    std::map<size_t, ScannerData> on_disk;

    /// Maximum sequence found on disk
    size_t max_sequence = 0;

    Scanner(const std::string& format, const std::filesystem::path& abspath);

    /// Fill on_disk and max_sequence with the data found on disk
    void list_files();

    /// Scan the data found in on_disk sending results to dest
    bool scan(std::shared_ptr<segment::Reader> reader, metadata_dest_func dest);

    /// Scan the data found in on_disk sending results to dest, reporting scanning errors to the reporter
    bool scan(std::function<void(const std::string&)> reporter, std::shared_ptr<segment::Reader> reader, metadata_dest_func dest);
};

}
}
}
#endif
