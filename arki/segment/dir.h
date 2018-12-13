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

struct Segment : public arki::Segment
{
    using arki::Segment::Segment;

    const char* type() const override;
    bool single_file() const override;
    time_t timestamp() const override;
    std::shared_ptr<segment::Reader> reader(std::shared_ptr<core::Lock> lock) const override;
    std::shared_ptr<segment::Checker> checker() const override;
    static std::shared_ptr<Checker> make_checker(const std::string& format, const std::string& rootdir, const std::string& relpath, const std::string& abspath);
    static std::shared_ptr<Checker> create(const std::string& format, const std::string& rootdir, const std::string& relpath, const std::string& abspath, metadata::Collection& mds, const RepackConfig& cfg=RepackConfig());
    static bool can_store(const std::string& format);
};


struct HoleSegment : public Segment
{
    using Segment::Segment;

    const char* type() const override;
};


struct Reader : public segment::BaseReader<Segment>
{
    utils::sys::Path dirfd;

    Reader(const std::string& format, const std::string& root, const std::string& relpath, const std::string& abspath, std::shared_ptr<core::Lock> lock);

    bool scan_data(metadata_dest_func dest) override;
    utils::sys::File open_src(const types::source::Blob& src);
    std::vector<uint8_t> read(const types::source::Blob& src) override;
    size_t stream(const types::source::Blob& src, core::NamedFileDescriptor& out) override;
};


template<typename Segment>
struct BaseWriter : public segment::BaseWriter<Segment>
{
    SequenceFile seqfile;
    std::vector<std::string> written;
    std::vector<segment::Writer::PendingMetadata> pending;
    size_t current_pos;

    BaseWriter(const std::string& format, const std::string& root, const std::string& relpath, const std::string& abspath);
    ~BaseWriter();

    virtual void write_file(Metadata& md, core::NamedFileDescriptor& fd) = 0;

    size_t next_offset() const override;
    const types::source::Blob& append(Metadata& md, bool drop_cached_data_on_commit) override;

    void commit() override;
    void rollback() override;
    void rollback_nothrow() noexcept override;
};


struct Writer : public BaseWriter<Segment>
{
    using BaseWriter<Segment>::BaseWriter;
    void write_file(Metadata& md, core::NamedFileDescriptor& fd) override;
};

struct HoleWriter: public BaseWriter<HoleSegment>
{
    using BaseWriter<HoleSegment>::BaseWriter;
    void write_file(Metadata& md, core::NamedFileDescriptor& fd) override;
};


template<typename Segment>
struct BaseChecker : public segment::BaseChecker<Segment>
{
    using segment::BaseChecker<Segment>::BaseChecker;

    /// Call f for each nnnnnn.format file in the directory segment, passing the file name
    void foreach_datafile(std::function<void(const char*)> f);
    void validate(Metadata& md, const scan::Validator& v);
    void move_data(const std::string& new_root, const std::string& new_relpath, const std::string& new_abspath) override;
    bool exists_on_disk() override;
    bool is_empty() override;
    size_t size() override;

    bool rescan_data(std::function<void(const std::string&)> reporter, std::shared_ptr<core::Lock> lock, metadata_dest_func dest) override;
    State check(std::function<void(const std::string&)> reporter, const metadata::Collection& mds, bool quick=true) override;
    size_t remove() override;
    Pending repack(const std::string& rootdir, metadata::Collection& mds, const RepackConfig& cfg=RepackConfig()) override;

    void test_truncate(size_t offset) override;
    void test_make_hole(metadata::Collection& mds, unsigned hole_size, unsigned data_idx) override;
    void test_make_overlap(metadata::Collection& mds, unsigned overlap_size, unsigned data_idx) override;
    void test_corrupt(const metadata::Collection& mds, unsigned data_idx) override;
};


struct Checker : public BaseChecker<Segment>
{
    using BaseChecker<Segment>::BaseChecker;
};


/**
 * Same as Writer, but uses ftruncate to write dummy empty file with the
 * original size but that take up no blocks in the file system. This is used
 * only for testing.
 */
struct HoleChecker : public BaseChecker<HoleSegment>
{
    using BaseChecker<HoleSegment>::BaseChecker;
    State check(std::function<void(const std::string&)> reporter, const metadata::Collection& mds, bool quick=true) override;
};


struct ScannerData
{
    std::string fname;
    size_t size;

    ScannerData(const std::string& fname, size_t size)
        : fname(fname), size(size)
    {
    }
};


struct Scanner
{
    /// Format of the data to scan
    std::string format;

    /// Pathname to the directory to scan
    std::string abspath;

    /// File size by offset
    std::map<size_t, ScannerData> on_disk;

    /// Maximum sequence found on disk
    size_t max_sequence = 0;

    Scanner(const std::string& format, const std::string& abspath);

    /// Fill on_disk and max_sequence with the data found on disk
    void list_files();

    /// Scan the data found in on_disk sending results to dest
    bool scan(std::shared_ptr<segment::Reader> reader, metadata_dest_func dest);
};

}
}
}
#endif
