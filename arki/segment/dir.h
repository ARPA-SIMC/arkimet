#ifndef ARKI_SEGMENT_DIR_H
#define ARKI_SEGMENT_DIR_H

/// Directory based data collection

#include <arki/defs.h>
#include <arki/segment.h>
#include <arki/segment/seqfile.h>
#include <arki/core/file.h>
#include <arki/utils/sys.h>
#include <vector>

namespace arki {
class Metadata;

namespace segment {
namespace dir {

struct Reader : public segment::Reader
{
    utils::sys::Path dirfd;

    Reader(const std::string& format, const std::string& root, const std::string& relpath, const std::string& abspath, std::shared_ptr<core::Lock> lock);

    const char* type() const override;
    bool single_file() const override;
    time_t timestamp() override;

    bool scan_data(metadata_dest_func dest) override;
    utils::sys::File open_src(const types::source::Blob& src);
    std::vector<uint8_t> read(const types::source::Blob& src) override;
    size_t stream(const types::source::Blob& src, core::NamedFileDescriptor& out) override;
};


struct Writer : public segment::Writer
{
    SequenceFile seqfile;
    std::vector<std::string> written;
    std::vector<PendingMetadata> pending;
    size_t current_pos;

    Writer(const std::string& format, const std::string& root, const std::string& relpath, const std::string& abspath);
    ~Writer();

    const char* type() const override;
    bool single_file() const override;

    size_t next_offset() const override;
    const types::source::Blob& append(Metadata& md) override;

    virtual void write_file(Metadata& md, core::NamedFileDescriptor& fd);

    void commit() override;
    void rollback() override;
    void rollback_nothrow() noexcept override;
};

struct HoleWriter: public Writer
{
    using Writer::Writer;

    const char* type() const override;

    void write_file(Metadata& md, core::NamedFileDescriptor& fd) override;
};


class Checker : public segment::Checker
{
public:
    void validate(Metadata& md, const scan::Validator& v);
    void move_data(const std::string& new_root, const std::string& new_relpath, const std::string& new_abspath) override;

public:
    Checker(const std::string& format, const std::string& root, const std::string& relpath, const std::string& abspath);

    const char* type() const override;
    bool single_file() const override;

    bool exists_on_disk() override;
    time_t timestamp() override;
    size_t size() override;

    std::shared_ptr<segment::Reader> reader(std::shared_ptr<core::Lock> lock) override;
    State check(std::function<void(const std::string&)> reporter, const metadata::Collection& mds, bool quick=true) override;
    size_t remove() override;
    Pending repack(const std::string& rootdir, metadata::Collection& mds, unsigned test_flags=0) override;

    /// Call f for each nnnnnn.format file in the directory segment, passing the file name
    void foreach_datafile(std::function<void(const char*)> f);

    void test_truncate(size_t offset) override;
    void test_make_hole(metadata::Collection& mds, unsigned hole_size, unsigned data_idx) override;
    void test_make_overlap(metadata::Collection& mds, unsigned overlap_size, unsigned data_idx) override;
    void test_corrupt(const metadata::Collection& mds, unsigned data_idx) override;

    static std::shared_ptr<Checker> create(const std::string& format, const std::string& rootdir, const std::string& relpath, const std::string& abspath, metadata::Collection& mds, unsigned test_flags=0);
    static bool can_store(const std::string& format);
};


/**
 * Same as Writer, but uses ftruncate to write dummy empty file with the
 * original size but that take up no blocks in the file system. This is used
 * only for testing.
 */
class HoleChecker : public Checker
{
public:
    using Checker::Checker;

    const char* type() const override;

    State check(std::function<void(const std::string&)> reporter, const metadata::Collection& mds, bool quick=true) override;
};

}
}
}
#endif
