#ifndef ARKI_SEGMENT_FD_H
#define ARKI_SEGMENT_FD_H

/// Base class for unix fd based read/write functions

#include <arki/libconfig.h>
#include <arki/segment.h>
#include <arki/segment/common.h>
#include <arki/core/file.h>
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
    virtual size_t write_data(const std::vector<uint8_t>& buf) = 0;
    virtual void test_add_padding(size_t size) = 0;
};

struct Creator : public AppendCreator
{
    std::unique_ptr<File> out;
    size_t written = 0;

    Creator(const std::string& root, const std::string& relpath, metadata::Collection& mds, std::unique_ptr<File> out);
    size_t append(const std::vector<uint8_t>& data) override;
    void create();
};

struct CheckBackend : public AppendCheckBackend
{
    core::File data;
    struct stat st;

    CheckBackend(const std::string& abspath, const std::string& relpath, std::function<void(const std::string&)> reporter, const metadata::Collection& mds);
    void validate(Metadata& md, const types::source::Blob& source) override;
    size_t offset_end() const override;
    State check();
};


struct Reader : public segment::Reader
{
    core::File fd;

    Reader(const std::string& root, const std::string& relpath, const std::string& abspath, std::shared_ptr<core::Lock> lock);

    const char* type() const override;
    bool single_file() const override;

    bool scan(metadata_dest_func dest) override;
    std::vector<uint8_t> read(const types::source::Blob& src) override;
    size_t stream(const types::source::Blob& src, core::NamedFileDescriptor& out) override;
};


struct Writer : public segment::Writer
{
    File* fd = nullptr;
    off_t initial_size;
    off_t current_pos;
    std::vector<PendingMetadata> pending;

    Writer(const std::string& root, const std::string& relpath, std::unique_ptr<File> fd);
    ~Writer();

    bool single_file() const override;
    virtual std::unique_ptr<File> open_file(const std::string& pathname, int flags, mode_t mode) = 0;
    size_t next_offset() const override;
    const types::source::Blob& append(Metadata& md) override;

    void commit() override;
    void rollback() override;
    void rollback_nothrow() noexcept override;
};


class Checker : public segment::Checker
{
protected:
    virtual std::unique_ptr<File> open(const std::string& pathname) = 0;

    virtual std::unique_ptr<File> open_file(const std::string& pathname, int flags, mode_t mode) = 0;
    void move_data(const std::string& new_root, const std::string& new_relpath, const std::string& new_abspath) override;

public:
    using segment::Checker::Checker;

    bool single_file() const override;
    bool exists_on_disk() override;
    time_t timestamp() override;
    size_t size() override;

    Pending repack(const std::string& rootdir, metadata::Collection& mds, unsigned test_flags=0) override;
    size_t remove() override;

    void test_truncate(size_t offset) override;
    void test_make_hole(metadata::Collection& mds, unsigned hole_size, unsigned data_idx) override;
    void test_make_overlap(metadata::Collection& mds, unsigned overlap_size, unsigned data_idx) override;
    void test_corrupt(const metadata::Collection& mds, unsigned data_idx) override;
};

bool can_store(const std::string& format);

}
}
}
#endif

