#ifndef ARKI_DATASET_SEGMENT_FD_H
#define ARKI_DATASET_SEGMENT_FD_H

/// Base class for unix fd based read/write functions

#include <arki/libconfig.h>
#include <arki/dataset/segment.h>
#include <arki/core/file.h>
#include <string>
#include <vector>

namespace arki {
struct Reader;

namespace dataset {
namespace segment {
namespace fd {

/**
 * Customize in subclasses to add format-specific I/O
 */
struct File : public arki::core::File
{
    using arki::core::File::File;

    void fdtruncate_nothrow(off_t pos) noexcept;
    virtual size_t write_data(const std::vector<uint8_t>& buf) = 0;
    virtual void test_add_padding(size_t size) = 0;
};


struct Writer : public dataset::segment::Writer
{
    File* fd = nullptr;
    off_t initial_size;
    off_t current_pos;
    std::vector<PendingMetadata> pending;

    Writer(const std::string& root, const std::string& relname, std::unique_ptr<File> fd);
    ~Writer();

    bool single_file() const override;
    virtual std::unique_ptr<File> open_file(const std::string& pathname, int flags, mode_t mode) = 0;
    size_t next_offset() const override;
    const types::source::Blob& append(Metadata& md) override;

    void commit() override;
    void rollback() override;
    void rollback_nothrow() noexcept override;
};


class Checker : public dataset::segment::Checker
{
protected:
    virtual std::unique_ptr<File> open(const std::string& pathname) = 0;

    void validate(Metadata& md, const scan::Validator& v) override;

    /**
     * If skip_validation is true, repack will skip validating the data that is
     * being read.
     *
     * This is only used during tests to support repacking files with mock data
     * inside. The files are made of filesystem holes, so the data that is read
     * from them is always zeroes.
     */
    Pending repack_impl(
            const std::string& rootdir,
            metadata::Collection& mds,
            bool skip_validation=false,
            unsigned test_flags=0);

    virtual std::unique_ptr<File> open_file(const std::string& pathname, int flags, mode_t mode) = 0;
    void move_data(const std::string& new_root, const std::string& new_relname, const std::string& new_absname) override;

public:
    using dataset::segment::Checker::Checker;

    bool single_file() const override;
    bool exists_on_disk() override;
    time_t timestamp() override;

    size_t remove() override;

    State check_fd(dataset::Reporter& reporter, const std::string& ds, const metadata::Collection& mds, unsigned max_gap=0, bool quick=true);

    void test_truncate(size_t offset) override;
    void test_make_hole(metadata::Collection& mds, unsigned hole_size, unsigned data_idx) override;
    void test_make_overlap(metadata::Collection& mds, unsigned overlap_size, unsigned data_idx) override;
    void test_corrupt(const metadata::Collection& mds, unsigned data_idx) override;
};

bool can_store(const std::string& format);

}
}
}
}
#endif

