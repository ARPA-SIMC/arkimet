#ifndef ARKI_DATASET_SEGMENT_FD_H
#define ARKI_DATASET_SEGMENT_FD_H

/// Base class for unix fd based read/write functions

#include <arki/libconfig.h>
#include <arki/dataset/segment.h>
#include <arki/file.h>
#include <string>

namespace arki {
struct Reader;

namespace dataset {
namespace segment {
namespace fd {

/**
 * Customize in subclasses to add format-specific I/O
 */
struct File : public arki::File
{
    using arki::File::File;

    void fdtruncate(off_t pos);
    virtual void write_data(off_t wrpos, const std::vector<uint8_t>& buf) = 0;
    virtual void test_add_padding(size_t size) = 0;
};


struct Writer : public dataset::segment::Writer
{
    File* fd = nullptr;

    Writer(const std::string& root, const std::string& relname, std::unique_ptr<File> fd);
    ~Writer();

    Pending append(Metadata& md, const types::source::Blob** new_source=0) override;

    /**
     * Append raw data to the file, wrapping it with the right envelope if
     * needed.
     *
     * All exceptions are propagated upwards without special handling. If this
     * operation fails, the file should be considered invalid.
     *
     * This function is intended to be used by low-level maintenance operations,
     * like a file repack.
     *
     * @return the offset at which the buffer is written
     */
    off_t append(const std::vector<uint8_t>& buf);

    void truncate(size_t offset) override;

    void test_add_padding(unsigned size) override;
    void test_make_overlap(metadata::Collection& mds, unsigned overlap_size, unsigned data_idx) override;
    void test_make_hole(metadata::Collection& mds, unsigned hole_size, unsigned data_idx) override;
    void test_corrupt(const metadata::Collection& mds, unsigned data_idx) override;
};


class Checker : public dataset::segment::Checker
{
protected:
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

    virtual std::unique_ptr<fd::Writer> make_tmp_segment(const std::string& relname, const std::string& absname) = 0;

public:
    using dataset::segment::Checker::Checker;

    size_t remove() override;

    State check_fd(dataset::Reporter& reporter, const std::string& ds, const metadata::Collection& mds, unsigned max_gap=0, bool quick=true);
};

bool can_store(const std::string& format);

}
}
}
}
#endif

