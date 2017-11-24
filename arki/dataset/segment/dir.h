#ifndef ARKI_DATASET_DATA_DIR_H
#define ARKI_DATASET_DATA_DIR_H

/// Directory based data collection

#include <arki/defs.h>
#include <arki/dataset/segment.h>
#include <arki/dataset/segment/seqfile.h>
#include <arki/file.h>
#include <arki/utils/lock.h>

namespace arki {
class Metadata;

namespace dataset {
namespace segment {
namespace dir {


struct Writer : public dataset::segment::Writer
{
    SequenceFile seqfile;
    File write_lock_file;
    utils::Lock lock;
    std::string format;

    Writer(const std::string& format, const std::string& root, const std::string& relname, const std::string& absname);
    ~Writer();

    Pending append(Metadata& md, const types::source::Blob** new_source=0) override;

    virtual size_t write_file(Metadata& md, File& fd);

    /// Call f for each nnnnnn.format file in the directory segment, passing the file name
    void foreach_datafile(std::function<void(const char*)> f);

    /*
     * Append a hardlink to the data pointed by md.
     *
     * This of course only works if it is possible to hardlink from this
     * segment to the file pointed by md. That is, if they are in the same file
     * system.
     *
     * @returns the offset in the segment at which md was appended
     */
    off_t link(const std::string& absname);
};

struct HoleWriter: public Writer
{
    using Writer::Writer;

    size_t write_file(Metadata& md, File& fd) override;
};


class Checker : public dataset::segment::Checker
{
public:
    std::string format;

    void validate(Metadata& md, const scan::Validator& v) override;

public:
    Checker(const std::string& format, const std::string& root, const std::string& relname, const std::string& absname);

    void lock() override;

    bool exists_on_disk() override;

    State check(dataset::Reporter& reporter, const std::string& ds, const metadata::Collection& mds, bool quick=true) override;
    size_t remove() override;
    Pending repack(const std::string& rootdir, metadata::Collection& mds, unsigned test_flags=0) override;

    /// Call f for each nnnnnn.format file in the directory segment, passing the file name
    void foreach_datafile(std::function<void(const char*)> f);

    void test_truncate(size_t offset) override;
    void test_make_hole(metadata::Collection& mds, unsigned hole_size, unsigned data_idx) override;
    void test_make_overlap(metadata::Collection& mds, unsigned overlap_size, unsigned data_idx) override;
    void test_corrupt(const metadata::Collection& mds, unsigned data_idx) override;

protected:
    virtual std::unique_ptr<dir::Writer> make_tmp_segment(const std::string& format, const std::string& relname, const std::string& absname);
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

    State check(dataset::Reporter& reporter, const std::string& ds, const metadata::Collection& mds, bool quick=true) override;

protected:
    std::unique_ptr<dir::Writer> make_tmp_segment(const std::string& format, const std::string& relname, const std::string& absname) override;
};

bool can_store(const std::string& format);

}
}
}
}
#endif
