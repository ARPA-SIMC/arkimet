#ifndef ARKI_DATASET_DATA_DIR_H
#define ARKI_DATASET_DATA_DIR_H

/// Directory based data collection

#include <arki/defs.h>
#include <arki/dataset/segment.h>
#include <arki/dataset/segment/seqfile.h>
#include <arki/core/file.h>
#include <vector>

namespace arki {
class Metadata;

namespace dataset {
namespace segment {
namespace dir {


struct Writer : public dataset::segment::Writer
{
    SequenceFile seqfile;
    std::string format;
    std::vector<std::string> written;
    std::vector<PendingMetadata> pending;
    size_t current_pos;

    Writer(const std::string& format, const std::string& root, const std::string& relname, const std::string& absname);
    ~Writer();

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

    void write_file(Metadata& md, core::NamedFileDescriptor& fd) override;
};


class Checker : public dataset::segment::Checker
{
public:
    std::string format;

    void validate(Metadata& md, const scan::Validator& v) override;

public:
    Checker(const std::string& format, const std::string& root, const std::string& relname, const std::string& absname);

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
};

bool can_store(const std::string& format);

}
}
}
}
#endif
