#ifndef ARKI_SEGMENT_TAR_H
#define ARKI_SEGMENT_TAR_H

/// Base class for unix fd based read/write functions

#include <arki/libconfig.h>
#include <arki/segment.h>
#include <arki/segment/base.h>
#include <arki/core/file.h>
#include <string>

namespace arki {
struct Reader;

namespace segment {
namespace tar {

struct Segment : public arki::Segment
{
    using arki::Segment::Segment;

    const char* type() const override;
    bool single_file() const override;
    time_t timestamp() const override;
    std::shared_ptr<segment::Reader> reader(std::shared_ptr<core::Lock> lock) const override;
    std::shared_ptr<segment::Checker> checker() const override;
    static bool can_store(const std::string& format);
    static std::shared_ptr<Checker> make_checker(const std::string& format, const std::string& rootdir, const std::string& relpath, const std::string& abspath);
    static std::shared_ptr<Checker> create(const std::string& format, const std::string& rootdir, const std::string& relpath, const std::string& abspath, metadata::Collection& mds, const RepackConfig& cfg=RepackConfig());
};


struct Reader : public segment::BaseReader<Segment>
{
    core::File fd;

    Reader(const std::string& format, const std::string& root, const std::string& relpath, const std::string& abspath, std::shared_ptr<core::Lock> lock);

    bool scan_data(metadata_dest_func dest) override;
    std::vector<uint8_t> read(const types::source::Blob& src) override;
    size_t stream(const types::source::Blob& src, core::NamedFileDescriptor& out) override;
};


class Checker : public segment::BaseChecker<Segment>
{
protected:
    std::string tarabspath;
    void validate(Metadata& md, const scan::Validator& v);

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
            const RepackConfig& cfg=RepackConfig());
    void move_data(const std::string& new_root, const std::string& new_relpath, const std::string& new_abspath) override;

public:
    Checker(const std::string& format, const std::string& root, const std::string& relpath, const std::string& abspath);

    bool exists_on_disk() override;
    bool is_empty() override;
    size_t size() override;

    bool rescan_data(std::shared_ptr<core::Lock> lock, metadata_dest_func dest) override;
    State check(std::function<void(const std::string&)> reporter, const metadata::Collection& mds, bool quick=true) override;
    size_t remove() override;
    Pending repack(const std::string& rootdir, metadata::Collection& mds, const RepackConfig& cfg=RepackConfig()) override;

    void test_truncate(size_t offset) override;
    void test_make_hole(metadata::Collection& mds, unsigned hole_size, unsigned data_idx) override;
    void test_make_overlap(metadata::Collection& mds, unsigned overlap_size, unsigned data_idx) override;
    void test_corrupt(const metadata::Collection& mds, unsigned data_idx) override;
};

}
}
}
#endif

