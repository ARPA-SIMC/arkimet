#ifndef ARKI_SEGMENT_GZ_H
#define ARKI_SEGMENT_GZ_H

/// Base class for unix fd based read/write functions

#include <arki/segment.h>
#include <arki/utils/gzip.h>
#include <string>

namespace arki {
struct Reader;

namespace segment {
namespace gz {

struct Segment : public arki::Segment
{
    using arki::Segment::Segment;
};

struct Reader : public segment::Reader
{
    utils::gzip::File fd;
    uint64_t last_ofs = 0;

    Reader(const std::string& abspath, std::shared_ptr<core::Lock> lock);

    time_t timestamp() override;

    bool scan_data(metadata_dest_func dest) override;
    std::vector<uint8_t> read(const types::source::Blob& src) override;
    size_t stream(const types::source::Blob& src, core::NamedFileDescriptor& out) override;
};

class Checker : public segment::Checker
{
protected:
    std::string gzabspath;

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
    void move_data(const std::string& new_root, const std::string& new_relpath, const std::string& new_abspath) override;

public:
    Checker(const std::string& format, const std::string& root, const std::string& relpath, const std::string& abspath);

    const char* type() const override;
    bool single_file() const override;

    bool exists_on_disk() override;
    time_t timestamp() override;
    size_t size() override;

    State check(std::function<void(const std::string&)> reporter, const metadata::Collection& mds, bool quick=true) override;
    size_t remove() override;
    Pending repack(const std::string& rootdir, metadata::Collection& mds, unsigned test_flags=0) override;

    void test_truncate(size_t offset) override;
    void test_make_hole(metadata::Collection& mds, unsigned hole_size, unsigned data_idx) override;
    void test_make_overlap(metadata::Collection& mds, unsigned overlap_size, unsigned data_idx) override;
    void test_corrupt(const metadata::Collection& mds, unsigned data_idx) override;
};

}


namespace gzconcat {

struct Segment : public gz::Segment
{
    using gz::Segment::Segment;
    const char* type() const override;
    bool single_file() const override;
};

class Reader : public gz::Reader
{
protected:
    Segment m_segment;

public:
    Reader(const std::string& format, const std::string& root, const std::string& relpath, const std::string& abspath, std::shared_ptr<core::Lock> lock);
    const Segment& segment() const override;
};

class Checker : public gz::Checker
{
public:
    using gz::Checker::Checker;

    std::shared_ptr<segment::Reader> reader(std::shared_ptr<core::Lock> lock) override;

    static std::shared_ptr<Checker> create(const std::string& format, const std::string& rootdir, const std::string& relpath, const std::string& abspath, metadata::Collection& mds, unsigned test_flags=0);
    static bool can_store(const std::string& format);
};


}


namespace gzlines {

struct Segment : public gz::Segment
{
    using gz::Segment::Segment;
    const char* type() const override;
    bool single_file() const override;
};


class Reader : public gz::Reader
{
protected:
    Segment m_segment;

public:
    Reader(const std::string& format, const std::string& root, const std::string& relpath, const std::string& abspath, std::shared_ptr<core::Lock> lock);
    const Segment& segment() const override;
};


class Checker : public gz::Checker
{
public:
    using gz::Checker::Checker;

    std::shared_ptr<segment::Reader> reader(std::shared_ptr<core::Lock> lock) override;
    State check(std::function<void(const std::string&)> reporter, const metadata::Collection& mds, bool quick=true) override;
    Pending repack(const std::string& rootdir, metadata::Collection& mds, unsigned test_flags=0) override;

    static std::shared_ptr<Checker> create(const std::string& format, const std::string& rootdir, const std::string& relpath, const std::string& abspath, metadata::Collection& mds, unsigned test_flags=0);
    static bool can_store(const std::string& format);
};


}

}
}
#endif


