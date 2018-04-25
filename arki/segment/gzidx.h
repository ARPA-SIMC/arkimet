#ifndef ARKI_SEGMENT_GZIDX_H
#define ARKI_SEGMENT_GZIDX_H

/// Base class for unix fd based read/write functions

#include <arki/segment.h>
#include <arki/utils/compress.h>
#include <arki/utils/gzip.h>
#include <string>

namespace arki {
struct Reader;

namespace segment {
namespace gzidx {

struct Segment : public arki::Segment
{
    using arki::Segment::Segment;
    time_t timestamp() const override;
};


struct Reader : public segment::Reader
{
    utils::compress::SeekIndex idx;
    size_t last_block = 0;
    core::File fd;
    utils::gzip::File gzfd;
    uint64_t last_ofs = 0;

    Reader(const std::string& abspath, std::shared_ptr<core::Lock> lock);

    bool scan_data(metadata_dest_func dest) override;
    void reposition(off_t ofs);
    std::vector<uint8_t> read(const types::source::Blob& src) override;
    size_t stream(const types::source::Blob& src, core::NamedFileDescriptor& out) override;
};


class Checker : public segment::Checker
{
protected:
    std::string gzabspath;
    std::string gzidxabspath;

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
    Checker(const std::string& abspath);

    bool exists_on_disk() override;
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

namespace gzidxconcat {

struct Segment : public gzidx::Segment
{
    using gzidx::Segment::Segment;
    const char* type() const override;
    bool single_file() const override;
    std::shared_ptr<segment::Reader> reader(std::shared_ptr<core::Lock> lock) const override;
    std::shared_ptr<segment::Checker> checker() const override;
    static bool can_store(const std::string& format);
    static std::shared_ptr<Checker> make_checker(const std::string& format, const std::string& rootdir, const std::string& relpath, const std::string& abspath);
    static std::shared_ptr<Checker> create(const std::string& format, const std::string& rootdir, const std::string& relpath, const std::string& abspath, metadata::Collection& mds, unsigned test_flags=0);
};


class Reader : public gzidx::Reader
{
protected:
    Segment m_segment;

public:
    Reader(const std::string& format, const std::string& root, const std::string& relpath, const std::string& abspath, std::shared_ptr<core::Lock> lock);
    const Segment& segment() const override;
};


class Checker : public gzidx::Checker
{
protected:
    Segment m_segment;

public:
    Checker(const std::string& format, const std::string& root, const std::string& relpath, const std::string& abspath);

    const Segment& segment() const override;

    static std::shared_ptr<Checker> create(const std::string& format, const std::string& rootdir, const std::string& relpath, const std::string& abspath, metadata::Collection& mds, unsigned test_flags=0);
};

}

namespace gzidxlines {

struct Segment : public gzidx::Segment
{
    using gzidx::Segment::Segment;
    const char* type() const override;
    bool single_file() const override;
    std::shared_ptr<segment::Reader> reader(std::shared_ptr<core::Lock> lock) const override;
    std::shared_ptr<segment::Checker> checker() const override;
    static bool can_store(const std::string& format);
    static std::shared_ptr<Checker> make_checker(const std::string& format, const std::string& rootdir, const std::string& relpath, const std::string& abspath);
    static std::shared_ptr<Checker> create(const std::string& format, const std::string& rootdir, const std::string& relpath, const std::string& abspath, metadata::Collection& mds, unsigned test_flags=0);
};


class Reader : public gzidx::Reader
{
protected:
    Segment m_segment;

public:
    Reader(const std::string& format, const std::string& root, const std::string& relpath, const std::string& abspath, std::shared_ptr<core::Lock> lock);
    const Segment& segment() const override;
};


class Checker : public gzidx::Checker
{
protected:
    Segment m_segment;

public:
    Checker(const std::string& format, const std::string& root, const std::string& relpath, const std::string& abspath);

    const Segment& segment() const override;
    State check(std::function<void(const std::string&)> reporter, const metadata::Collection& mds, bool quick=true) override;
    Pending repack(const std::string& rootdir, metadata::Collection& mds, unsigned test_flags=0) override;
};

}

}
}
#endif


