#ifndef ARKI_SEGMENT_GZ_H
#define ARKI_SEGMENT_GZ_H

#include <arki/segment.h>
#include <arki/segment/base.h>
#include <arki/utils/sys.h>
#include <arki/utils/compress.h>
#include <string>

namespace arki {
namespace segment {
namespace gz {

class Segment : public arki::Segment
{
public:
    using arki::Segment::Segment;
    time_t timestamp() const override;
    static bool can_store(const std::string& format);
};

template<typename Segment>
class Reader : public segment::BaseReader<Segment>
{
public:
    core::File fd;
    utils::compress::SeekIndexReader reader;

    Reader(const std::string& format, const std::filesystem::path& root, const std::filesystem::path& relpath, const std::filesystem::path& abspath, std::shared_ptr<core::Lock> lock);

    bool scan_data(metadata_dest_func dest) override;
    void reposition(off_t ofs);
    std::vector<uint8_t> read(const types::source::Blob& src) override;
};

template<typename Segment>
class Checker : public segment::BaseChecker<Segment>
{
protected:
    std::filesystem::path gzabspath;
    std::filesystem::path gzidxabspath;

    void move_data(const std::filesystem::path& new_root, const std::filesystem::path& new_relpath, const std::filesystem::path& new_abspath) override;

public:
    Checker(const std::string& format, const std::filesystem::path& root, const std::filesystem::path& relpath, const std::filesystem::path& abspath);

    bool exists_on_disk() override;
    size_t size() override;
    bool is_empty() override;

    bool rescan_data(std::function<void(const std::string&)> reporter, std::shared_ptr<core::Lock> lock, metadata_dest_func dest) override;
    State check(std::function<void(const std::string&)> reporter, const metadata::Collection& mds, bool quick=true) override;
    size_t remove() override;
    core::Pending repack(const std::filesystem::path& rootdir, metadata::Collection& mds, const RepackConfig& cfg=RepackConfig()) override;

    void test_truncate(size_t offset) override;
    void test_make_hole(metadata::Collection& mds, unsigned hole_size, unsigned data_idx) override;
    void test_make_overlap(metadata::Collection& mds, unsigned overlap_size, unsigned data_idx) override;
    void test_corrupt(const metadata::Collection& mds, unsigned data_idx) override;
};

}


namespace gzconcat {

class Segment : public gz::Segment
{
public:
    using gz::Segment::Segment;
    const char* type() const override;
    bool single_file() const override;
    std::shared_ptr<segment::Reader> reader(std::shared_ptr<core::Lock> lock) const override;
    std::shared_ptr<segment::Checker> checker() const override;
    static bool can_store(const std::string& format);
    static std::shared_ptr<Checker> make_checker(const std::string& format, const std::filesystem::path& rootdir, const std::filesystem::path& relpath, const std::filesystem::path& abspath);
    static std::shared_ptr<Checker> create(const std::string& format, const std::filesystem::path& rootdir, const std::filesystem::path& relpath, const std::filesystem::path& abspath, metadata::Collection& mds, const RepackConfig& cfg);
    static const unsigned padding = 0;
};

class Reader : public gz::Reader<Segment>
{
public:
    using gz::Reader<Segment>::Reader;
};

class Checker : public gz::Checker<Segment>
{
public:
    using gz::Checker<Segment>::Checker;
};


}


namespace gzlines {

struct Segment : public gz::Segment
{
    using gz::Segment::Segment;
    const char* type() const override;
    bool single_file() const override;
    std::shared_ptr<segment::Reader> reader(std::shared_ptr<core::Lock> lock) const override;
    std::shared_ptr<segment::Checker> checker() const override;
    static bool can_store(const std::string& format);
    static std::shared_ptr<Checker> make_checker(const std::string& format, const std::filesystem::path& rootdir, const std::filesystem::path& relpath, const std::filesystem::path& abspath);
    static std::shared_ptr<Checker> create(const std::string& format, const std::filesystem::path& rootdir, const std::filesystem::path& relpath, const std::filesystem::path& abspath, metadata::Collection& mds, const RepackConfig& cfg);
    static const unsigned padding = 1;
};


struct Reader : public gz::Reader<Segment>
{
    using gz::Reader<Segment>::Reader;
};


struct Checker : public gz::Checker<Segment>
{
    using gz::Checker<Segment>::Checker;
};


}

}
}
#endif


