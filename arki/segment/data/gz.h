#ifndef ARKI_SEGMENT_GZ_H
#define ARKI_SEGMENT_GZ_H

#include <arki/segment/data.h>
#include <arki/segment/data/base.h>
#include <arki/utils/sys.h>
#include <arki/utils/compress.h>
#include <string>

namespace arki::segment::data {
namespace gz {

class Data : public arki::segment::Data
{
public:
    using arki::segment::Data::Data;
    time_t timestamp() const override;
    static bool can_store(const std::string& format);
};

template<typename Data>
class Reader : public data::BaseReader<Data>
{
public:
    core::File fd;
    utils::compress::SeekIndexReader reader;

    Reader(std::shared_ptr<const Data> data, std::shared_ptr<core::Lock> lock);

    bool scan_data(metadata_dest_func dest) override;
    void reposition(off_t ofs);
    std::vector<uint8_t> read(const types::source::Blob& src) override;
};

template<typename Data>
class Checker : public data::BaseChecker<Data>
{
protected:
    std::filesystem::path gzabspath;
    std::filesystem::path gzidxabspath;

    void move_data(const std::filesystem::path& new_root, const std::filesystem::path& new_relpath, const std::filesystem::path& new_abspath) override;

public:
    explicit Checker(std::shared_ptr<const Data> data);

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

class Data : public gz::Data
{
public:
    using gz::Data::Data;
    const char* type() const override;
    bool single_file() const override;

    std::shared_ptr<segment::data::Reader> reader(std::shared_ptr<core::Lock> lock) const override;
    std::shared_ptr<segment::data::Writer> writer(const data::WriterConfig& config, bool mock_data) const override;
    std::shared_ptr<segment::data::Checker> checker(bool mock_data) const override;

    static bool can_store(const std::string& format);
    static std::shared_ptr<Checker> create(const Segment& segment, metadata::Collection& mds, const RepackConfig& cfg);
    static const unsigned padding = 0;
};

class Reader : public gz::Reader<Data>
{
public:
    using gz::Reader<Data>::Reader;
};

class Checker : public gz::Checker<Data>
{
public:
    using gz::Checker<Data>::Checker;
};


}


namespace gzlines {

struct Data : public gz::Data
{
    using gz::Data::Data;
    const char* type() const override;
    bool single_file() const override;

    std::shared_ptr<segment::data::Reader> reader(std::shared_ptr<core::Lock> lock) const override;
    std::shared_ptr<segment::data::Writer> writer(const data::WriterConfig& config, bool mock_data) const override;
    std::shared_ptr<segment::data::Checker> checker(bool mock_data) const override;

    static bool can_store(const std::string& format);
    static std::shared_ptr<Checker> make_checker(const std::string& format, const std::filesystem::path& rootdir, const std::filesystem::path& relpath, const std::filesystem::path& abspath);
    static std::shared_ptr<Checker> create(const Segment& segment, metadata::Collection& mds, const RepackConfig& cfg);
    static const unsigned padding = 1;
};


struct Reader : public gz::Reader<Data>
{
    using gz::Reader<Data>::Reader;
};


struct Checker : public gz::Checker<Data>
{
    using gz::Checker<Data>::Checker;
};


}

}
#endif
