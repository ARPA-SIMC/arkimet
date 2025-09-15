#ifndef ARKI_SEGMENT_ZIP_H
#define ARKI_SEGMENT_ZIP_H

/// Base class for unix fd based read/write functions

#include <arki/core/file.h>
#include <arki/segment/data.h>
#include <arki/segment/data/base.h>
#include <arki/utils/zip.h>
#include <string>

namespace arki {
struct Reader;

namespace segment::data::zip {

/**
 * Data stored in .zip files.
 *
 * zip file contain one file per data entry, named `<offset>.<format>` where
 * offset is the sequential position in the file and format is the data format.
 */
struct Data : public arki::segment::Data
{
    std::filesystem::path zipabspath;

    Data(std::shared_ptr<const Segment> segment);

    const char* type() const override;
    bool single_file() const override;
    std::optional<time_t> timestamp() const override;
    bool exists_on_disk() const override;
    bool is_empty() const override;
    size_t size() const override;
    utils::files::PreserveFileTimes preserve_mtime() override;
    size_t next_offset(size_t offset, size_t size) const override
    {
        return offset + 1;
    }

    std::shared_ptr<segment::data::Reader>
    reader(std::shared_ptr<const core::ReadLock> lock) const override;
    std::shared_ptr<segment::data::Writer>
    writer(const segment::WriterConfig& config) const override;
    std::shared_ptr<segment::data::Checker> checker() const override;
    void create_segment(arki::metadata::Collection& mds,
                        const RepackConfig& cfg = RepackConfig()) override
    {
        create(*m_segment, mds, cfg);
    }

    static bool can_store(DataFormat format);
    static std::shared_ptr<const Data>
    create(const Segment& segment, arki::metadata::Collection& mds,
           const RepackConfig& cfg = RepackConfig());
};

struct Reader : public data::BaseReader<Data>
{
    utils::ZipReader zip;

    Reader(std::shared_ptr<const Data> data,
           std::shared_ptr<const core::ReadLock> lock);

    bool scan_data(metadata_dest_func dest) override;
    std::vector<uint8_t> read(const types::source::Blob& src) override;
};

class Checker : public data::BaseChecker<Data>
{
protected:
    void validate(Metadata& md, const arki::scan::Validator& v);

    /**
     * If skip_validation is true, repack will skip validating the data that is
     * being read.
     *
     * This is only used during tests to support repacking files with mock data
     * inside. The files are made of filesystem holes, so the data that is read
     * from them is always zeroes.
     */
    core::Pending repack_impl(const std::filesystem::path& rootdir,
                              arki::metadata::Collection& mds,
                              bool skip_validation    = false,
                              const RepackConfig& cfg = RepackConfig());
    void move_data(std::shared_ptr<const Segment> new_segment) override;

public:
    Checker(std::shared_ptr<const Data> data);

    bool rescan_data(std::function<void(const std::string&)> reporter,
                     std::shared_ptr<const core::ReadLock> lock,
                     metadata_dest_func dest) override;
    State check(std::function<void(const std::string&)> reporter,
                const arki::metadata::Collection& mds,
                bool quick = true) override;
    size_t remove() override;
    core::Pending repack(arki::metadata::Collection& mds,
                         const RepackConfig& cfg = RepackConfig()) override;

    void test_truncate(size_t offset) override;
    void test_make_hole(arki::metadata::Collection& mds, unsigned hole_size,
                        unsigned data_idx) override;
    void test_make_overlap(arki::metadata::Collection& mds,
                           unsigned overlap_size, unsigned data_idx) override;
    void test_corrupt(const arki::metadata::Collection& mds,
                      unsigned data_idx) override;
    void test_touch_contents(time_t timestamp) override;
};

} // namespace segment::data::zip
} // namespace arki
#endif
