#ifndef ARKI_DATA_JPEG_H
#define ARKI_DATA_JPEG_H

#include <arki/data.h>
#include <cstdint>
#include <string>
#include <vector>

namespace arki::data {

namespace jpeg {
const Validator& validator();
}

class JPEGScanner : public SingleFileScanner
{
    void set_blob_source(Metadata& md, std::shared_ptr<segment::Reader> reader);

protected:
    virtual std::shared_ptr<Metadata>
    scan_jpeg_file(const std::filesystem::path& pathname) = 0;
    virtual std::shared_ptr<Metadata>
    scan_jpeg_data(const std::vector<uint8_t>& data) = 0;

public:
    DataFormat name() const override { return DataFormat::JPEG; }

    std::shared_ptr<Metadata>
    scan_data(const std::vector<uint8_t>& data) override;
    bool scan_pipe(core::NamedFileDescriptor& in,
                   metadata_dest_func dest) override;
    std::shared_ptr<Metadata>
    scan_file_single(const std::filesystem::path& abspath) override;
};

} // namespace arki::data

#endif
