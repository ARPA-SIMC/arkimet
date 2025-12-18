#ifndef ARKI_DATA_JPEG_H
#define ARKI_DATA_JPEG_H

#include <arki/data.h>
#include <cstdint>
#include <string>
#include <vector>

namespace arki::data {
class MockEngine;

namespace jpeg {
const Validator& validator();
}

class JPEGScanner : public Scanner
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
    bool scan_segment(std::shared_ptr<segment::Reader> reader,
                      metadata_dest_func dest) override;
    std::shared_ptr<Metadata>
    scan_singleton(const std::filesystem::path& abspath) override;
};

class MockJPEGScanner : public JPEGScanner
{
protected:
    MockEngine* engine;

    std::shared_ptr<Metadata>
    scan_jpeg_file(const std::filesystem::path& pathname) override;
    std::shared_ptr<Metadata>
    scan_jpeg_data(const std::vector<uint8_t>& data) override;

public:
    MockJPEGScanner();
    virtual ~MockJPEGScanner();
};

void register_jpeg_scanner();

} // namespace arki::data

#endif
