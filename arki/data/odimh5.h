#ifndef ARKI_DATA_ODIMH5_H
#define ARKI_DATA_ODIMH5_H

#include <arki/data.h>
#include <cstdint>
#include <string>
#include <vector>

namespace arki::data {
class MockEngine;

namespace odimh5 {
const Validator& validator();
}

class OdimScanner : public Scanner
{
    void set_blob_source(Metadata& md, std::shared_ptr<segment::Reader> reader);

protected:
    virtual std::shared_ptr<Metadata>
    scan_h5_file(const std::filesystem::path& pathname) = 0;
    virtual std::shared_ptr<Metadata>
    scan_h5_data(const std::vector<uint8_t>& data);

public:
    DataFormat name() const override { return DataFormat::ODIMH5; }

    std::shared_ptr<Metadata>
    scan_data(const std::vector<uint8_t>& data) override;
    bool scan_pipe(core::NamedFileDescriptor& in,
                   metadata_dest_func dest) override;
    bool scan_segment(std::shared_ptr<segment::Reader> reader,
                      metadata_dest_func dest) override;
    std::shared_ptr<Metadata>
    scan_singleton(const std::filesystem::path& abspath) override;
};

class MockOdimScanner : public OdimScanner
{
protected:
    std::shared_ptr<Metadata>
    scan_h5_file(const std::filesystem::path& pathname) override;
    std::shared_ptr<Metadata>
    scan_h5_data(const std::vector<uint8_t>& data) override;
};

void register_odimh5_scanner();

} // namespace arki::data

#endif
