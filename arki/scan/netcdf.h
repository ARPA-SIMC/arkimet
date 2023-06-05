#ifndef ARKI_SCAN_NETCDF_H
#define ARKI_SCAN_NETCDF_H

#include <arki/scan.h>
#include <string>
#include <vector>
#include <cstdint>

namespace arki {
namespace scan {
class MockEngine;

namespace netcdf {
const Validator& validator();
}

class NetCDFScanner : public Scanner
{
    void set_blob_source(Metadata& md, std::shared_ptr<segment::Reader> reader);

protected:
    virtual std::shared_ptr<Metadata> scan_nc_file(const std::string& pathname) = 0;
    virtual std::shared_ptr<Metadata> scan_nc_data(const std::vector<uint8_t>& data);

public:
    std::string name() const override { return "nc"; }

    std::shared_ptr<Metadata> scan_data(const std::vector<uint8_t>& data) override;
    bool scan_pipe(core::NamedFileDescriptor& in, metadata_dest_func dest) override;
    bool scan_segment(std::shared_ptr<segment::Reader> reader, metadata_dest_func dest) override;
    std::shared_ptr<Metadata> scan_singleton(const std::string& abspath) override;
};


class MockNetCDFScanner : public NetCDFScanner
{
protected:
    MockEngine* engine;

    std::shared_ptr<Metadata> scan_nc_file(const std::string& pathname) override;
    std::shared_ptr<Metadata> scan_nc_data(const std::vector<uint8_t>& data) override;

public:
    MockNetCDFScanner();
    virtual ~MockNetCDFScanner();
};


void register_netcdf_scanner();

}
}

#endif

