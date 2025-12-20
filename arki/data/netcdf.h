#ifndef ARKI_DATA_NETCDF_H
#define ARKI_DATA_NETCDF_H

#include <arki/data.h>
#include <cstdint>
#include <string>
#include <vector>

namespace arki::data {

namespace netcdf {
const Validator& validator();
}

class NetCDFScanner : public SingleFileScanner
{
    void set_blob_source(Metadata& md, std::shared_ptr<segment::Reader> reader);

protected:
    virtual std::shared_ptr<Metadata>
    scan_nc_file(const std::filesystem::path& pathname) = 0;
    virtual std::shared_ptr<Metadata>
    scan_nc_data(const std::vector<uint8_t>& data);

public:
    DataFormat name() const override { return DataFormat::NETCDF; }

    std::shared_ptr<Metadata>
    scan_data(const std::vector<uint8_t>& data) override;
    bool scan_pipe(core::NamedFileDescriptor& in,
                   metadata_dest_func dest) override;
    std::shared_ptr<Metadata>
    scan_file_single(const std::filesystem::path& abspath) override;
};

} // namespace arki::data

#endif
