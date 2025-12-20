#include "netcdf.h"
#include "arki/metadata.h"
#include "arki/metadata/data.h"
#include "arki/segment.h"
#include "arki/types/source.h"
#include "arki/utils/string.h"
#include "arki/utils/sys.h"
#include <cstring>
#include <memory>
#include <sstream>
#include <stdexcept>
#include <string>
#include <unistd.h>
#include <vector>

using namespace std;
using namespace arki::types;
using namespace arki::utils;

namespace arki::data {
namespace netcdf {

/* taken from: http://www.hdfgroup.org/HDF5/doc/H5.format.html#Superblock */
static const unsigned char hdf5_sign[8]   = {0x89, 0x48, 0x44, 0x46,
                                             0x0d, 0x0a, 0x1a, 0x0a};
static const unsigned char nc3_32_sign[4] = {'C', 'D', 'F', 0x01};
static const unsigned char nc3_64_sign[4] = {'C', 'D', 'F', 0x02};
static const unsigned char nc5_sign[4]    = {'C', 'D', 'F', 0x05};

struct NetCDFValidator : public Validator
{
    DataFormat format() const override { return DataFormat::NETCDF; }

    void validate_file(sys::NamedFileDescriptor& fd, off_t offset,
                       size_t size) const override
    {
        if (size < 8)
            throw_check_error(
                fd, offset,
                "file segment to check is only " + std::to_string(size) +
                    " bytes (minimum required for NetCDF identification is 8)");

        /* we check that file header is a valid NetCDF or HDF5 header */
        char buf[8];
        ssize_t res;
        if ((res = fd.pread(buf, 8, offset)) != 8)
            throw_check_error(fd, offset,
                              "read only " + std::to_string(res) +
                                  "/8 bytes of NetCDF header");

        if (memcmp(buf, hdf5_sign, 8) == 0)
            return;

        if (memcmp(buf, nc3_32_sign, 4) == 0)
            return;

        if (memcmp(buf, nc3_64_sign, 4) == 0)
            return;

        if (memcmp(buf, nc5_sign, 4) == 0)
            return;

        throw_check_error(fd, offset, "invalid NetCDF or HDF5 header");
    }

    void validate_buf(const void* buf, size_t size) const override
    {
        /* we check that file header is a valid HDF5 header */

        if (size < 8)
            throw_check_error("buffer is shorter than 8 bytes");

        if (memcmp(buf, hdf5_sign, 8) == 0)
            return;

        if (memcmp(buf, nc3_32_sign, 4) == 0)
            return;

        if (memcmp(buf, nc3_64_sign, 4) == 0)
            return;

        if (memcmp(buf, nc5_sign, 4) == 0)
            return;

        throw_check_error(
            "buffer does not start with NetCDF or HDF5 signature");
    }
};

static NetCDFValidator netcdf_validator;

const Validator& validator() { return netcdf_validator; }

} // namespace netcdf

/*
 * NetCDFScanner
 */

std::shared_ptr<Metadata>
NetCDFScanner::scan_nc_data(const std::vector<uint8_t>& data)
{
    sys::Tempfile tmpfd;
    tmpfd.write_all_or_throw(data.data(), data.size());
    return scan_nc_file(tmpfd.path());
}

std::shared_ptr<Metadata>
NetCDFScanner::scan_data(const std::vector<uint8_t>& data)
{
    std::shared_ptr<Metadata> md = scan_nc_data(data);
    md->set_source_inline(DataFormat::NETCDF,
                          metadata::DataManager::get().to_data(
                              DataFormat::NETCDF, std::vector<uint8_t>(data)));
    return md;
}

std::shared_ptr<Metadata>
NetCDFScanner::scan_file_single(const std::filesystem::path& abspath)
{
    return scan_nc_file(abspath);
}

bool NetCDFScanner::scan_pipe(core::NamedFileDescriptor& in,
                              metadata_dest_func dest)
{
    // Read all in a buffer
    std::vector<uint8_t> buf;
    const unsigned blocksize = 4096;
    while (true)
    {
        buf.resize(buf.size() + blocksize);
        unsigned read = in.read(buf.data() + buf.size() - blocksize, blocksize);
        if (read < blocksize)
        {
            buf.resize(buf.size() - blocksize + read);
            break;
        }
    }

    return dest(scan_data(buf));
}

} // namespace arki::data
