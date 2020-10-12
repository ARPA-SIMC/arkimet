#include "netcdf.h"
#include "arki/libconfig.h"
#include "arki/metadata.h"
#include "arki/metadata/data.h"
#include "arki/segment.h"
#include "arki/types/source.h"
#include "arki/types/origin.h"
#include "arki/types/reftime.h"
#include "arki/types/task.h"
#include "arki/types/quantity.h"
#include "arki/types/product.h"
#include "arki/types/level.h"
#include "arki/types/area.h"
#include "arki/types/timerange.h"
#include "arki/runtime.h"
#include "arki/utils/string.h"
#include "arki/utils/files.h"
#include "arki/utils/sys.h"
#include "arki/scan/validator.h"
#include "arki/scan/mock.h"
#include <cstring>
#include <unistd.h>
#include <fcntl.h>
#include <vector>
#include <stdexcept>
#include <set>
#include <string>
#include <sstream>
#include <memory>

using namespace std;
using namespace arki::types;
using namespace arki::utils;

namespace arki {
namespace scan {
namespace netcdf {

/* taken from: http://www.hdfgroup.org/HDF5/doc/H5.format.html#Superblock */
static const unsigned char hdf5_sign[8] = { 0x89, 0x48, 0x44, 0x46, 0x0d, 0x0a, 0x1a, 0x0a };
static const unsigned char nc3_32_sign[4] = { 'C', 'D', 'F', 0x01 };
static const unsigned char nc3_64_sign[4] = { 'C', 'D', 'F', 0x02 };
static const unsigned char nc5_sign[4] = { 'C', 'D', 'F', 0x05 };

struct NetCDFValidator : public Validator
{
    std::string format() const override { return "NetCDF"; }

    void validate_file(sys::NamedFileDescriptor& fd, off_t offset, size_t size) const override
    {
        if (size < 8)
            throw_check_error(fd, offset, "file segment to check is only " + std::to_string(size) + " bytes (minimum required for NetCDF identification is 8)");

        /* we check that file header is a valid NetCDF or HDF5 header */
        char buf[8];
        ssize_t res;
        if ((res = fd.pread(buf, 8, offset)) != 8)
            throw_check_error(fd, offset, "read only " + std::to_string(res) + "/8 bytes of NetCDF header");

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

        throw_check_error("buffer does not start with NetCDF or HDF5 signature");
    }
};

static NetCDFValidator netcdf_validator;

const Validator& validator() { return netcdf_validator; }

}

}
}

