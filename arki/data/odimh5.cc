#include "odimh5.h"
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

namespace arki::data::odimh5 {

/* taken from: http://www.hdfgroup.org/HDF5/doc/H5.format.html#Superblock */
static const unsigned char hdf5sign[8] = {0x89, 0x48, 0x44, 0x46,
                                          0x0d, 0x0a, 0x1a, 0x0a};

struct OdimH5Validator : public Validator
{
    DataFormat format() const override { return DataFormat::ODIMH5; }

    void validate_file(sys::NamedFileDescriptor& fd, off_t offset,
                       size_t size) const override
    {
        if (size < 8)
            throw_check_error(fd, offset,
                              "file segment to check is only " +
                                  std::to_string(size) +
                                  " bytes (minimum for a ODIMH5 is 8)");

        /* we check that file header is a valid HDF5 header */
        char buf[8];
        ssize_t res;
        if ((res = fd.pread(buf, 8, offset)) != 8)
            throw_check_error(fd, offset,
                              "read only " + std::to_string(res) +
                                  "/8 bytes of ODIMH5 header");

        if (memcmp(buf, hdf5sign, 8) != 0)
            throw_check_error(fd, offset, "invalid HDF5 header");
    }

    void validate_buf(const void* buf, size_t size) const override
    {
        /* we check that file header is a valid HDF5 header */

        if (size < 8)
            throw_check_error("buffer is shorter than 8 bytes");

        if (memcmp(buf, hdf5sign, 8) != 0)
            throw_check_error("buffer does not start with hdf5 signature");
    }
};

static OdimH5Validator odimh5_validator;

const Validator& validator() { return odimh5_validator; }

} // namespace arki::data::odimh5
