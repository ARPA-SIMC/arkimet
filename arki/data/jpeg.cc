#include "jpeg.h"
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
namespace jpeg {

struct JPEGValidator : public Validator
{
    // For reference about signatures, see
    // https://en.wikipedia.org/wiki/JPEG#Syntax_and_structure

    DataFormat format() const override { return DataFormat::JPEG; }

    void validate_file(sys::NamedFileDescriptor& fd, off_t offset,
                       size_t size) const override
    {
        if (size < 4)
            throw_check_error(
                fd, offset,
                "file segment to check is only " + std::to_string(size) +
                    " bytes (minimum required for JPEG identification is 4)");

        // check that the file begins and ends with the right markers
        unsigned char buf[2];
        ssize_t res;
        if ((res = fd.pread(buf, 2, offset)) != 2)
            throw_check_error(fd, offset,
                              "read only " + std::to_string(res) +
                                  "/2 bytes of JPEG header");

        if (buf[0] != 0xff || buf[1] != 0xd8)
            throw_check_error(fd, offset,
                              "JPEG Start Of Image signature not found");

        if ((res = fd.pread(buf, 2, offset + size - 2)) != 2)
            throw_check_error(fd, offset,
                              "read only " + std::to_string(res) +
                                  "/2 bytes of JPEG trailer");

        if (buf[0] != 0xff || buf[1] != 0xd9)
            throw_check_error(fd, offset,
                              "JPEG End Of Image signature not found");
    }

    void validate_buf(const void* buf, size_t size) const override
    {
        /* we check that file header is a valid HDF5 header */

        if (size < 4)
            throw_check_error("buffer is shorter than 4 bytes");

        const unsigned char* chunk =
            reinterpret_cast<const unsigned char*>(buf);
        if (chunk[0] != 0xff || chunk[1] != 0xd8)
            throw_check_error("JPEG Start Of Image signature not found");

        chunk += size - 2;
        if (chunk[0] != 0xff || chunk[1] != 0xd9)
            throw_check_error("JPEG End Of Image signature not found");
    }
};

static JPEGValidator jpeg_validator;

const Validator& validator() { return jpeg_validator; }

} // namespace jpeg

} // namespace arki::data
