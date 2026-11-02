#include "bufr.h"
#include "arki/libconfig.h"
#include "arki/metadata.h"
#include "arki/metadata/data.h"
#include "arki/segment.h"
#include "arki/types/origin.h"
#include "arki/types/product.h"
#include "arki/types/reftime.h"
#include "arki/types/source.h"
#include "arki/types/source/blob.h"
#include "arki/types/timerange.h"
#include "arki/types/values.h"
#include "arki/utils/files.h"
#include "arki/utils/sys.h"
#include <cstring>
#include <sstream>

using namespace std;
using namespace arki::types;
using namespace arki::utils;

namespace arki::data::bufr {

struct BufrValidator : public Validator
{
    DataFormat format() const override { return DataFormat::BUFR; }

    // Validate data found in a file
    void validate_file(sys::NamedFileDescriptor& fd, off_t offset,
                       size_t size) const override
    {
        if (size < 8)
            throw_check_error(fd, offset,
                              "file segment to check is only " +
                                  std::to_string(size) +
                                  " bytes (minimum for a BUFR is 8)");

        char buf[4];
        ssize_t res;
        if ((res = fd.pread(buf, 4, offset)) != 4)
            throw_check_error(fd, offset,
                              "read only " + std::to_string(res) +
                                  "/4 bytes of BUFR header");
        if (memcmp(buf, "BUFR", 4) != 0)
            throw_check_error(fd, offset, "data does not start with 'BUFR'");
        if ((res = fd.pread(buf, 4, offset + size - 4)) != 4)
            throw_check_error(fd, offset,
                              "read only " + std::to_string(res) +
                                  "/4 bytes of BUFR trailer");
        if (memcmp(buf, "7777", 4) != 0)
            throw_check_error(fd, offset, "data does not end with '7777'");
    }

    // Validate a memory buffer
    void validate_buf(const void* buf, size_t size) const override
    {
        if (size < 8)
            throw_check_error("buffer is shorter than 8 bytes");
        if (memcmp(buf, "BUFR", 4) != 0)
            throw_check_error("buffer does not start with 'BUFR'");
        if (memcmp((const char*)buf + size - 4, "7777", 4) != 0)
            throw_check_error("buffer does not end with '7777'");
    }
};

static BufrValidator bufr_validator;

const Validator& validator() { return bufr_validator; }

} // namespace arki::data::bufr
