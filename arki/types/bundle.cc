#include "bundle.h"
#include "arki/core/file.h"
#include "arki/core/binary.h"
#include "arki/utils/sys.h"
#include <algorithm>

using namespace arki::utils;
using namespace arki::core;

namespace arki {
namespace types {

bool Bundle::read_header(NamedFileDescriptor& fd)
{
    // Skip all leading blank bytes
    char c;
    while (true)
    {
        int res = fd.read(&c, 1);
        if (res == 0) return false; // EOF
        if (c) break;
    }

    // Read the rest of the first 8 bytes
    unsigned char hdr[8];
    hdr[0] = c;
    size_t res = fd.read(hdr + 1, 7);
    if (res < 7) return false; // EOF

    core::BinaryDecoder dec(hdr, 8);

    // Read the signature
    signature = dec.pop_string(2, "header of metadata bundle");

    // Get the version in next 2 bytes
    version = dec.pop_uint(2, "version of metadata bundle");

    // Get length from next 4 bytes
    length = dec.pop_uint(4, "size of metadata bundle");

    return true;
}

bool Bundle::read_data(NamedFileDescriptor& fd)
{
    // Use reserve, then read a bit at a time, resizing appropriately, to avoid
    // allocating and using (which triggers the kernel to actually allocate
    // memory pages) potentially a lot of ram, and then read fails right away
    // because the length field of the file was corrupted
    data.resize(0);
    data.reserve(length);
    size_t to_read = length;
    while (to_read > 0)
    {
        size_t chunk_size = std::min(to_read, (size_t)1024 * 1024);
        size_t pos = data.size();
        data.resize(pos + chunk_size);
        size_t res = fd.read(data.data() + pos, chunk_size);
        if (res == 0)
            return false;
        to_read -= res;
        data.resize(pos + res);
    }
    return true;
}

bool Bundle::read(NamedFileDescriptor& fd)
{
    if (!read_header(fd)) return false;
    return read_data(fd);
}

bool Bundle::read_header(AbstractInputFile& fd)
{
    // Skip all leading blank bytes
    char c;
    while (true)
    {
        int res = fd.read(&c, 1);
        if (res == 0) return false; // EOF
        if (c) break;
    }

    // Read the rest of the first 8 bytes
    unsigned char hdr[8];
    hdr[0] = c;
    size_t res = fd.read(hdr + 1, 7);
    if (res < 7) return false; // EOF

    core::BinaryDecoder dec(hdr, 8);

    // Read the signature
    signature = dec.pop_string(2, "header of metadata bundle");

    // Get the version in next 2 bytes
    version = dec.pop_uint(2, "version of metadata bundle");

    // Get length from next 4 bytes
    length = dec.pop_uint(4, "size of metadata bundle");

    return true;
}

bool Bundle::read_data(AbstractInputFile& fd)
{
    // Use reserve, then read a bit at a time, resizing appropriately, to avoid
    // allocating and using (which triggers the kernel to actually allocate
    // memory pages) potentially a lot of ram, and then read fails right away
    // because the length field of the file was corrupted
    data.resize(0);
    data.reserve(length);
    size_t to_read = length;
    while (to_read > 0)
    {
        size_t chunk_size = std::min(to_read, (size_t)1024 * 1024);
        size_t pos = data.size();
        data.resize(pos + chunk_size);
        size_t res = fd.read(data.data() + pos, chunk_size);
        if (res == 0)
            return false;
        to_read -= res;
        data.resize(pos + res);
    }
    return true;
}

bool Bundle::read(AbstractInputFile& fd)
{
    if (!read_header(fd)) return false;
    return read_data(fd);
}


}
}
