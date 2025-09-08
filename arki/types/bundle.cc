#include "bundle.h"
#include "arki/core/binary.h"
#include "arki/core/file.h"
#include "arki/utils/sys.h"
#include <algorithm>

using namespace arki::utils;
using namespace arki::core;

namespace arki::types {

size_t Bundle::read_header(NamedFileDescriptor& fd)
{
    size_t count_read = 0;

    // Skip all leading blank bytes
    char c;
    while (true)
    {
        int res = fd.read(&c, 1);
        if (res == 0)
            return 0; // EOF
        ++count_read;
        if (c)
            break;
    }

    // Read the rest of the first 8 bytes
    unsigned char hdr[8];
    hdr[0] = c;
    if (!fd.read_all_or_retry(hdr + 1, 7))
        return 0; // EOF
    count_read += 7;

    core::BinaryDecoder dec(hdr, 8);

    // Read the signature
    signature = dec.pop_string(2, "header of metadata bundle");

    // Get the version in next 2 bytes
    version = dec.pop_uint(2, "version of metadata bundle");

    // Get length from next 4 bytes
    length = dec.pop_uint(4, "size of metadata bundle");

    return count_read;
}

size_t Bundle::read_data(NamedFileDescriptor& fd)
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
        size_t pos        = data.size();
        data.resize(pos + chunk_size);
        size_t res = fd.read(data.data() + pos, chunk_size);
        if (res == 0)
            return 0;
        to_read -= res;
        data.resize(pos + res);
    }
    return length;
}

size_t Bundle::read(NamedFileDescriptor& fd)
{
    size_t count_read = read_header(fd);
    if (!count_read)
        return false;
    return count_read + read_data(fd);
}

size_t Bundle::read_header(AbstractInputFile& fd)
{
    size_t count_read = 0;

    // Skip all leading blank bytes
    char c;
    while (true)
    {
        int res = fd.read(&c, 1);
        if (res == 0)
            return 0; // EOF
        ++count_read;
        if (c)
            break;
    }

    // Read the rest of the first 8 bytes
    unsigned char hdr[8];
    hdr[0]     = c;
    size_t res = fd.read(hdr + 1, 7);
    if (res < 7)
        return false; // EOF
    count_read += 7;

    core::BinaryDecoder dec(hdr, 8);

    // Read the signature
    signature = dec.pop_string(2, "header of metadata bundle");

    // Get the version in next 2 bytes
    version = dec.pop_uint(2, "version of metadata bundle");

    // Get length from next 4 bytes
    length = dec.pop_uint(4, "size of metadata bundle");

    return count_read;
}

size_t Bundle::read_data(AbstractInputFile& fd)
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
        size_t pos        = data.size();
        data.resize(pos + chunk_size);
        size_t res = fd.read(data.data() + pos, chunk_size);
        if (res == 0)
            return 0;
        to_read -= res;
        data.resize(pos + res);
    }
    return length;
}

size_t Bundle::read(AbstractInputFile& fd)
{
    size_t count_read = read_header(fd);
    if (!count_read)
        return false;
    return count_read + read_data(fd);
}

} // namespace arki::types
