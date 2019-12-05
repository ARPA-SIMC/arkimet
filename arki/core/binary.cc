#include "binary.h"
#include <stdexcept>
#include <algorithm>
#include <sstream>

using namespace std;

namespace arki {
namespace core {

void BinaryDecoder::throw_parse_error(const std::string& what, const std::string& errmsg)
{
    throw std::runtime_error("Cannot parse " + what + ": " + errmsg);
}

void BinaryDecoder::throw_insufficient_size(const std::string& what, size_t wanted)
{
    std::stringstream ss;
    ss << "Cannot parse " << what << ": size is " << size << " but at least " << wanted << " are needed";
    throw std::runtime_error(ss.str());
}

std::string BinaryDecoder::pop_line(char sep)
{
    string res;

    if (!size) return res;

    const uint8_t* pos = std::find(buf, buf + size, (uint8_t)sep);
    if (pos == buf + size)
    {
        res.assign(buf, buf + size);
        buf += size;
        size = 0;
        return res;
    }

    res.assign(buf, pos);
    size -= (pos + 1 - buf);
    buf = pos + 1;
    return res;
}

std::string BinaryDecoder::pop_line(const std::string& sep)
{
    string res;

    if (!size) return res;

    const uint8_t* pos = std::search(buf, buf + size, (uint8_t*)sep.data(), (uint8_t*)sep.data() + sep.size());
    if (pos == buf + size)
    {
        res.assign(buf, buf + size);
        buf += size;
        size = 0;
        return res;
    }

    res.assign(buf, pos);
    size -= (pos + sep.size() - buf);
    buf = pos + sep.size();
    return res;
}

BinaryDecoder BinaryDecoder::pop_type_envelope(TypeCode& code)
{
    // Decode the element type
    code = (TypeCode)pop_varint<unsigned>("element code");
    // Decode the element size
    size_t inner_size = pop_varint<size_t>("element size");

    // Finally decode the element body
    const uint8_t* inner_buf = buf;
    ensure_size(inner_size, "element body");

    buf += inner_size;
    size -= inner_size;

    return BinaryDecoder(inner_buf, inner_size);
}

BinaryDecoder BinaryDecoder::pop_metadata_bundle(std::string& signature, unsigned& version)
{
    // Skip all leading blank bytes
    ensure_size(8, "header of metadata bundle");

    // Read the signature
    signature = pop_string(2, "signature of metadata bundle");

    // Get the version in next 2 bytes
    version = pop_uint(2, "version of metadata bundle");

    // Get length from next 4 bytes
    size_t len = pop_uint(4, "size of metadata bundle");

    BinaryDecoder res(buf, len);

    // Move to the end of the inner data
    buf += len;
    size -= len;

    return res;
}

}
}
