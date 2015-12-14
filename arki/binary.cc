#include "binary.h"
#include <stdexcept>
#include <sstream>

namespace arki {

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
