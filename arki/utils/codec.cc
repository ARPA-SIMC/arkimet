#include "config.h"

#include <arki/utils/codec.h>
#include <wibble/sys/buffer.h>

using namespace std;
using namespace wibble;

namespace arki {
namespace utils {
namespace codec {

uint64_t decodeULInt(const unsigned char* val, unsigned int bytes)
{
	uint64_t res = 0;
	for (unsigned int i = 0; i < bytes; ++i)
	{
		res <<= 8;
		res |= val[i];
	}
	return res;
}

Encoder& Encoder::addBuffer(const wibble::sys::Buffer& buf)
{
    this->buf.append((const char*)buf.data(), buf.size());
    return *this;
}

Encoder& Encoder::addBuffer(const std::vector<uint8_t>& buf)
{
    this->buf.append((const char*)buf.data(), buf.size());
    return *this;
}

Decoder::Decoder(const wibble::sys::Buffer& buf, size_t offset)
    : buf((const unsigned char*)buf.data() + offset), len(buf.size() - offset)
{
}

Decoder::Decoder(const std::vector<uint8_t>& buf, size_t offset)
    : buf(buf.data() + offset), len(buf.size() - offset)
{
}


}
}
}
