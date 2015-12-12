#include "config.h"
#include <arki/utils/codec.h>

using namespace std;

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

Decoder::Decoder(const std::vector<uint8_t>& buf, size_t offset)
    : buf(buf.data() + offset), len(buf.size() - offset)
{
}


}
}
}
