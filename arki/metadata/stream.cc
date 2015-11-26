#include "stream.h"
#include "consumer.h"
#include <arki/types/source/inline.h>
#include <arki/utils/codec.h>

#include <wibble/exception.h>
#include <arki/utils/string.h>

using namespace std;
using namespace arki::utils;

namespace arki {
namespace metadata {

bool Stream::checkMetadata()
{
	using namespace utils::codec;

	if (buffer.size() < 8) return false;

	// Ensure first 2 bytes are MD
	if (buffer[0] != 'M' || buffer[1] != 'D')
		throw wibble::exception::Consistency("checking partial buffer", "buffer contains data that is not encoded metadata");

	// Get version from next 2 bytes
	unsigned int version = codec::decodeUInt((const unsigned char*)buffer.data()+2, 2);
	// Get length from next 4 bytes
	unsigned int len = codec::decodeUInt((const unsigned char*)buffer.data()+4, 4);

	// Check if we have a full metadata, in that case read it, remove it
	// from the beginning of the string, then consume it it
	if (buffer.size() < 8 + len)
		return false;

    metadata::ReadContext rc("http-connection", streamname);
    md.reset(new Metadata);
    md->read((const unsigned char*)buffer.data() + 8, len, version, rc);

    buffer = buffer.substr(len + 8);
    if (md->source().style() == types::Source::INLINE)
    {
        dataToGet = md->data_size();
        state = DATA;
        return true;
    } else {
        consumer.eat(move(md));
        return true;
    }
}

bool Stream::checkData()
{
    if (buffer.size() < dataToGet)
        return false;

    vector<uint8_t> buf(buffer.data(), buffer.data() + dataToGet);
    buffer = buffer.substr(dataToGet);
    dataToGet = 0;
    state = METADATA;
    md->set_source_inline(md->source().format, move(buf));
    consumer.eat(move(md));
    return true;
}

bool Stream::check()
{
    switch (state)
    {
        case METADATA: return checkMetadata(); break;
        case DATA: return checkData(); break;
        default:
            throw wibble::exception::Consistency("checking inbound data", "metadata stream state is in invalid state");
    }
}

void Stream::readData(const void* buf, size_t size)
{
    buffer.append((const char*)buf, size);

    while (check())
        ;
}

}
}
