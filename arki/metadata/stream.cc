#include "stream.h"
#include "data.h"
#include "arki/core/binary.h"
#include "arki/exceptions.h"
#include "arki/metadata.h"
#include "arki/types/source.h"
#include <cstring>

using namespace std;
using namespace arki::utils;

namespace arki {
namespace metadata {

bool Stream::checkMetadata()
{
    if (buffer.size() < 8) return false;

    core::BinaryDecoder dec(buffer);

    // Ensure first 2 bytes are MD
    char header[2];
    header[0] = dec.pop_byte("metadata header");
    header[1] = dec.pop_byte("metadata header");
    if (header[0] != 'M' || header[1] != 'D')
        throw std::runtime_error("partial buffer contains data that is not encoded metadata");

    // Get version from next 2 bytes
    unsigned int version = dec.pop_uint(2, "metadata version");
    // Get length from next 4 bytes
    unsigned int len = dec.pop_uint(4, "metadata length");

    // Check if we have a full metadata, in that case read it, remove it
    // from the beginning of the string, then consume it it
    if (dec.size < len)
        return false;

    core::BinaryDecoder inner = dec.pop_data(len, "encoded metadata body");

    metadata::ReadContext rc("http-connection", streamname);
    md = Metadata::read_binary_inner(inner, version, rc);

    buffer = vector<uint8_t>(dec.buf, dec.buf + dec.size);
    if (md->source().style() == types::Source::Style::INLINE)
    {
        dataToGet = md->data_size();
        state = DATA;
        return true;
    } else {
        if (!canceled)
            if (!consumer(move(md)))
                canceled = true;
        return true;
    }
}

bool Stream::checkData()
{
    if (buffer.size() < dataToGet)
        return false;

    vector<uint8_t> buf(buffer.begin(), buffer.begin() + dataToGet);
    buffer = vector<uint8_t>(buffer.begin() + dataToGet, buffer.end());
    dataToGet = 0;
    state = METADATA;
    md->set_source_inline(md->source().format, metadata::DataManager::get().to_data(md->source().format, move(buf)));
    if (!canceled)
        if (!consumer(move(md)))
            canceled = true;
    return true;
}

bool Stream::check()
{
    switch (state)
    {
        case METADATA: return checkMetadata(); break;
        case DATA: return checkData(); break;
        default:
            throw_consistency_error("checking inbound data", "metadata stream state is in invalid state");
    }
}

void Stream::readData(const void* buf, size_t size)
{
    buffer.insert(buffer.end(), (const uint8_t*)buf, (const uint8_t*)buf + size);

    while (check())
        ;
}

}
}
