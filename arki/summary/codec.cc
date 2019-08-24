#include "codec.h"
#include "table.h"
#include "stats.h"
#include <arki/summary.h>
#include <arki/metadata.h>
#include <arki/core/binary.h>
#include <arki/utils/compress.h>
#include <arki/utils/sys.h>
#include <arki/types/utils.h>

// #define DEBUG_THIS
#ifdef DEBUG_THIS
#warning Debug enabled
#include <iostream>
#define codeclog(...) cerr << __VA_ARGS__ << endl
#else
#define codeclog(...) do {} while(0)
#endif

using namespace std;
using namespace arki::types;

namespace arki {
namespace summary {

namespace {

struct DecoderBase
{
    Table& target;
    size_t count;

    DecoderBase(Table& target) : target(target), count(0) {}
};

struct Format1Decoder : public DecoderBase
{
    Row row;

    Format1Decoder(Table& target) : DecoderBase(target)
    {
        row.set_to_zero();
    }

    const Type* decodeUItem(size_t msoIdx, core::BinaryDecoder& dec)
    {
        codeclog("Decode metadata item " << msoIdx << ":" << formatCode(Table::mso[msoIdx]) << " len " << Table::msoSerLen[msoIdx]);
        size_t itemsizelen = Table::msoSerLen[msoIdx];
        size_t itemsize = dec.pop_uint(itemsizelen, "Metadata item size");
        codeclog("  item size " << itemsize);

        if (itemsize)
        {
            core::BinaryDecoder inner = dec.pop_data(itemsize, "encoded metadata body");
            return target.intern(msoIdx, decodeInner(Table::mso[msoIdx], inner));
        }
        else
            return 0;
    }

    void decode(core::BinaryDecoder& dec, size_t scanpos = 0)
    {
        codeclog("Start decoding scanpos " << scanpos);

        // Decode the metadata stripe length
        size_t stripelen = dec.pop_uint(2, "Metadata stripe size");
        codeclog("Stripe size " << stripelen);

        // Decode the metadata stripe
        for (size_t i = 0; i < stripelen; ++i)
            row.items[scanpos + i] = decodeUItem(scanpos + i, dec);

        size_t childnum = dec.pop_uint(2, "Number of child stripes");
        codeclog("Found " << childnum << " children");

        if (childnum)
        {
            // Decode the children
            for (size_t i = 0; i < childnum; ++i)
                decode(dec, scanpos + stripelen);
        } else {
            // Leaf node: decode stats
            /*size_t statlen =*/ dec.pop_uint(2, "Summary statistics size");
            codeclog("Decoding stats in " << statlen << "b");
            row.stats = *Stats::decode(dec);

            // Zero out the remaining items
            row.set_to_zero(scanpos + stripelen);

            // Produce a (metadata, stats) couple
            target.merge(row);
            ++count;
        }
    }
};

struct Format3Decoder : public DecoderBase
{
    Row row;

    Format3Decoder(Table& target) : DecoderBase(target)
    {
        row.set_to_zero();
    }

    void decode(core::BinaryDecoder& dec)
    {
        while (dec)
        {
            // Decode the list of removed items
            unsigned count_removed = dec.pop_varint<unsigned>("number of items to unset");
            for (unsigned i = 0; i < count_removed; ++i)
            {
                types::Code code = (types::Code)dec.pop_varint<unsigned>("typecode");
                int pos = Visitor::posForCode(code);
                if (pos < 0)
                {
                    stringstream ss;
                    ss << "cannot parse summary: found unsupported typecode " << (int)code;
                    throw runtime_error(ss.str());
                }
                row.items[pos] = nullptr;
            }

            // Decode the list of changed/added items
            unsigned count_added = dec.pop_varint<unsigned>("number of items to add/change");
            for (unsigned i = 0; i < count_added; ++i)
            {
                unique_ptr<Type> item = types::decode(dec);
                int pos = Visitor::posForCode(item->type_code());
                if (pos < 0)
                {
                    stringstream ss;
                    ss << "cannot parse summary: found unsupported typecode " << (int)item->type_code();
                    throw runtime_error(ss.str());
                }
                row.items[pos] = target.intern(pos, move(item));
            }

            // Decode the stats
            types::Code code;
            core::BinaryDecoder inner = dec.pop_type_envelope(code);
            if (code != TYPE_SUMMARYSTATS)
            {
                stringstream ss;
                ss << "cannot parse summary: found typecode " << (int)code << " instead of summary stats (" << (int)TYPE_SUMMARYSTATS << ")";
                throw runtime_error(ss.str());
            }
            row.stats = *Stats::decode(inner);

            // Produce a (metadata, stats) couple
            target.merge(row);
            ++count;
        }
    }
};

}

size_t decode1(core::BinaryDecoder& dec, Table& target)
{
    // Stripe size
    // either: child count + children
    //     or: 0 + stats
    // msoSerLen used for number of bytes used for item lenght

    if (!dec) return 0;

    Format1Decoder decoder(target);
    decoder.decode(dec);

    return decoder.count;
}

size_t decode3(core::BinaryDecoder& dec, Table& target)
{
    // Stripe size
    // stats size + stats
    // child size + children
    // item length encoded using varints
    if (!dec) return 0;

    Format3Decoder decoder(target);
    decoder.decode(dec);

    return decoder.count;
}

size_t decode(core::BinaryDecoder& dec, unsigned version, const std::string& filename, Table& target)
{
    // Check version and ensure we can decode
    switch (version)
    {
        case 1: {
            // Standard summary
            return decode1(dec, target);
        }
        case 2: {
            // LZO compressed summary
            if (!dec) return 0;

            // Read uncompressed size
            uint32_t uncsize = dec.pop_uint(4, "size of uncompressed data");

            vector<uint8_t> decomp = utils::compress::unlzo(dec.buf, dec.size, uncsize);
            core::BinaryDecoder uncdec(decomp);
            return decode1(uncdec, target);
        }
        case 3: {
            // Compression type byte, node in new format
            if (!dec) return 0;

            unsigned compression = dec.pop_uint(1, "compression type");
            switch (compression)
            {
                case 0: // Uncompressed
                    return decode3(dec, target);
                case 1: { // LZO compressed
                    // Read uncompressed size
                    uint32_t uncsize = dec.pop_uint(4, "uncompressed item size");
                    vector<uint8_t> decomp = utils::compress::unlzo(dec.buf, dec.size, uncsize);
                    core::BinaryDecoder uncdec(decomp);
                    return decode3(uncdec, target);
                }
                default:
                {
                    stringstream ss;
                    ss << "cannot parse file " << filename << ": file compression type is " << compression << " but I can only decode 0 (uncompressed) or 1 (LZO)";
                    throw runtime_error(ss.str());
                }
            }
        }
        default:
        {
            stringstream ss;
            ss << "cannot parse file " << filename << ": version of the file is " << version << " but I can only decode version 1 or 2";
            throw runtime_error(ss.str());
        }
    }
}

EncodingVisitor::EncodingVisitor(core::BinaryEncoder& enc)
    : enc(enc)
{
    // Start with all undef
    last.resize(Table::msoSize);
}

bool EncodingVisitor::operator()(const std::vector<const Type*>& md, const Stats& stats)
{
    vector<types::Code> removed;
    size_t added_count = 0;
    vector<uint8_t> added;
    core::BinaryEncoder added_enc(added);

    // Prepare the diff between last and md
    for (size_t i = 0; i < Table::msoSize; ++i)
    {
        bool md_has_it = i < md.size() && md[i];
        if (!md_has_it && last[i])
        {
            // Enqueue an empty codeForPos(i)
            removed.push_back(codeForPos(i));
            last[i] = 0;
        } else if (md_has_it && (!last[i] || md[i] != last[i])) {
            // Enqueue md[i]
            ++added_count;
            md[i]->encodeBinary(added_enc);
            last[i] = md[i];
        }
    }

    // Encode the list of removed items
    enc.add_varint(removed.size());
    for (vector<types::Code>::const_iterator i = removed.begin();
            i != removed.end(); ++i)
        enc.add_varint((unsigned)*i);

    // Encode the list of changed/added items
    enc.add_varint(added_count);
    enc.add_raw(added);

    // Encode the stats
    stats.encodeBinary(enc);

    return true;
}

}
}
