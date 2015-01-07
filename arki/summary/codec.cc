/*
 * summary/codec - Summary I/O implementation
 *
 * Copyright (C) 2007--2014  ARPA-SIM <urpsim@smr.arpa.emr.it>
 *
 * This program is free software; you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation; either version 2 of the License, or
 * (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License along
 * with this program; if not, write to the Free Software Foundation, Inc.,
 * 51 Franklin Street, Fifth Floor, Boston, MA 02110-1301 USA.
 *
 * Author: Enrico Zini <enrico@enricozini.com>
 */

#include "codec.h"
#include "node.h"
#include "stats.h"
#include <arki/summary.h>
#include <arki/metadata.h>
#include <arki/utils/codec.h>
#include <arki/utils/compress.h>
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
using namespace wibble;
using namespace arki::utils::codec;
using namespace arki::types;

namespace arki {
namespace summary {

namespace {

struct DecoderBase
{
    Node& target;
    size_t count;

    DecoderBase(Node& target) : target(target), count(0) {}

    void add_to_target(const TypeVector& md, auto_ptr<Stats> stats)
    {
        ++count;
        target.merge(md.raw_items(), md.size(), *stats);
    }
};

struct Format1Decoder : public DecoderBase
{
    TypeVector mdvec;

    Format1Decoder(Node& target) : DecoderBase(target) {}

    static auto_ptr<Type> decodeUItem(size_t msoIdx, const unsigned char*& buf, size_t& len)
    {
        using namespace utils::codec;
        codeclog("Decode metadata item " << msoIdx << " len " << msoSerLen[msoIdx]);
        size_t itemsizelen = msoSerLen[msoIdx];
        ensureSize(len, itemsizelen, "Metadata item size");
        size_t itemsize = decodeUInt(buf, itemsizelen);
        buf += itemsizelen; len -= itemsizelen;
        codeclog("  item size " << itemsize);

        if (itemsize)
        {
            ensureSize(len, itemsize, "Metadata item");
            auto_ptr<Type> res = decodeInner(mso[msoIdx], buf, itemsize);
            buf += itemsize; len -= itemsize;
            return res;
        } else
            return auto_ptr<Type>();
    }

    void decode(const unsigned char*& buf, size_t& len, size_t scanpos = 0)
    {
        using namespace utils::codec;

        codeclog("Start decoding scanpos " << scanpos);

        // Decode the metadata stripe length
        ensureSize(len, 2, "Metadata stripe size");
        size_t stripelen = decodeUInt(buf, 2);
        buf += 2; len -= 2;

        codeclog("Stripe size " << stripelen);

        // Decode the metadata stripe
        mdvec.resize(scanpos + stripelen);
        for (size_t i = 0; i < stripelen; ++i)
            mdvec.set(scanpos + i, decodeUItem(scanpos + i, buf, len));

        ensureSize(len, 2, "Number of child stripes");
        size_t childnum = decodeUInt(buf, 2);
        buf += 2; len -= 2;
        codeclog("Found " << childnum << " children");

        if (childnum)
        {
            // Decode the children
            for (size_t i = 0; i < childnum; ++i)
                decode(buf, len, scanpos + stripelen);
        } else {
            // Leaf node: decode stats
            ensureSize(len, 2, "Summary statistics size");
            size_t statlen = decodeUInt(buf, 2);
            buf += 2; len -= 2;
            codeclog("Decoding stats in " << statlen << "b");
            ensureSize(len, 2, "Summary statistics");
            auto_ptr<Stats> stats = Stats::decode(buf, statlen);
            buf += statlen; len -= statlen;

            // Strip undef values at the end of mdvec
            mdvec.rtrim();

            // Produce a (metadata, stats) couple
            add_to_target(mdvec, stats);
        }
    }
};

struct Format3Decoder : public DecoderBase
{
    TypeVector mdvec;

    Format3Decoder(Node& target) : DecoderBase(target) {}

    void decode(Decoder& dec)
    {
        using namespace utils::codec;

        while (dec.len > 0)
        {
            // Decode the list of removed items
            unsigned count_removed = dec.popVarint<unsigned>("number of items to unset");
            for (unsigned i = 0; i < count_removed; ++i)
            {
                types::Code code = (types::Code)dec.popVarint<unsigned>("typecode");
                int pos = Visitor::posForCode(code);
                if (pos < 0)
                    throw wibble::exception::Consistency("parsing summary",
                            str::fmtf("unsupported typecode found: %d", (int)code));
                mdvec.unset(pos);
            }

            // Decode the list of changed/added items
            unsigned count_added = dec.popVarint<unsigned>("number of items to add/change");
            for (unsigned i = 0; i < count_added; ++i)
            {
                auto_ptr<Type> item = types::decode(dec);
                int pos = Visitor::posForCode(item->serialisationCode());
                if (pos < 0)
                    throw wibble::exception::Consistency("parsing summary",
                            str::fmtf("unsupported typecode found: %d", (int)item->serialisationCode()));
                mdvec.set(pos, item);
            }

            // Decode the stats
            auto_ptr<Stats> stats = downcast<Stats>(types::decode(dec));

            // Strip undef values at the end of mdvec
            mdvec.rtrim();

            // Produce a (metadata, stats) couple
            add_to_target(mdvec, stats);
        }
    }
};

}

size_t decode1(utils::codec::Decoder& dec, Node& target)
{
    // Stripe size
    // either: child count + children
    //     or: 0 + stats
    // msoSerLen used for number of bytes used for item lenght

    if (dec.len == 0) return 0;

    Node::buildMsoSerLen();

    Format1Decoder decoder(target);
    decoder.decode(dec.buf, dec.len);

    return decoder.count;
}

size_t decode3(utils::codec::Decoder& dec, Node& target)
{
    // Stripe size
    // stats size + stats
    // child size + children
    // item length encoded using varints
    if (dec.len == 0) return 0;

    Format3Decoder decoder(target);
    decoder.decode(dec);

    return decoder.count;
}

size_t decode(const wibble::sys::Buffer& buf, unsigned version, const std::string& filename, Node& target)
{
    using namespace utils::codec;

    // Check version and ensure we can decode
    switch (version)
    {
        case 1: {
            // Standard summary
            Decoder dec(buf);
            return decode1(dec, target);
        }
        case 2: {
            // LZO compressed summary
            if (buf.size() == 0) return 0;

            // Read uncompressed size
            ensureSize(buf.size(), 4, "uncompressed item size");
            uint32_t uncsize = decodeUInt((const unsigned char*)buf.data(), 4);

            sys::Buffer decomp = utils::compress::unlzo((const unsigned char*)buf.data() + 4, buf.size() - 4, uncsize);
            Decoder dec(decomp);
            return decode1(dec, target);
        }
        case 3: {
            // Compression type byte, node in new format
            if (buf.size() == 0) return 0;

            Decoder dec(buf);
            unsigned compression = dec.popUInt(1, "compression type");
            switch (compression)
            {
                case 0: // Uncompressed
                    return decode3(dec, target);
                case 1: { // LZO compressed
                    // Read uncompressed size
                    uint32_t uncsize = dec.popUInt(4, "uncompressed item size");
                    sys::Buffer decomp = utils::compress::unlzo(dec.buf, dec.len, uncsize);
                    Decoder uncdec(decomp);
                    return decode3(uncdec, target);
                }
                default:
                    throw wibble::exception::Consistency("parsing file " + filename, "file compression type is " + str::fmt(compression) + " but I can only decode 0 (uncompressed) or 1 (LZO)");
            }
        }
        default:
            throw wibble::exception::Consistency("parsing file " + filename, "version of the file is " + str::fmt(version) + " but I can only decode version 1 or 2");
    }
}

EncodingVisitor::EncodingVisitor(utils::codec::Encoder& enc)
    : enc(enc)
{
    // Start with all undef
    last.resize(Node::msoSize);
}

bool EncodingVisitor::operator()(const std::vector<const Type*>& md, const Stats& stats)
{
    vector<types::Code> removed;
    size_t added_count = 0;
    string added;

    // Prepare the diff between last and md
    for (size_t i = 0; i < Node::msoSize; ++i)
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
            added += md[i]->encodeBinary();
            last[i] = md[i];
        }
    }

    // Encode the list of removed items
    enc.addVarint(removed.size());
    for (vector<types::Code>::const_iterator i = removed.begin();
            i != removed.end(); ++i)
        enc.addVarint((unsigned)*i);

    // Encode the list of changed/added items
    enc.addVarint(added_count);
    enc.addString(added);

    // Encode the stats
    enc.addString(stats.encodeBinary());

    return true;
}

}
}