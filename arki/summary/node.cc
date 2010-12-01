/*
 * summary/node - Summary node implementation
 *
 * Copyright (C) 2007--2010  ARPA-SIM <urpsim@smr.arpa.emr.it>
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

#include <arki/summary/node.h>
#include <arki/summary.h>
#include <arki/summary/stats.h>
#include <arki/metadata.h>
#include <arki/matcher.h>
#include <arki/utils/codec.h>
#include <arki/utils/compress.h>
#include <arki/types/utils.h>
#if 0
#include <arki/utils/compress.h>
#include <arki/emitter.h>
#include <arki/emitter/memory.h>

#include <wibble/exception.h>
#include <wibble/string.h>
#include <wibble/sys/buffer.h>

#include <fstream>

#include <sys/types.h>
#include <sys/stat.h>
#include <fcntl.h>

#include "config.h"
#endif

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

namespace arki {
namespace summary {

// Metadata Scan Order
static const types::Code mso[] = {
        types::TYPE_ORIGIN,
        types::TYPE_PRODUCT,
        types::TYPE_LEVEL,
        types::TYPE_TIMERANGE,
        types::TYPE_AREA,
        types::TYPE_ENSEMBLE,
        types::TYPE_BBOX,
        types::TYPE_RUN,
        types::TYPE_QUANTITY,
        types::TYPE_TASK

};
const size_t Node::msoSize = sizeof(mso) / sizeof(types::Code);
static int* msoSerLen = 0;

// Reverse mapping
static int* itemMsoMap = 0;
static size_t itemMsoMapSize = 0;

void Node::buildMsoSerLen()
{
    if (msoSerLen) return;
    msoSerLen = new int[msoSize];
    for (size_t i = 0; i < msoSize; ++i)
    {
        const types::MetadataType* mdt = types::MetadataType::get(mso[i]);
        msoSerLen[i] = mdt ? mdt->serialisationSizeLen : 0;
    }
}
void Node::buildItemMsoMap()
{
    if (itemMsoMap) return;

    for (size_t i = 0; i < msoSize; ++i)
        if ((size_t)summary::mso[i] > itemMsoMapSize)
            itemMsoMapSize = (size_t)summary::mso[i];
    itemMsoMapSize += 1;

    itemMsoMap = new int[itemMsoMapSize];
    for (size_t i = 0; i < itemMsoMapSize; ++i)
        itemMsoMap[i] = -1;
    for (size_t i = 0; i < msoSize; ++i)
        itemMsoMap[(size_t)summary::mso[i]] = i;
}

types::Code Visitor::codeForPos(size_t pos)
{
    return mso[pos];
}

int Visitor::posForCode(types::Code code)
{
    if ((size_t)code >= itemMsoMapSize) return -1;
    return itemMsoMap[(size_t)code];
}

static void metadata_to_mdvec(const Metadata& md, std::vector< UItem<> >& vec)
{
    for (size_t i = 0; i < Node::msoSize; ++i)
    {
        UItem<> item = md.get(mso[i]);
        if (item.defined())
        {
            vec.resize(i + 1);
            vec[i] = item;
        }
    }
}

Node::Node()
{
}
Node::Node(const std::vector< UItem<> >& m, const UItem<Stats>& st, size_t scanpos)
{
    if (!scanpos)
        md = m;
    else
        for (size_t i = scanpos; i < m.size(); ++i)
            md.push_back(m[i]);
    stats = st;
}
Node::~Node()
{
    for (vector<Node*>::iterator i = children.begin();
            i != children.end(); ++i)
        delete *i;
}

bool Node::visitStats(StatsVisitor& visitor) const
{
    if (stats)
        if (!visitor(*stats))
            return false;

    for (vector<Node*>::const_iterator i = children.begin();
            i != children.end(); ++i)
        if (!(*i)->visitStats(visitor))
            return false;

    return true;
}

bool Node::visitItem(size_t msoidx, ItemVisitor& visitor) const
{
    if (msoidx < md.size())
        return visitor(md[msoidx]);

    for (vector<Node*>::const_iterator i = children.begin();
            i != children.end(); ++i)
        if (!(*i)->visitItem(msoidx - md.size(), visitor))
            return false;

    return true;
}

static inline void mdvec_set(vector< UItem<> >& mdvec, size_t pos, const UItem<>& val)
{
    if (pos >= mdvec.size())
        mdvec.resize(pos + 1);
    mdvec[pos] = val;
}

bool Node::visit(Visitor& visitor, std::vector< UItem<> >& visitmd, size_t scanpos) const
{
    // Set this node's metadata in visitmd
    for (size_t i = 0; i < md.size(); ++i)
        mdvec_set(visitmd, scanpos + i, md[i]);

    // If we have a stats, emit
    if (stats)
    {
        visitmd.resize(scanpos + md.size());
        if (!visitor(visitmd, stats))
            return false;
    }

    // If we have children, visit them
    for (vector<Node*>::const_iterator i = children.begin();
            i != children.end(); ++i)
        if (!(*i)->visit(visitor, visitmd, scanpos + md.size()))
            return false;

    return true;
}

bool Node::visitFiltered(const Matcher& matcher, Visitor& visitor) const
{
    buildItemMsoMap();
    vector< UItem<> > visitmd;
    return visitFiltered(matcher, visitor, visitmd);
}

bool Node::visitFiltered(const Matcher& matcher, Visitor& visitor, std::vector< UItem<> >& visitmd, size_t scanpos) const
{
    // Check if the matcher is ok with this node; if it's not, return true right away
    if (matcher.m_impl)
    {
        const matcher::AND& mand = *matcher.m_impl;
        for (size_t i = 0; i < md.size(); ++i)
        {
            matcher::AND::const_iterator j = mand.find(mso[scanpos + i]);
            if (j == mand.end()) continue;
            if (!md[i].defined()) return true;
            if (!j->second->matchItem(md[i])) return true;
        }
        if (stats)
        {
            matcher::AND::const_iterator rm = mand.find(types::TYPE_REFTIME);
            if (rm != mand.end() && !rm->second->matchItem(stats->reftimeMerger.makeReftime()))
                return true;
        }
    }

    // Set this node's metadata in visitmd
    for (size_t i = 0; i < md.size(); ++i)
        mdvec_set(visitmd, scanpos + i, md[i]);

    // If we have a stats, emit
    if (stats)
    {
        visitmd.resize(scanpos + md.size());
        if (!visitor(visitmd, stats))
            return false;
    }

    // If we have children, visit them
    for (vector<Node*>::const_iterator i = children.begin(); i != children.end(); ++i)
        if (!(*i)->visitFiltered(matcher, visitor, visitmd, scanpos + md.size()))
            return false;

    return true;
}

void Node::split(size_t pos)
{
    // m has a subset of our metadata: split.
    Node* n = new Node(md, stats, pos);

    // Move the rest of md to a new child node
    md.resize(pos);

    // Move our stats to the new child node
    stats = 0;

    // Move our children to the new child node, and add n as the only
    // child
    n->children = children;
    children.clear();
    children.push_back(n);
}

Node* Node::obtain_node(const std::vector< UItem<> >& m, size_t scanpos)
{
    // If the node is completely blank, assign it to m.
    // This only makes sense when building a tree starting from a blank root
    // node. It can happen during decoding, or during tests
    if (md.empty() && children.empty() && !stats.defined())
    {
        if (scanpos == 0)
            md = m;
        else
            for (size_t i = scanpos; i < m.size(); ++i)
                md.push_back(m[i]);
        return this;
    }

    // Ensure that we can store it
    for (size_t i = 0; i < md.size(); ++i)
    {
        if (scanpos + i >= m.size())
        {
            // m has a subset of our metadata: split.
            split(i);

            // We are now a new node perfectly representing m
            return this;
        }
        else if (int cmp = m[scanpos + i].compare(md[i]))
        {
            // one of m differs from one of our md: split
            split(i);

            // Create the new node for m
            Node* n = new Node(m, 0, scanpos + i);
            if (cmp < 0)
                // prepend the new node
                children.insert(children.begin(), n);
            else
                // append the new node
                children.push_back(n);

            // Return the new node for m
            return n;
        }
    }

    // If we are here, then scanpos+md.size() <= m.size()
    // and foreach(scanpos <= i < md.size()): md[i] == m[i]

    // Perfectly match for md, return this node
    if (scanpos + md.size() == m.size())
        return this;

    // If we are here, then m.size() > scanpos+md.size()
    // and we need to find or create the child to which we delegate the search
    // FIXME: is it worth replacing with binary search and insertionsort?
    size_t child_scanpos = scanpos + md.size();
    for (vector<Node*>::iterator i = children.begin(); i != children.end(); ++i)
    {
        int cmp = m[child_scanpos].compare((*i)->md[0]);
        if (cmp < 0)
        {
            // Insert a new node for m at this position
            Node* n = new Node(m, 0, scanpos + md.size());
            children.insert(i, n);
            return n;
        }
        else if (cmp == 0)
            // Delegate to this child
            return (*i)->obtain_node(m, child_scanpos);
        // Else keep searching
    }

    // Append a new node
    Node* n = new Node(m, 0, scanpos + md.size());
    children.push_back(n);
    return n;
}

Node* Node::clone() const
{
    Node* n = new Node(md, stats);
    for (vector<Node*>::const_iterator i = children.begin();
            i != children.end(); ++i)
        n->children.push_back((*i)->clone());
    return n;
}

int Node::compare(const Node& node) const
{
    // Compare metadata stripes
    if (int res = md.size() - node.md.size()) return res;
    for (size_t i = 0; i < md.size(); ++i)
        if (int res = md[i].compare(node.md[i])) return res;

    // Compare stats
    if (!stats.defined() && node.stats.defined()) return -1;
    if (stats.defined() && !node.stats.defined()) return 1;
    if (stats.defined() && node.stats.defined())
        if (int res = stats->compare(*node.stats)) return res;

    // Compare children
    if (int res = children.size() - node.children.size()) return res;
    vector<Node*>::const_iterator a = children.begin();
    vector<Node*>::const_iterator b = node.children.begin();
    for (; a != children.end(); ++a, ++b)
        if (int res = (*a)->compare(**b)) return res;
    return 0;
}

void Node::dump(std::ostream& out, size_t depth) const
{
    string head(depth, ' ');
    out << head << md.size() << " md:" << endl;
    for (size_t i = 0; i < md.size(); ++i)
        out << head << md[i] << endl;
    if (stats.ptr())
        out << head << "Stats:" << stats;
    out << head << children.size() << " children:" << endl;
    for (vector<Node*>::const_iterator i = children.begin(); i != children.end(); ++i)
        (*i)->dump(out, depth+1);
}

RootNode::RootNode()
{
}
RootNode::RootNode(const Metadata& m)
{
    metadata_to_mdvec(m, md);
    stats = new Stats(m);
}
RootNode::RootNode(const Metadata& m, const UItem<Stats>& st)
{
    metadata_to_mdvec(m, md);
    stats = st;
}
RootNode::RootNode(const std::vector< UItem<> >& m, const UItem<Stats>& st)
    : Node(m, st)
{
}

RootNode* RootNode::clone() const
{
    RootNode* n = new RootNode(md, stats);
    for (vector<Node*>::const_iterator i = children.begin();
            i != children.end(); ++i)
        n->children.push_back((*i)->clone());
    return n;
}

bool RootNode::visit(Visitor& visitor) const
{
    buildItemMsoMap();
    vector< UItem<> > visitmd;
    return Node::visit(visitor, visitmd);
}

static UItem<Stats> merge_stats(const UItem<Stats>& s1, const UItem<Stats>& s2)
{
    if (!s1.ptr()) return s2;
    if (!s2.ptr()) return s1;
    auto_ptr<Stats> res(new Stats);
    res->merge(*s1);
    res->merge(*s2);
    return res.release();
}

static UItem<Stats> merge_stats(const UItem<Stats>& s1, const Metadata& m)
{
    if (!s1.ptr()) return new Stats(m);
    auto_ptr<Stats> res(new Stats);
    res->merge(*s1);
    res->merge(m);
    return res.release();
}

void RootNode::add(const Metadata& m)
{
    std::vector< UItem<> > mdvec;
    metadata_to_mdvec(m, mdvec);
    Node* n = obtain_node(mdvec);
    n->stats = merge_stats(n->stats, m);
}

void RootNode::add(const Metadata& m, const UItem<Stats>& st)
{
    std::vector< UItem<> > mdvec;
    metadata_to_mdvec(m, mdvec);
    add(mdvec, st);
}

void RootNode::add(const std::vector< UItem<> >& m, const UItem<Stats>& st)
{
    Node* n = obtain_node(m);
    n->stats = merge_stats(n->stats, st);
}

namespace {

struct EncodingVisitor : public Visitor
{
    // Encoder we send data to
    utils::codec::Encoder& enc;

    // Last metadata encoded so far
    vector< UItem<> > last;

    EncodingVisitor(utils::codec::Encoder& enc)
        : enc(enc)
    {
        // Start with all undef
        last.resize(Node::msoSize);
    }

    virtual bool operator()(const std::vector< UItem<> >& md, const UItem<Stats>& stats)
    {
        vector<types::Code> removed;
        size_t added_count = 0;
        string added;

        // Prepare the diff between last and md
        for (size_t i = 0; i < Node::msoSize; ++i)
        {
            bool md_has_it = i < md.size() && md[i].defined();
            if (!md_has_it && last[i].defined())
            {
                // Enqueue an empty codeForPos(i)
                removed.push_back(codeForPos(i));
                last[i] = 0;
            } else if (md_has_it && (!last[i].defined() || md[i] != last[i])) {
                // Enqueue md[i]
                ++added_count;
                added += md[i]->encodeWithEnvelope();
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
        enc.addString(stats->encodeWithEnvelope());

        return true;
    }
};

}

void RootNode::encode(utils::codec::Encoder& enc) const
{
    EncodingVisitor visitor(enc);
    visit(visitor);
}

namespace {

struct Format1Decoder
{
    Visitor& dest;
    vector< UItem<> > mdvec;

    Format1Decoder(Visitor& dest) : dest(dest) {}

    static UItem<> decodeUItem(size_t msoIdx, const unsigned char*& buf, size_t& len)
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
            UItem<> res = decodeInner(mso[msoIdx], buf, itemsize);
            buf += itemsize; len -= itemsize;
            return res;
        } else
            return UItem<>();
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
            mdvec[scanpos + i] = decodeUItem(scanpos + i, buf, len);

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
            UItem<Stats> stats = Stats::decode(buf, statlen);
            buf += statlen; len -= statlen;

            // Strip undef values at the end of mdvec
            while (!mdvec.empty() && !mdvec.back().defined())
                mdvec.pop_back();

            // Produce a (metadata, stats) couple
            dest(mdvec, stats);
        }
    }
};

struct Format3Decoder
{
    Visitor& dest;
    vector< UItem<> > mdvec;

    Format3Decoder(Visitor& dest) : dest(dest) {}

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
                if (mdvec.size() > (unsigned)pos)
                    mdvec[pos] = 0;
            }

            // Decode the list of changed/added items
            unsigned count_added = dec.popVarint<unsigned>("number of items to add/change");
            for (unsigned i = 0; i < count_added; ++i)
            {
                Item<> item = types::decode(dec);
                int pos = Visitor::posForCode(item->serialisationCode());
                if (pos < 0)
                    throw wibble::exception::Consistency("parsing summary",
                            str::fmtf("unsupported typecode found: %d", (int)item->serialisationCode()));
                if (mdvec.size() <= (unsigned)pos)
                    mdvec.resize(pos + 1);
                mdvec[pos] = item;
            }

            // Decode the stats
            Item<Stats> stats = types::decode(dec).upcast<Stats>();

            // Strip undef values at the end of mdvec
            while (!mdvec.empty() && !mdvec.back().defined())
                mdvec.pop_back();

            // Produce a (metadata, stats) couple
            dest(mdvec, stats);
        }
    }
};

struct NodeAdder : public Visitor
{
    RootNode& root;
    size_t count;

    NodeAdder(RootNode& root) : root(root), count(0) {}

    virtual bool operator()(const std::vector< UItem<> >& md, const UItem<Stats>& stats)
    {
        ++count;
        root.add(md, stats);
        return true;
    }
};

}

size_t RootNode::decode1(utils::codec::Decoder& dec)
{
    // Stripe size
    // either: child count + children
    //     or: 0 + stats
    // msoSerLen used for number of bytes used for item lenght

    if (dec.len == 0) return 0;

    Node::buildMsoSerLen();

    NodeAdder adder(*this);
    Format1Decoder decoder(adder);
    decoder.decode(dec.buf, dec.len);

    return adder.count;
}

size_t RootNode::decode3(utils::codec::Decoder& dec)
{
    // Stripe size
    // stats size + stats
    // child size + children
    // item length encoded using varints
    if (dec.len == 0) return 0;

    NodeAdder adder(*this);
    Format3Decoder decoder(adder);
    decoder.decode(dec);

    return adder.count;
}

size_t RootNode::decode(const wibble::sys::Buffer& buf, unsigned version, const std::string& filename)
{
    using namespace utils::codec;

    buildItemMsoMap();

    // Check version and ensure we can decode
    switch (version)
    {
        case 1: {
            // Standard summary
            Decoder dec(buf);
            return decode1(dec);
        }
        case 2: {
            // LZO compressed summary
            if (buf.size() == 0) return 0;

            // Read uncompressed size
            ensureSize(buf.size(), 4, "uncompressed item size");
            uint32_t uncsize = decodeUInt((const unsigned char*)buf.data(), 4);

            sys::Buffer decomp = utils::compress::unlzo((const unsigned char*)buf.data() + 4, buf.size() - 4, uncsize);
            Decoder dec(decomp);
            return decode1(dec);
        }
        case 3: {
            // Compression type byte, node in new format
            if (buf.size() == 0) return 0;

            Decoder dec(buf);
            unsigned compression = dec.popUInt(1, "compression type");
            switch (compression)
            {
                case 0: // Uncompressed
                    return decode3(dec);
                case 1: { // LZO compressed
                    // Read uncompressed size
                    uint32_t uncsize = dec.popUInt(4, "uncompressed item size");
                    sys::Buffer decomp = utils::compress::unlzo(dec.buf, dec.len, uncsize);
                    Decoder uncdec(decomp);
                    return decode3(uncdec);
                }
                default:
                    throw wibble::exception::Consistency("parsing file " + filename, "file compression type is " + str::fmt(compression) + " but I can only decode 0 (uncompressed) or 1 (LZO)");
            }
        }
        default:
            throw wibble::exception::Consistency("parsing file " + filename, "version of the file is " + str::fmt(version) + " but I can only decode version 1 or 2");
    }
}

}
}

// vim:set ts=4 sw=4:
