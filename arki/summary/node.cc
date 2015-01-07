/*
 * summary/node - Summary node implementation
 *
 * Copyright (C) 2007--2011  ARPA-SIM <urpsim@smr.arpa.emr.it>
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

using namespace std;
using namespace wibble;
using namespace arki::utils::codec;
using namespace arki::types;

namespace arki {
namespace summary {

// Metadata Scan Order
const types::Code mso[] = {
        types::TYPE_ORIGIN,
        types::TYPE_PRODUCT,
        types::TYPE_LEVEL,
        types::TYPE_TIMERANGE,
        types::TYPE_AREA,
        types::TYPE_PRODDEF,
        types::TYPE_BBOX,
        types::TYPE_RUN,
        types::TYPE_QUANTITY,
        types::TYPE_TASK

};
const size_t Node::msoSize = sizeof(mso) / sizeof(types::Code);
int* msoSerLen = 0;

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

namespace {

bool item_equals(const Type* a, const Type* b)
{
    if (!a && !b) return true;
    if (!a || !b) return false;
    return a->equals(*b);
}

}

TypeVector::TypeVector() {}

TypeVector::TypeVector(const Metadata& md)
{
    for (size_t i = 0; i < Node::msoSize; ++i)
    {
        const Type* item = md.get(mso[i]);
        if (item) set(i, item);
    }
}

TypeVector::TypeVector(const TypeVector& o)
{
    vals.reserve(o.vals.size());
    for (const_iterator i = o.begin(); i != o.end(); ++i)
        vals.push_back(*i ? (*i)->clone() : 0);
}

TypeVector::~TypeVector()
{
    for (vector<Type*>::iterator i = vals.begin(); i != vals.end(); ++i)
        delete *i;
}

bool TypeVector::operator==(const TypeVector& o) const
{
    if (size() != o.size()) return false;
    const_iterator a = begin();
    const_iterator b = o.begin();
    while (a != end() && b != o.end())
        if (!item_equals(*a, *b))
            return false;
    return true;
}

void TypeVector::set(size_t pos, std::auto_ptr<types::Type> val)
{
    if (pos >= vals.size())
        vals.resize(pos + 1);
    else if (vals[pos])
        delete vals[pos];
    vals[pos] = val.release();
}

void TypeVector::set(size_t pos, const Type* val)
{
    if (pos >= vals.size())
        vals.resize(pos + 1);
    else if (vals[pos])
        delete vals[pos];

    if (val)
        vals[pos] = val->clone();
    else
        vals[pos] = 0;
}

void TypeVector::unset(size_t pos)
{
    if (pos >= vals.size()) return;
    delete vals[pos];
    vals[pos] = 0;
}

void TypeVector::resize(size_t new_size)
{
    if (new_size < size())
    {
        // If we are shrinking, we need to deallocate the elements that are left
        // out
        for (size_t i = new_size; i < size(); ++i)
            delete vals[i];
    }

    // For everything else, just go with the vector default of padding with
    // zeroes
    vals.resize(new_size);
    return;

}

void TypeVector::rtrim()
{
    while (!vals.empty() && !vals.back())
        vals.pop_back();
}

void TypeVector::split(size_t pos, TypeVector& dest)
{
    dest.vals.reserve(dest.size() + size() - pos);
    for (unsigned i = pos; i < size(); ++i)
        dest.vals.push_back(vals[i]);
    vals.resize(pos);
}

Node::Node() {}

Node::Node(const Stats& stats) : stats(stats) {}

Node::Node(const Node& o)
    : md(o.md), stats(o.stats)
{
    children.reserve(o.children.size());
    for (vector<Node*>::const_iterator i = o.children.begin(); i != o.children.end(); ++i)
        children.push_back((*i)->clone());
}

#if 0
Node::Node(const std::vector<Type*>& m, const Stats& st, size_t scanpos)
{
    for (vector<Type*>::const_iterator i = m.begin() + scanpos; i != m.end(); ++i)
        md.push_back((*i)->clone());
    stats = st.clone();
}
#endif
Node::~Node()
{
    for (vector<Node*>::iterator i = children.begin();
            i != children.end(); ++i)
        delete *i;
}

Node* Node::clone() const { return new Node(*this); }

bool Node::equals(const Node& node) const
{
    // Compare metadata stripes
    if (md != node.md) return false;

    // Compare stats
    if (stats != node.stats) return false;

    // Compare children
    if (children.size() != node.children.size()) return false;
    vector<Node*>::const_iterator a = children.begin();
    vector<Node*>::const_iterator b = node.children.begin();
    for (; a != children.end(); ++a, ++b)
        if (!(*a)->equals(**b)) return false;

    return true;
}

#if 0
int Node::compare(const Node& node) const
{
    // Compare metadata stripes
    if (int res = md.size() - node.md.size()) return res;
    for (size_t i = 0; i < md.size(); ++i)
    {
        if (!md[i] && node.md[i]) return -1;
        if (!md[i] && !node.md[i]) continue;
        if (md[i] && !node.md[i]) return 1;
        if (int res = md[i]->compare(node.md[i])) return res;
    }

    // Compare stats
    if (int res = stats.compare(node.stats)) return res;

    // Compare children
    if (int res = children.size() - node.children.size()) return res;
    vector<Node*>::const_iterator a = children.begin();
    vector<Node*>::const_iterator b = node.children.begin();
    for (; a != children.end(); ++a, ++b)
        if (int res = (*a)->compare(**b)) return res;
    return 0;
}
#endif

void Node::dump(std::ostream& out, size_t depth) const
{
    string head(depth, ' ');
    out << head << md.size() << " md:" << endl;
    for (size_t i = 0; i < md.size(); ++i)
        if (md[i])
            out << head << *md[i] << endl;
        else
            out << head << "--" << endl;
    out << head << "Stats:" << stats;
    out << head << children.size() << " children:" << endl;
    for (vector<Node*>::const_iterator i = children.begin(); i != children.end(); ++i)
        (*i)->dump(out, depth+1);
}

bool Node::visitItem(size_t msoidx, ItemVisitor& visitor) const
{
    if (msoidx < md.size())
    {
        if (md[msoidx])
            return visitor(*md[msoidx]);
        else
            return true;
    }

    for (vector<Node*>::const_iterator i = children.begin();
            i != children.end(); ++i)
        if (!(*i)->visitItem(msoidx - md.size(), visitor))
            return false;

    return true;
}

static inline void mdvec_set(vector<const Type*>& mdvec, size_t pos, const Type* val)
{
    if (pos >= mdvec.size())
        mdvec.resize(pos + 1);
    mdvec[pos] = val;
}

bool Node::visit(Visitor& visitor, std::vector<const Type*>& visitmd, size_t scanpos) const
{
    // Set this node's metadata in visitmd
    for (size_t i = 0; i < md.size(); ++i)
        mdvec_set(visitmd, scanpos + i, md[i]);

    // If we are a leaf node, emit and stop here
    if (children.empty())
    {
        visitmd.resize(scanpos + md.size());
        if (!visitor(visitmd, stats))
            return false;
        return true;
    }

    // If we have children, visit them
    for (vector<Node*>::const_iterator i = children.begin();
            i != children.end(); ++i)
        if (!(*i)->visit(visitor, visitmd, scanpos + md.size()))
            return false;

    return true;
}

bool Node::visitFiltered(const Matcher& matcher, Visitor& visitor, std::vector<const Type*>& visitmd, size_t scanpos) const
{
    // Check if the matcher is ok with this node; if it's not, return true right away
    if (matcher.m_impl)
    {
        const matcher::AND& mand = *matcher.m_impl;
        for (size_t i = 0; i < md.size(); ++i)
        {
            matcher::AND::const_iterator j = mand.find(mso[scanpos + i]);
            if (j == mand.end()) continue;
            if (!md[i]) return true;
            if (!j->second->matchItem(*md[i])) return true;
        }
        matcher::AND::const_iterator rm = mand.find(types::TYPE_REFTIME);
        if (rm != mand.end() && !rm->second->matchItem(*stats.reftimeMerger.makeReftime()))
            return true;
    }

    // Set this node's metadata in visitmd
    for (size_t i = 0; i < md.size(); ++i)
        mdvec_set(visitmd, scanpos + i, md[i]);

    // If we are a leaf node, emit and stop here
    if (children.empty())
    {
        visitmd.resize(scanpos + md.size());
        if (!visitor(visitmd, stats))
            return false;
        return true;
    }

    // If we have children, visit them
    for (vector<Node*>::const_iterator i = children.begin(); i != children.end(); ++i)
        if (!(*i)->visitFiltered(matcher, visitor, visitmd, scanpos + md.size()))
            return false;

    return true;
}

void Node::split(size_t pos)
{
    /// Create a new node with a copy of our stats
    auto_ptr<Node> new_node(new Node(stats));

    /// Split the metadata between us and the new node
    md.split(pos, new_node->md);

    /// Hand over all children to the new node
    new_node->children = children;
    children.clear();

    /// Keep the new node as the only child
    children.push_back(new_node.release());
}

bool Node::candidate_for_merge(const types::Type* const* items, size_t items_size) const
{
    if (items_size == 0)
        return md.empty();

    if (md.empty()) return false;

    return item_equals(md[0], items[0]);
}

void Node::merge(const types::Type* const* items, size_t items_size, const Stats& stats)
{
    // Compute the number of common items
    unsigned common = 0;
    for ( ; common < items_size && common < md.size(); ++common)
        if (!item_equals(items[common], md[common]))
            break;

    // If no items are in common or only some items are in common, split the
    // node
    if (common < md.size())
        split(common);

    // Now all our items are in common

    // Advance items to the (possibly empty) tail of remaining items
    items += common;
    items_size -= common;

    if (items_size == 0 && children.empty())
    {
        // We are exactly the same: merge the stats
        this->stats.merge(stats);
        return;
    }

    // Look for a child to merge into
    for (vector<Node*>::iterator i = children.begin(); i != children.end(); ++i)
    {
        if ((*i)->candidate_for_merge(items, items_size))
        {
            (*i)->merge(items + common, items_size - common, stats);
            this->stats.merge(stats);
            return;
        }
    }

    // No children were found to merge into: create a new one
    children.push_back(createPopulated(items, items_size, stats).release());
    this->stats.merge(stats);
}

auto_ptr<Node> createPopulated(const types::Type* const* items, unsigned items_size, const Stats& stats)
{
    auto_ptr<Node> new_node(new Node(stats));
    for (unsigned i = 0; i < items_size; ++i)
        new_node->md.set(i, items[i]);
}

#if 0
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

#endif

}
}

// vim:set ts=4 sw=4:
