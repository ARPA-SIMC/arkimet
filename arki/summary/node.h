#ifndef ARKI_SUMMARY_NODE_H
#define ARKI_SUMMARY_NODE_H

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


#include <arki/types.h>
#include <arki/summary/stats.h>
#include <vector>

namespace arki {
class Metadata;
class Matcher;

namespace summary {
struct Stats;
struct Visitor;
struct StatsVisitor;
struct ItemVisitor;

extern const types::Code mso[];
extern int* msoSerLen;

/**
 * Vector of owned Type pointers.
 *
 * FIXME: it would be nice if const_iterator gave pointers to const Type*, but
 * it seems overkill to implement it now.
 */
class TypeVector
{
protected:
    std::vector<types::Type*> vals;

public:
    typedef std::vector<types::Type*>::const_iterator const_iterator;

    TypeVector();
    TypeVector(const Metadata& md);
    TypeVector(const TypeVector& o);
    ~TypeVector();

    const_iterator begin() const { return vals.begin(); }
    const_iterator end() const { return vals.end(); }
    size_t size() const { return vals.size(); }
    bool empty() const { return vals.empty(); }

    bool operator==(const TypeVector&) const;
    bool operator!=(const TypeVector& o) const { return !operator==(o); }

    types::Type* operator[](unsigned i) { return vals[i]; }
    const types::Type* operator[](unsigned i) const { return vals[i]; }

    void resize(size_t new_size);

    /// Set a value, expanding the vector if needed
    void set(size_t pos, std::auto_ptr<types::Type> val);

    /// Set a value, expanding the vector if needed
    void set(size_t pos, const types::Type* val);

    /// Set a value to 0, or do nothing if pos > size()
    void unset(size_t pos);

    /// Remove trailing undefined items
    void rtrim();

    /**
     * Split this vector in two at \a pos.
     *
     * This vector will be shortened to be exactly of size \a pos.
     *
     * Returns a vector with the rest of the items
     */
    void split(size_t pos, TypeVector& dest);

    /// Return the raw pointer to the items
    const types::Type* const* raw_items() const { return vals.data(); }

protected:
    TypeVector& operator=(const TypeVector&);
};

struct Node
{
    /**
     * List of metadata items for this node.
     *
     * A metadata item can be 0, to represent items that do not have that
     * metadata.
     */
    TypeVector md;

    /**
     * Children representing the rest of the metadata.
     *
     * Invariants:
     *  - children are ordered (children[i].md < children[i+1].md).
     *  - no children can represent an empty sequence of metadata
     *    (if a child exists, some metadata in it or its subtree has a value)
     *  - having children does not mean that stats is undefined: two metadata
     *    can differ by an item being present in one and absent in the other
     *  - except for the root, can never be an empty vector
     */
    std::vector<Node*> children;

    /**
     * Statistics about the metadata scanned so far.
     */
    Stats stats;

    Node();
    Node(const Stats& stats);
    Node(const Node&);

#if 0
    /**
     * New root node initialized from the given metadata and stats.
     *
     * @param m
     *   A vector of all metadata items in mso order. The vector stops at the
     *   last defined item, so the last item of the vector is always defined
     *   unless the vector is empty
     * @param scanpos If present, only m[scanpos:] is used for this node.
     */
    Node(const std::vector< UItem<> >& m, const UItem<Stats>& st = 0, size_t scanpos = 0);
#endif

    ~Node();

    static std::auto_ptr<Node> createPopulated(const types::Type* const* items, unsigned items_size, const Stats& stats);

    /// Return a deep copy of this node
    Node* clone() const;

    bool equals(const Node& node) const;

    void dump(std::ostream& out, size_t depth=0) const;

    // Notifies the visitor of all the values of the given metadata item.
    // Due to the internal structure, the same item can be notified more than once.
    bool visitItem(size_t msoidx, ItemVisitor& visitor) const;

    /**
     * Visit all the contents of this node, notifying visitor of all the full
     * nodes found
     */
    bool visit(Visitor& visitor, std::vector<const types::Type*>& visitmd, size_t scanpos = 0) const;

    /**
     * Visit all the contents of this node, notifying visitor of all the full
     * nodes found that match the matcher
     */
    bool visitFiltered(const Matcher& matcher, Visitor& visitor, std::vector<const types::Type*>& visitmd, size_t scanpos = 0) const;

    /**
     * Add statistics about the \a size items in \a items into this node
     */
    void merge(const types::Type* const* items, size_t items_size, const Stats& stats);

    /**
     * Check if this node is a candidate for merging with the given sequence.
     *
     * A node is a candidate if both the sequence and its md are empty, or if
     * they have the first item in common.
     */
    bool candidate_for_merge(const types::Type* const* items, size_t items_size) const;

    /**
     * Split this node in two at the given position.
     *
     * This node's md will be truncated to pos.
     *
     * This node will end up with only one child, which contains the rest of md
     * and all the former children and stats.
     */
    void split(size_t pos);

    /// Get the trie depth (used only for development)
    unsigned devel_get_max_depth() const;

    /// Get the number of nodes in the trie (used only for development)
    unsigned devel_get_node_count() const;

    static void buildMsoSerLen();
    static void buildItemMsoMap();

    /// Number of item types that contribute to a summary context
    static const size_t msoSize;

private:
    Node& operator=(const Node&);
};

}
}

#endif
