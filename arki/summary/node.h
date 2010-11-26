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
#include <vector>

namespace arki {
class Metadata;
class Matcher;

namespace summary {
struct Stats;
struct Visitor;
struct StatsVisitor;
struct ItemVisitor;

struct Node : public refcounted::Base
{
    // Metadata represented here
    std::vector< UItem<> > md;

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
    UItem<Stats> stats;

    Node();

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

    virtual ~Node();

    // Visit all the contents of this node, notifying visitor of all the full
    // nodes found
    bool visit(Visitor& visitor, std::vector< UItem<> >& visitmd, size_t scanpos = 0) const;

    // Visit all the stats contained in this node and all subnotes
    bool visitStats(StatsVisitor& visitor) const;

    // Notifies the visitor of all the values of the given metadata item.
    // Due to the internal structure, the same item can be notified more than once.
    bool visitItem(size_t msoidx, ItemVisitor& visitor) const;

    /**
     * Visit the whole tree, filtered with a Matcher.
     *
     * This method can only be called in the root node, or it will be out of
     * sync with the metadata positions.
     */
    bool visitFiltered(const Matcher& matcher, Visitor& visitor) const;

    // Visit all the contents of this node, notifying visitor of all the full
    // nodes found that match the matcher
    bool visitFiltered(const Matcher& matcher, Visitor& visitor, std::vector< UItem<> >& visitmd, size_t scanpos = 0) const;

    /**
     * Obtain a node for the given metadata combination.
     *
     * If the node exists, return it. Else, create it in the trie.
     *
     * @param m
     *   A vector of all metadata items in mso order. The vector stops at the
     *   last defined item, so the last item of the vector is always defined
     *   unless the vector is empty
     * @param scanpos
     *   mso offset for this node
     * @return
     *   Node with stats for the given metadata combination
     */
    Node* obtain_node(const std::vector< UItem<> >& m, size_t scanpos = 0);

    /**
     * Split this node in two at the given position.
     *
     * This node's md will be truncated to pos.
     *
     * This node will end up with only one child, which contains the rest of md
     * and all the former children and stats.
     */
    void split(size_t pos);

    /**
     * Return a deep copy of this node
     */
    Node* clone() const;

    int compare(const Node& node) const;

    void dump(std::ostream& out, size_t depth=0) const;

    static void buildMsoSerLen();
    static void buildItemMsoMap();

    /// Number of item types that contribute to a summary context
    static const size_t msoSize;
};

struct RootNode : public Node
{
    /**
     * Create an empty root node.
     *
     * This is not normally used, unless during decoding or for tests.
     */
    RootNode();

    /**
     * New root node initialized from the given metadata.
     *
     * This can only be used to create a root node.
     */
    RootNode(const Metadata& m);

    /**
     * New root node initialized from the given metadata and stats.
     *
     * This can only be used to create a root node.
     */
    RootNode(const Metadata& m, const UItem<Stats>& st);

    /**
     * New root node initialized from the given metadata and stats.
     *
     * @param m
     *   A vector of all metadata items in mso order. The vector stops at the
     *   last defined item, so the last item of the vector is always defined
     *   unless the vector is empty
     */
    RootNode(const std::vector< UItem<> >& m, const UItem<Stats>& st = 0);

    /**
     * Visit the whole tree.
     *
     * This method can only be called in the root node, or it will be out of
     * sync with the metadata positions.
     */
    bool visit(Visitor& visitor) const;

    /**
     * Add a metadata item
     *
     * This only works if called on the root node, otherwise it gives
     * unpredictable results.
     */
    void add(const Metadata& m);

    /**
     * Add a metadata item and associated stats
     *
     * This only works if called on the root node, otherwise it gives
     * unpredictable results.
     */
    void add(const Metadata& m, const UItem<Stats>& st);

    /**
     * Add the given metadata and stats to the tree rooted in this node.
     *
     * This only works if called on the root node, otherwise it gives
     * unpredictable results.
     *
     * @param m
     *   A vector of all metadata items in mso order. The vector stops at the
     *   last defined item, so the last item of the vector is always defined
     *   unless the vector is empty
     * @param st
     *   Stats to add
     */
    void add(const std::vector< UItem<> >& m, const UItem<Stats>& st);

    /**
     * Return a deep copy of this node
     */
    RootNode* clone() const;

    /// Encode the whole trie to the given encoder
    void encode(utils::codec::Encoder& enc) const;

    // Decode formats 1 and 2
    static RootNode* decode1(utils::codec::Decoder& dec);
    // Decode format 3
    static RootNode* decode3(utils::codec::Decoder& dec);

    static RootNode* decode(const wibble::sys::Buffer& buf, unsigned version, const std::string& filename);
};

}
}

// vim:set ts=4 sw=4:
#endif
