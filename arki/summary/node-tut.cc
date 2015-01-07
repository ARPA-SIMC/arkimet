/*
 * Copyright (C) 2007--2015  ARPA-SIM <urpsim@smr.arpa.emr.it>
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

#include <arki/types/tests.h>
#include <arki/summary/node.h>
#include <arki/metadata.h>
#include <arki/metadata/consumer.h>
#include <arki/metadata/test-generator.h>

namespace tut {
using namespace std;
using namespace wibble::tests;
using namespace arki;
using namespace arki::types;
using namespace arki::summary;

/**
 * Add metadata to RootNodes
 */
struct Adder : public metadata::Consumer
{
    Node*& root;
    Adder(Node*& root) : root(root) {}
    bool operator()(Metadata& md)
    {
        TypeVector tv;
        Node::md_to_tv(md, tv);
        if (!root)
            root = new Node(tv.raw_items(), tv.size(), Stats());
        else
            root->merge(tv.raw_items(), tv.size(), Stats());
        return true;
    }
};

struct arki_summary_node_shar {
    arki_summary_node_shar()
    {
        Node::buildItemMsoMap();
    }

    void fill_with_6_samples(Node*& root) const
    {
        metadata::test::Generator gen("grib1");
        gen.add(types::TYPE_ORIGIN, "GRIB1(200, 0, 1)");
        gen.add(types::TYPE_ORIGIN, "GRIB1(98, 0, 1)");
        gen.add(types::TYPE_REFTIME, "2010-09-08T00:00:00");
        gen.add(types::TYPE_REFTIME, "2010-09-09T00:00:00");
        gen.add(types::TYPE_REFTIME, "2010-09-10T00:00:00");

        Adder adder(root);
        gen.generate(adder);
    }
};
TESTGRP(arki_summary_node);

// Test basic operations
template<> template<>
void to::test<1>()
{
    Metadata md;
    md.set(decodeString(TYPE_REFTIME, "2015-01-05T12:00:00Z"));
    auto_ptr<Type> item1(decodeString(TYPE_ORIGIN, "GRIB1(98, 1, 2)"));
    auto_ptr<Type> item2(decodeString(TYPE_PRODUCT, "GRIB1(98, 1, 2)"));
    auto_ptr<Type> item4(decodeString(TYPE_TIMERANGE, "GRIB1(1)"));
    Type* t[] = { item1.get(), item2.get(), 0, item4.get() };

    // Create a node
    auto_ptr<Node> root(new Node(t, 1, Stats(md)));
    wassert(actual(root->md.empty()).isfalse());
    wassert(actual(root->children.empty()).istrue());
    //wassert(actual(root->equals(Node())).isfalse());
    wassert(actual(root->devel_get_max_depth()) == 1);
    wassert(actual(root->devel_get_node_count()) == 1);

    // Merge in a new node
    root->merge(t, 2, Stats(md));
    root->dump(cerr);
    wassert(actual(root->md.empty()).isfalse());
    wassert(actual(root->children.empty()).isfalse());
    //wassert(actual(root->equals(Node())).isfalse());
    wassert(actual(root->devel_get_max_depth()) == 2);
    wassert(actual(root->devel_get_node_count()) == 3);

    // And another one
    root->merge(t, 4, Stats(md));
    root->dump(cerr);
    wassert(actual(root->md.empty()).isfalse());
    wassert(actual(root->children.empty()).isfalse());
    //wassert(actual(root->equals(Node())).isfalse());
    wassert(actual(root->devel_get_max_depth()) == 3);
    wassert(actual(root->devel_get_node_count()) == 4);

//    /**
//     * Add statistics about the \a size items in \a items into this node
//     */
//    void merge(const types::Type* const* items, size_t items_size, const Stats& stats);

//    Node(const Stats& stats);
//    Node(const Node&);
//
//    static std::auto_ptr<Node> createPopulated(const types::Type* const* items, unsigned items_size, const Stats& stats);
//
//    /// Return a deep copy of this node
//    Node* clone() const;
//
//    // Notifies the visitor of all the values of the given metadata item.
//    // Due to the internal structure, the same item can be notified more than once.
//    bool visitItem(size_t msoidx, ItemVisitor& visitor) const;
//
//    /**
//     * Visit all the contents of this node, notifying visitor of all the full
//     * nodes found
//     */
//    bool visit(Visitor& visitor, std::vector<const types::Type*>& visitmd, size_t scanpos = 0) const;
//
//    /**
//     * Visit all the contents of this node, notifying visitor of all the full
//     * nodes found that match the matcher
//     */
//    bool visitFiltered(const Matcher& matcher, Visitor& visitor, std::vector<const types::Type*>& visitmd, size_t scanpos = 0) const;
//
//    /**
//     * Add statistics about the \a size items in \a items into this node
//     */
//    void merge(const types::Type* const* items, size_t items_size, const Stats& stats);
//
//    /**
//     * Check if this node is a candidate for merging with the given sequence.
//     *
//     * A node is a candidate if both the sequence and its md are empty, or if
//     * they have the first item in common.
//     */
//    bool candidate_for_merge(const types::Type* const* items, size_t items_size) const;
//
//    /**
//     * Split this node in two at the given position.
//     *
//     * This node's md will be truncated to pos.
//     *
//     * This node will end up with only one child, which contains the rest of md
//     * and all the former children and stats.
//     */
//    void split(size_t pos);
//
//    /// Get the trie depth (used only for development)
//    unsigned devel_get_max_depth() const;
//
//    /// Get the number of nodes in the trie (used only for development)
//    unsigned devel_get_node_count() const;
//
//    static void md_to_tv(const Metadata& md, types::TypeVector& out);
}

// Test encoding
template<> template<>
void to::test<2>()
{
    using namespace arki::utils::codec;

    // Get a test trie
    Node* root = 0;
    fill_with_6_samples(root);

    wassert(actual(root->devel_get_max_depth()) == 2);
    wassert(actual(root->devel_get_node_count()) == 6);

    delete root;
}

}
