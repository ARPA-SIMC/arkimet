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
    Node& root;
    Adder(Node& root) : root(root) {}
    bool operator()(Metadata& md)
    {
        TypeVector tv(md);
        root.merge(tv.raw_items(), tv.size(), Stats());
        return true;
    }
};

struct arki_summary_node_shar {
    arki_summary_node_shar()
    {
        Node::buildItemMsoMap();
    }

    void fill_with_6_samples(Node& root) const
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

// Test encoding
template<> template<>
void to::test<1>()
{
    using namespace arki::utils::codec;

    // Get a test trie
    Node root;
    fill_with_6_samples(root);

    wassert(actual(root.devel_get_max_depth()) == 2);
    wassert(actual(root.devel_get_node_count()) == 6);
}

}
