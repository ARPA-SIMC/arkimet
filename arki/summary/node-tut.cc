/*
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

#include <arki/tests/test-utils.h>
#include <arki/summary/node.h>
#include <arki/metadata.h>
#include <arki/metadata/consumer.h>
#include <arki/metadata/test-generator.h>
#include <arki/emitter/json.h>
#include <arki/emitter/memory.h>
#include <arki/matcher.h>
#include <arki/utils.h>
#include <arki/utils/codec.h>

#include <sstream>
#include <iostream>

#include "config.h"

#ifdef HAVE_LUA
#include <arki/tests/lua.h>
#endif

#ifdef HAVE_GRIBAPI
#include <arki/scan/grib.h>
#endif

namespace tut {
using namespace std;
using namespace arki;
using namespace arki::types;
using namespace arki::summary;

/**
 * Add metadata to RootNodes
 */
struct Adder : public metadata::Consumer
{
    RootNode& root;
    Adder(RootNode& root) : root(root) {}
    bool operator()(Metadata& md)
    {
        root.add(md);
        return true;
    }
};

struct arki_summary_node_shar {
    arki_summary_node_shar()
    {
    }

    void fill_with_6_samples(RootNode& root) const
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

template<> template<>
void to::test<1>()
{
}

// Test encoding
template<> template<>
void to::test<2>()
{
    using namespace arki::utils::codec;

    // Get a test trie
    RootNode root;
    fill_with_6_samples(root);

    // Encode it to binary
    string encoded;
    Encoder enc(encoded);
    root.encode(enc);

    ensure(encoded.size() > 0);

    // Decode it
    Decoder dec(encoded);
    auto_ptr<RootNode> root1(RootNode::decode3(dec));

    ensure_equals(root.compare(*root1), 0);
}

// Test Lua functions
template<> template<>
void to::test<3>()
{
#ifdef HAVE_LUA
    /*
    s.add(md2);

    tests::Lua test(
        "function test(s) \n"
        "  if s:count() ~= 3 then return 'count is '..s.count()..' instead of 3' end \n"
        "  if s:size() ~= 50 then return 'size is '..s.size()..' instead of 50' end \n"
        "  i = 0 \n"
        "  items = {} \n"
        "  for idx, entry in ipairs(s:data()) do \n"
        "    item, stats = unpack(entry) \n"
        "    for name, val in pairs(item) do \n"
        "      o = name..':'..tostring(val) \n"
        "      count = items[o] or 0 \n"
        "      items[o] = count + stats.count() \n"
        "    end \n"
        "    i = i + 1 \n"
        "  end \n"
        "  if i ~= 2 then return 'iterated '..i..' times instead of 2' end \n"
        "  c = items['origin:GRIB1(001, 002, 003)'] \n"
        "  if c ~= 1 then return 'origin1 c is '..tostring(c)..' instead of 1' end \n"
        "  c = items['origin:GRIB1(003, 004, 005)'] \n"
        "  if c ~= 2 then return 'origin2 c is '..c..' instead of 2' end \n"
        "  c = items['product:GRIB1(001, 002, 003)'] \n"
        "  if c ~= 1 then return 'product1 c is '..c..' instead of 1' end \n"
        "  c = items['product:GRIB1(002, 003, 004)'] \n"
        "  if c ~= 2 then return 'product2 c is '..c..' instead of 2' end \n"
        "  return nil\n"
        "end \n"
    );

    test.pusharg(s);
    ensure_equals(test.run(), "");
    */
#endif
}

}

// vim:set ts=4 sw=4:
