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
#include <arki/metadata.h>
#include <arki/metadata/consumer.h>
#include <arki/metadata/test-generator.h>
#include "table.h"

namespace tut {
using namespace std;
using namespace wibble::tests;
using namespace arki;
using namespace arki::types;
using namespace arki::summary;

namespace {

/**
 * Add metadata to Tables
 */
struct Adder : public metadata::Eater
{
    Table& root;
    Adder(Table& root) : root(root) {}
    bool eat(unique_ptr<Metadata>&& md) override
    {
        root.merge(*md);
        return true;
    }
};

}

struct arki_summary_table_shar {
    Metadata md;

    arki_summary_table_shar()
    {
        md.set(decodeString(TYPE_ORIGIN, "GRIB1(98, 1, 2)"));
        md.set(decodeString(TYPE_PRODUCT, "GRIB1(98, 1, 2)"));
        md.set(decodeString(TYPE_TIMERANGE, "GRIB1(1)"));
        md.set(decodeString(TYPE_REFTIME, "2015-01-05T12:00:00Z"));
    }

    void fill_with_6_samples(Table& root) const
    {
        metadata::test::Generator gen("grib1");
        gen.add(types::TYPE_ORIGIN, "GRIB1(200, 0, 1)");
        gen.add(types::TYPE_ORIGIN, "GRIB1(98, 0, 1)");
        gen.add(types::TYPE_PRODUCT, "GRIB1(200, 0, 1)");
        gen.add(types::TYPE_PRODUCT, "GRIB1(98, 0, 1)");
        gen.add(types::TYPE_PRODUCT, "GRIB1(98, 0, 2)");
        gen.add(types::TYPE_REFTIME, "2010-09-08T00:00:00");
        gen.add(types::TYPE_REFTIME, "2010-09-09T00:00:00");
        gen.add(types::TYPE_REFTIME, "2010-09-10T00:00:00");

        Adder adder(root);
        gen.generate(adder);
    }

    void fill_with_27_samples(Table& root) const
    {
        metadata::test::Generator gen("grib1");
        gen.add(types::TYPE_ORIGIN, "GRIB1(200, 0, 1)");
        gen.add(types::TYPE_ORIGIN, "GRIB1(98, 0, 1)");
        gen.add(types::TYPE_ORIGIN, "GRIB1(98, 0, 2)");
        gen.add(types::TYPE_PRODUCT, "GRIB1(200, 0, 1)");
        gen.add(types::TYPE_PRODUCT, "GRIB1(98, 0, 1)");
        gen.add(types::TYPE_PRODUCT, "GRIB1(98, 0, 2)");
        gen.add(types::TYPE_LEVEL, "GRIB1(0, 0, 0)");
        gen.add(types::TYPE_LEVEL, "GRIB1(1)");
        gen.add(types::TYPE_LEVEL, "GRIB1(2)");
        gen.add(types::TYPE_REFTIME, "2010-09-08T00:00:00");
        gen.add(types::TYPE_REFTIME, "2010-09-09T00:00:00");
        gen.add(types::TYPE_REFTIME, "2010-09-10T00:00:00");

        Adder adder(root);
        gen.generate(adder);
    }
};
TESTGRP(arki_summary_table);

// Test basic operations
template<> template<>
void to::test<1>()
{
    // Test the empty table
    Table table;
    wassert(actual(table.empty()).istrue());
    wassert(actual(table.equals(Table())).istrue());
    wassert(actual(table.size()) == 0);

    // Add an item and test results
    table.merge(md);
    wassert(actual(table.empty()).isfalse());
    wassert(actual(table.equals(Table())).isfalse());
    wassert(actual(table.size()) == 1);

    // Two different tables should compare the same even if they have different
    // intern pointers
    Table table1;
    table1.merge(md);
    wassert(actual(table1.size()) == 1);
    wassert(actual(table1.equals(table)).istrue());
}

template<> template<>
void to::test<2>()
{
    Table table;
    // Add the same item twice, they should get merged
    table.merge(md);
    table.merge(md);
    wassert(actual(table.empty()).isfalse());
    wassert(actual(table.size()) == 1);
    wassert(actual(table.stats.count) == 2);
}

// Build a table a bit larger, still no reallocs triggered
template<> template<>
void to::test<3>()
{
    // Look at a bigger table
    Table table;
    fill_with_6_samples(table);
    wassert(actual(table.size()) == 6);
}

// Build a table large enough to trigger a realloc
template<> template<>
void to::test<4>()
{
    // Look at an even bigger table, this should trigger a realloc
    Table table3;
    fill_with_27_samples(table3);
    wassert(actual(table3.size()) == 27);
    // Compare with itself to access all elements of the table and see if we got garbage
    wassert(actual(table3.equals(table3)).istrue());
}

// Test Row
template<> template<>
void to::test<5>()
{
    Row row1;
    wassert(actual(row1.stats.count) == 0);
    wassert(actual(row1.stats.size) == 0);

    for (unsigned i = 0; i < Row::mso_size; ++i)
        row1.items[i] = (const Type*)(i + 1);

    Row row2(row1);
    wassert(actual(row1 == row2).istrue());

    row2.set_to_zero();
    wassert(actual(row2 < row1).istrue());
    wassert(actual(row1 == row2).isfalse());
}

//    /// Return the intern version of an item
//    const types::Type* intern(unsigned pos, std::unique_ptr<types::Type> item);
//
//    /// Merge a row into the table
//    void merge(const Metadata& md, const Stats& st);
//
//    /// Merge a row into the table
//    void merge(const std::vector<const types::Type*>& md, const Stats& st);
//
//    /// Merge a row into the table, keeping only the items whose mso index is in \a positions
//    void merge(const std::vector<const types::Type*>& md, const Stats& st, const std::vector<unsigned>& positions);
//
//    /// Merge an entry decoded from a mapping
//    void merge(const emitter::memory::Mapping& val);
//
//    /// Merge a row into the table
//    void merge(const Row& row);
//
//    /// Merge rows read from a yaml input stream
//    bool merge_yaml(std::istream& in, const std::string& filename);
//
//    void dump(std::ostream& out) const;
//
//    // Notifies the visitor of all the values of the given metadata item.
//    // Due to the internal structure, the same item can be notified more than once.
//    bool visitItem(size_t msoidx, ItemVisitor& visitor) const;
//
//    /**
//     * Visit all the contents of this node, notifying visitor of all the full
//     * nodes found
//     */
//    bool visit(Visitor& visitor) const;
//
//    /**
//     * Visit all the contents of this node, notifying visitor of all the full
//     * nodes found that match the matcher
//     */
//    bool visitFiltered(const Matcher& matcher, Visitor& visitor) const;

}
