#include <arki/types/tests.h>
#include <arki/metadata.h>
#include <arki/metadata/consumer.h>
#include <arki/metadata/test-generator.h>
#include "table.h"

namespace tut {
using namespace std;
using namespace arki::tests;
using namespace arki;
using namespace arki::types;
using namespace arki::summary;

namespace {

/// Function to add metadata to Tables
metadata_dest_func make_adder(Table& root)
{
    return [&](unique_ptr<Metadata> md) {
        root.merge(*md);
        return true;
    };
}

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
        gen.add(TYPE_ORIGIN, "GRIB1(200, 0, 1)");
        gen.add(TYPE_ORIGIN, "GRIB1(98, 0, 1)");
        gen.add(TYPE_PRODUCT, "GRIB1(200, 0, 1)");
        gen.add(TYPE_PRODUCT, "GRIB1(98, 0, 1)");
        gen.add(TYPE_PRODUCT, "GRIB1(98, 0, 2)");
        gen.add(TYPE_REFTIME, "2010-09-08T00:00:00");
        gen.add(TYPE_REFTIME, "2010-09-09T00:00:00");
        gen.add(TYPE_REFTIME, "2010-09-10T00:00:00");
        gen.generate(make_adder(root));
    }

    void fill_with_27_samples(Table& root) const
    {
        metadata::test::Generator gen("grib1");
        gen.add(TYPE_ORIGIN, "GRIB1(200, 0, 1)");
        gen.add(TYPE_ORIGIN, "GRIB1(98, 0, 1)");
        gen.add(TYPE_ORIGIN, "GRIB1(98, 0, 2)");
        gen.add(TYPE_PRODUCT, "GRIB1(200, 0, 1)");
        gen.add(TYPE_PRODUCT, "GRIB1(98, 0, 1)");
        gen.add(TYPE_PRODUCT, "GRIB1(98, 0, 2)");
        gen.add(TYPE_LEVEL, "GRIB1(0, 0, 0)");
        gen.add(TYPE_LEVEL, "GRIB1(1)");
        gen.add(TYPE_LEVEL, "GRIB1(2)");
        gen.add(TYPE_REFTIME, "2010-09-08T00:00:00");
        gen.add(TYPE_REFTIME, "2010-09-09T00:00:00");
        gen.add(TYPE_REFTIME, "2010-09-10T00:00:00");
        gen.generate(make_adder(root));
    }
};
TESTGRP(arki_summary_table);

// Test basic operations
def_test(1)
{
    // Test the empty table
    Table table;
    wassert(actual(table.empty()).istrue());
    Table empty;
    wassert(actual(table.equals(empty)).istrue());
    wassert(actual(table.size()) == 0u);

    // Add an item and test results
    table.merge(md);
    wassert(actual(table.empty()).isfalse());
    wassert(actual(table.equals(empty)).isfalse());
    wassert(actual(table.size()) == 1u);

    // Two different tables should compare the same even if they have different
    // intern pointers
    Table table1;
    table1.merge(md);
    wassert(actual(table1.size()) == 1u);
    wassert(actual(table1.equals(table)).istrue());
}

def_test(2)
{
    Table table;
    // Add the same item twice, they should get merged
    table.merge(md);
    table.merge(md);
    wassert(actual(table.empty()).isfalse());
    wassert(actual(table.size()) == 1u);
    wassert(actual(table.stats.count) == 2u);
}

// Build a table a bit larger, still no reallocs triggered
def_test(3)
{
    // Look at a bigger table
    Table table;
    fill_with_6_samples(table);
    wassert(actual(table.size()) == 6u);
}

// Build a table large enough to trigger a realloc
def_test(4)
{
    // Look at an even bigger table, this should trigger a realloc
    Table table3;
    fill_with_27_samples(table3);
    wassert(actual(table3.size()) == 27u);
    // Compare with itself to access all elements of the table and see if we got garbage
    wassert(actual(table3.equals(table3)).istrue());
}

// Test Row
def_test(5)
{
    Row row1;
    wassert(actual(row1.stats.count) == 0u);
    wassert(actual(row1.stats.size) == 0u);

    for (unsigned i = 0; i < Row::mso_size; ++i)
        row1.items[i] = (const Type*)((const char*)0 + i + 1);

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
