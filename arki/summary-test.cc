#include "types/tests.h"
#include "tests/lua.h"
#include "core/file.h"
#include "summary.h"
#include "summary/stats.h"
#include "metadata.h"
#include "types/origin.h"
#include "types/product.h"
#include "types/timerange.h"
#include "types/reftime.h"
#include "types/source.h"
#include "types/run.h"
#include "structured/json.h"
#include "structured/memory.h"
#include "structured/keys.h"
#include "matcher.h"
#include "metadata/collection.h"
#include "utils.h"
#include "utils/files.h"
#include "utils/sys.h"
#include "binary.h"
#include <sys/fcntl.h>
#include <sstream>

#ifdef HAVE_GRIBAPI
#include "scan/grib.h"
#endif

namespace {
using namespace std;
using namespace arki;
using namespace arki::types;
using namespace arki::core;
using namespace arki::tests;
using arki::core::Time;

struct Fixture : public arki::utils::tests::Fixture
{
    Summary s;
    Metadata md1;
    Metadata md2;

    Fixture()
    {
    }

    void test_setup()
    {
        md1.clear();
        md1.set(Origin::createGRIB1(1, 2, 3));
        md1.set(Product::createGRIB1(1, 2, 3));
        md1.set(Timerange::createGRIB1(1, timerange::SECOND, 0, 0));
        md1.set(Reftime::createPosition(Time(2007, 1, 2, 3, 4, 5)));
        md1.set_source(Source::createInline("grib1", 10));

        md2.clear();
        md2.set(Origin::createGRIB1(3, 4, 5));
        md2.set(Product::createGRIB1(2, 3, 4));
        md2.set(Timerange::createGRIB1(1, timerange::SECOND, 0, 0));
        md2.set(Reftime::createPosition(Time(2006, 5, 4, 3, 2, 1)));
        md2.set_source(Source::createInline("grib1", 20));

        s.clear();
        s.add(md1);
        s.add(md2);
    }
};

class Tests : public FixtureTestCase<Fixture>
{
    using FixtureTestCase::FixtureTestCase;

    void register_tests() override;
} tests("arki_summary");

void Tests::register_tests() {

// Test that it contains the right things
add_method("stats", [](Fixture& f) {
    // Check that it contains 2 metadata
    wassert(actual(f.s.count()) == 2u);
    wassert(actual(f.s.size()) == 30u);
});

// Test assignment and comparison
add_method("compare", [](Fixture& f) {
    Summary s1;
    s1.add(f.s);
    wassert_true(s1 == f.s);
    wassert(actual(s1.count()) == 2u);
    wassert(actual(s1.size()) == 30u);
});

// Test matching
add_method("match", [](Fixture& f) {
    Summary s1;
    wassert(actual(f.s.match(Matcher::parse("origin:GRIB1,1"))).istrue());
    f.s.filter(Matcher::parse("origin:GRIB1,1"), s1); wassert(actual(s1.count()) == 1u);

    s1.clear();
    wassert(actual(f.s.match(Matcher::parse("origin:GRIB1,2"))).isfalse());
    f.s.filter(Matcher::parse("origin:GRIB1,2"), s1); wassert(actual(s1.count()) == 0u);
});

// Test matching runs
add_method("match_run", [](Fixture& f) {
    Summary s;
    f.md1.set(run::Minute::create(0, 0));
    f.md2.set(run::Minute::create(12, 0));
    s.clear();
    s.add(f.md1);
    s.add(f.md2);

    Summary s1;
    wassert(actual(s.match(Matcher::parse("run:MINUTE,0"))).istrue());
    s.filter(Matcher::parse("run:MINUTE,0"), s1); wassert(actual(s1.count()) == 1u);

    s1.clear();
    wassert(actual(s.match(Matcher::parse("run:MINUTE,12"))).istrue());
    s.filter(Matcher::parse("run:MINUTE,12"), s1); wassert(actual(s1.count()) == 1u);
});

// Test filtering
add_method("filter", [](Fixture& f) {
    Summary s1;
    f.s.filter(Matcher::parse("origin:GRIB1,1"), s1);
    wassert(actual(s1.count()) == 1u);
    wassert(actual(s1.size()) == 10u);

    Summary s2;
    f.s.filter(Matcher::parse("origin:GRIB1,1"), s2);
    wassert(actual(s2.count()) == 1u);
    wassert(actual(s2.size()) == 10u);

    wassert_true(s1 == s2);
});

// Test serialisation to binary
add_method("binary", [](Fixture& f) {
    {
        vector<uint8_t> encoded = f.s.encode();
        Summary s1;
        BinaryDecoder dec(encoded);
        wassert_true(s1.read(dec, "(test memory buffer)"));
        wassert_true(s1 == f.s);
    }

    {
        vector<uint8_t> encoded = f.s.encode(true);
        Summary s1;
        BinaryDecoder dec(encoded);
        wassert_true(s1.read(dec, "(test memory buffer)"));
        wassert_true(s1 == f.s);
    }
});

// Test serialisation to Yaml
add_method("yaml", [](Fixture& f) {
    string st = f.s.to_yaml();
    Summary s2;
    auto reader = LineReader::from_chars(st.data(), st.size());
    s2.readYaml(*reader, "(test memory buffer)");
    wassert_true(s2 == f.s);
});

// Test serialisation to JSON
add_method("json", [](Fixture& f) {
    // Serialise to JSON;
    stringstream stream1;
    structured::JSON json(stream1);
    f.s.serialise(json, structured::keys_json);

    // Parse back
    stringstream stream2(stream1.str(), ios_base::in);
    structured::Memory parsed;
    structured::JSON::parse(stream2, parsed);

    Summary s2;
    wassert(s2.read(structured::keys_json, parsed.root()));
    wassert_true(s2 == f.s);
});

// Test merging summaries
add_method("merge", [](Fixture& f) {
    Summary s1;
    s1.add(f.s);
    wassert_true(s1 == f.s);
});

// Test serialisation of empty summary
add_method("binary_empty", [](Fixture& f) {
    Summary s;
    vector<uint8_t> encoded = s.encode();
    Summary s1;
    BinaryDecoder dec(encoded);
    wassert_true(s1.read(dec, "(test memory buffer)"));
    wassert_true(s1 == s);
});

// Test a case of metadata wrongly considered the same
add_method("regression0", [](Fixture& f) {
    Metadata tmd1;
    Metadata tmd2;
    Summary ts;

    tmd1.set(Origin::createGRIB1(1, 2, 3));
    tmd1.set(Product::createGRIB1(1, 2, 3));
    tmd1.set(Reftime::createPosition(Time(2007, 1, 2, 3, 4, 5)));
    tmd1.set_source(Source::createInline("grib1", 10));

    tmd2.set(Origin::createGRIB1(1, 2, 3));
    tmd2.set(Product::createGRIB1(1, 2, 3));
    tmd2.set(Reftime::createPosition(Time(2007, 1, 2, 3, 4, 5)));
    tmd2.set(Run::createMinute(12, 0));
    tmd2.set_source(Source::createInline("grib1", 15));

    ts.add(tmd1);
    ts.add(tmd2);

    wassert(actual(ts.count()) == 2u);
    wassert(actual(ts.size()) == 25u);
});

// Test Lua functions
add_method("lua", [](Fixture& f) {
#ifdef HAVE_LUA
    Summary s;
    f.s.add(f.md2);

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
		"      items[o] = count + stats.count \n"
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

    test.pusharg(f.s);
    wassert(actual(test.run()) == "");
#endif
});

// Summarise the test gribs
add_method("summarise_grib", [](Fixture& f) {
#ifdef HAVE_GRIBAPI
    Summary s1;
    Metadata md;

    metadata::TestCollection mds("inbound/test.grib1");
    s1.add(mds[0]);
    //s1.add(mds[1]);
    //s1.add(mds[2]);

    // Serialisation to binary
    vector<uint8_t> encoded = s1.encode();
    Summary s2;
    BinaryDecoder dec(encoded);
    wassert_true(s2.read(dec, "(test memory buffer)"));
    wassert_true(s1 == s2);

    // Serialisation to Yaml
    std::string st2 = s1.to_yaml();
    Summary s3;
    auto reader = LineReader::from_chars(st2.data(), st2.size());
    s3.readYaml(*reader, "(test memory buffer)");
    wassert_true(s3 == s1);
#endif
});

// Test adding a metadata plus stats
add_method("add_with_stats", [](Fixture& f) {
    Metadata md3;
    md3.set(Origin::createGRIB1(5, 6, 7));
    md3.set(Product::createGRIB1(4, 5, 6));
    md3.set(Timerange::createGRIB1(1, timerange::SECOND, 0, 0));
    md3.set(Reftime::createPosition(Time(2006, 5, 4, 3, 2, 1)));

    summary::Stats st;
    st.count = 5;
    st.size = 123456;
    st.begin = Time(2008, 7, 6, 5, 4, 3);
    st.end = Time(2008, 9, 8, 7, 6, 5);

    f.s.add(md3, st);

    // Check that it contains 2 metadata
    wassert(actual(f.s.count()) == 7u);
    wassert(actual(f.s.size()) == 123486u);
});

// Test resolveMatcher
add_method("resolvematcher", [](Fixture& f) {
    std::vector<ItemSet> res = f.s.resolveMatcher(Matcher::parse("origin:GRIB1,1,2,3; product:GRIB1,1,2,3 or GRIB1,2,3,4"));
    wassert(actual(res.size()) == 1u);

    ItemSet& is = res[0];
    wassert(actual(is.size()) == 2u);
    wassert(actual(Origin::createGRIB1(1, 2, 3)) == is.get(TYPE_ORIGIN));
    wassert(actual(Product::createGRIB1(1, 2, 3)) == is.get(TYPE_PRODUCT));
});

// Test loading an old summary
add_method("binary_old", [](Fixture& f) {
    Summary s;
    s.readFile("inbound/old.summary");
    wassert_true(s.count() > 0);
    // Compare with a summary with different msoSerLen
    {
        Summary s1;
        s1.add(s);
        wassert(actual(s.count()) == s1.count());
        wassert(actual(s.size()) == s1.size());
        wassert_true(s == s1);
    }

    // Ensure we can recode it
    // There was a bug where we could not really visit it, because the visitor
    // would only emit a stats when the visit depth reached msoSerLen, which is
    // not the case with summaries generated with lower msoSerLens

    // Serialisation to binary
    {
        vector<uint8_t> encoded = s.encode();
        Summary s2;
        BinaryDecoder dec(encoded);
        wassert_true(s2.read(dec, "(test memory buffer)"));
        wassert_true(s == s2);
    }

    // Serialisation to Yaml
    {
        string st = s.to_yaml();
        auto reader = LineReader::from_chars(st.data(), st.size());
        Summary s2;
        s2.readYaml(*reader, "(test memory buffer)");
        wassert(actual(s == s2));
    }
});

// Test a case where adding a metadata twice duplicated some nodes
add_method("regression1", [](Fixture& f) {
    f.s.add(f.md2);
    struct Counter : public summary::Visitor
    {
        size_t count;
        Counter() : count(0) {}
        virtual bool operator()(const std::vector<const Type*>&, const summary::Stats&)
        {
            ++count;
            return true;
        }
    } counter;
    f.s.visit(counter);

    wassert(actual(counter.count) == 2u);
});

// Test loading an old summary
add_method("binary_old1", [](Fixture& f) {
    Summary s;
    s.readFile("inbound/all.summary");
    wassert_true(s.count() > 0);
});

// Test filtering with an empty matcher
add_method("filter_empty_matcher", [](Fixture& f) {
    Summary s1;
    f.s.filter(Matcher(), s1);
    wassert_true(s1 == f.s);
    wassert(actual(s1.count()) == 2u);
    wassert(actual(s1.size()) == 30u);
});

// Test loading and saving summaries that summarise data where
// 0000-00-00T00:00:00Z is a valid timestamp
add_method("zero_timestamp", [](Fixture& f) {
    Summary s;
    s.readFile("inbound/00-00.bufr.summary");

    // Check that ranges are computed correctly even with all zeroes
    unique_ptr<types::Reftime> rt = s.getReferenceTime();
    wassert(actual(rt->period_begin()) == Time(0, 0, 0, 0, 0, 0));
    wassert(actual(rt->period_end()) == Time(0, 0, 0, 0, 0, 14));

    // Check that serialization does not throw exceptions
    stringstream out_yaml;
    out_yaml << *rt << endl;

    utils::sys::File out("/dev/null", O_WRONLY);
    s.write(out);
});

}

}
