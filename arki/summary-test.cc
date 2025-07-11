#include "core/binary.h"
#include "core/file.h"
#include "matcher.h"
#include "matcher/parser.h"
#include "metadata.h"
#include "metadata/collection.h"
#include "structured/json.h"
#include "structured/keys.h"
#include "structured/memory.h"
#include "summary.h"
#include "summary/stats.h"
#include "types/origin.h"
#include "types/product.h"
#include "types/reftime.h"
#include "types/run.h"
#include "types/source.h"
#include "types/tests.h"
#include "types/timerange.h"
#include "utils/files.h"
#include "utils/sys.h"
#include <sstream>
#include <sys/fcntl.h>

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
    std::shared_ptr<Metadata> md1;
    std::shared_ptr<Metadata> md2;
    matcher::Parser parser;

    Fixture() {}

    void test_setup()
    {
        md1 = std::make_shared<Metadata>();
        md1->test_set(Origin::createGRIB1(1, 2, 3));
        md1->test_set(Product::createGRIB1(1, 2, 3));
        md1->test_set(Timerange::createGRIB1(1, timerange::SECOND, 0, 0));
        md1->test_set(Reftime::createPosition(Time(2007, 1, 2, 3, 4, 5)));
        md1->set_source(Source::createInline(DataFormat::GRIB, 10));

        md2 = std::make_shared<Metadata>();
        md2->test_set(Origin::createGRIB1(3, 4, 5));
        md2->test_set(Product::createGRIB1(2, 3, 4));
        md2->test_set(Timerange::createGRIB1(1, timerange::SECOND, 0, 0));
        md2->test_set(Reftime::createPosition(Time(2006, 5, 4, 3, 2, 1)));
        md2->set_source(Source::createInline(DataFormat::GRIB, 20));

        s.clear();
        s.add(*md1);
        s.add(*md2);
    }
};

class Tests : public FixtureTestCase<Fixture>
{
    using FixtureTestCase::FixtureTestCase;

    void register_tests() override;
} tests("arki_summary");

void Tests::register_tests()
{

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
        wassert(actual(f.s.match(f.parser.parse("origin:GRIB1,1"))).istrue());
        f.s.filter(f.parser.parse("origin:GRIB1,1"), s1);
        wassert(actual(s1.count()) == 1u);

        s1.clear();
        wassert(actual(f.s.match(f.parser.parse("origin:GRIB1,2"))).isfalse());
        f.s.filter(f.parser.parse("origin:GRIB1,2"), s1);
        wassert(actual(s1.count()) == 0u);
    });

    // Test matching runs
    add_method("match_run", [](Fixture& f) {
        Summary s;
        f.md1->test_set(Run::createMinute(0, 0));
        f.md2->test_set(Run::createMinute(12, 0));
        s.clear();
        s.add(*f.md1);
        s.add(*f.md2);

        Summary s1;
        wassert(actual(s.match(f.parser.parse("run:MINUTE,0"))).istrue());
        s.filter(f.parser.parse("run:MINUTE,0"), s1);
        wassert(actual(s1.count()) == 1u);

        s1.clear();
        wassert(actual(s.match(f.parser.parse("run:MINUTE,12"))).istrue());
        s.filter(f.parser.parse("run:MINUTE,12"), s1);
        wassert(actual(s1.count()) == 1u);
    });

    // Test filtering
    add_method("filter", [](Fixture& f) {
        Summary s1;
        f.s.filter(f.parser.parse("origin:GRIB1,1"), s1);
        wassert(actual(s1.count()) == 1u);
        wassert(actual(s1.size()) == 10u);

        Summary s2;
        f.s.filter(f.parser.parse("origin:GRIB1,1"), s2);
        wassert(actual(s2.count()) == 1u);
        wassert(actual(s2.size()) == 10u);

        wassert_true(s1 == s2);
    });

    // Test serialisation to binary
    add_method("binary", [](Fixture& f) {
        {
            vector<uint8_t> encoded = f.s.encode();
            Summary s1;
            core::BinaryDecoder dec(encoded);
            wassert_true(s1.read(dec, "(test memory buffer)"));
            wassert_true(s1 == f.s);
        }

        {
            vector<uint8_t> encoded = f.s.encode(true);
            Summary s1;
            core::BinaryDecoder dec(encoded);
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
        structured::Memory parsed;
        structured::JSON::parse(stream1.str(), parsed);

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
        core::BinaryDecoder dec(encoded);
        wassert_true(s1.read(dec, "(test memory buffer)"));
        wassert_true(s1 == s);
    });

    // Test a case of metadata wrongly considered the same
    add_method("regression0", [](Fixture& f) {
        Metadata tmd1;
        Metadata tmd2;
        Summary ts;

        tmd1.test_set(Origin::createGRIB1(1, 2, 3));
        tmd1.test_set(Product::createGRIB1(1, 2, 3));
        tmd1.test_set(Reftime::createPosition(Time(2007, 1, 2, 3, 4, 5)));
        tmd1.set_source(Source::createInline(DataFormat::GRIB, 10));

        tmd2.test_set(Origin::createGRIB1(1, 2, 3));
        tmd2.test_set(Product::createGRIB1(1, 2, 3));
        tmd2.test_set(Reftime::createPosition(Time(2007, 1, 2, 3, 4, 5)));
        tmd2.test_set(Run::createMinute(12, 0));
        tmd2.set_source(Source::createInline(DataFormat::GRIB, 15));

        ts.add(tmd1);
        ts.add(tmd2);

        wassert(actual(ts.count()) == 2u);
        wassert(actual(ts.size()) == 25u);
    });

    // Summarise the test gribs
    add_method("summarise_grib", [](Fixture& f) {
#ifdef HAVE_GRIBAPI
        Summary s1;
        Metadata md;

        metadata::TestCollection mds("inbound/test.grib1");
        s1.add(mds[0]);
        // s1.add(mds[1]);
        // s1.add(mds[2]);

        // Serialisation to binary
        vector<uint8_t> encoded = s1.encode();
        Summary s2;
        core::BinaryDecoder dec(encoded);
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
        md3.test_set(Origin::createGRIB1(5, 6, 7));
        md3.test_set(Product::createGRIB1(4, 5, 6));
        md3.test_set(Timerange::createGRIB1(1, timerange::SECOND, 0, 0));
        md3.test_set(Reftime::createPosition(Time(2006, 5, 4, 3, 2, 1)));

        summary::Stats st;
        st.count = 5;
        st.size  = 123456;
        st.begin = Time(2008, 7, 6, 5, 4, 3);
        st.end   = Time(2008, 9, 8, 7, 6, 5);

        f.s.add(md3, st);

        // Check that it contains 2 metadata
        wassert(actual(f.s.count()) == 7u);
        wassert(actual(f.s.size()) == 123486u);
    });

    // Test loading an old summary
    add_method("binary_old", [](Fixture& f) {
        Summary s;
        wassert(s.read_file("inbound/old.summary"));
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
        // There was a bug where we could not really visit it, because the
        // visitor would only emit a stats when the visit depth reached
        // msoSerLen, which is not the case with summaries generated with lower
        // msoSerLens

        // Serialisation to binary
        {
            vector<uint8_t> encoded = s.encode();
            Summary s2;
            core::BinaryDecoder dec(encoded);
            wassert_true(s2.read(dec, "(test memory buffer)"));
            wassert_true(s == s2);
        }

        // Serialisation to Yaml
        {
            string st   = s.to_yaml();
            auto reader = LineReader::from_chars(st.data(), st.size());
            Summary s2;
            s2.readYaml(*reader, "(test memory buffer)");
            wassert(actual(s == s2));
        }
    });

    // Test a case where adding a metadata twice duplicated some nodes
    add_method("regression1", [](Fixture& f) {
        f.s.add(*f.md2);
        struct Counter : public summary::Visitor
        {
            size_t count;
            Counter() : count(0) {}
            bool operator()(const std::vector<const Type*>&,
                            const summary::Stats&) override
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
        s.read_file("inbound/all.summary");
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
        s.read_file("inbound/00-00.bufr.summary");

        // Check that ranges are computed correctly even with all zeroes
        core::Interval rt = s.get_reference_time();
        wassert(actual(rt.begin) == Time(0, 0, 0, 0, 0, 0));
        // This is actually 0000-00-00 00:00:14 + 1 second, normalised
        wassert(actual(rt.end) == Time(-1, 11, 30, 0, 0, 15));

        // Check that serialization does not throw exceptions
        utils::sys::File out("/dev/null", O_WRONLY);
        s.write(out);
    });
}

} // namespace
