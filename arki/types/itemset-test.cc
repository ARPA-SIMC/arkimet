#include "arki/types/tests.h"
#include "itemset.h"
#include "arki/types/origin.h"
#include "arki/types/product.h"
#include "arki/types/level.h"
#include "arki/types/timerange.h"
#include "arki/types/reftime.h"
#include "arki/types/area.h"
#include "arki/types/proddef.h"
#include "arki/types/values.h"

using namespace std;
using namespace arki::tests;
using namespace arki;
using namespace arki::types;
using arki::core::Time;

namespace {

struct Fixture : public arki::utils::tests::Fixture
{
    ItemSet md;
    ValueBag testValues;

    Fixture()
    {
        using namespace arki::types::values;
        // 100663295 == 0x5ffffff
        testValues = ValueBag::parse("foo=5, bar=5000, baz=-200, moo=100663295, antani=-1, blinda=0, supercazzola=-1234567, pippo=pippo, pluto=\"12\", cippo=\"\"");
    }

    void fill(ItemSet& md)
    {
        md.set(Origin::createGRIB1(1, 2, 3));
        md.set(Product::createGRIB1(1, 2, 3));
        md.set(Level::createGRIB1(114, 12, 34));
        md.set(Timerange::createGRIB1(1, 1, 2, 3));
        md.set(area::GRIB::create(testValues));
        md.set(Proddef::createGRIB(testValues));
        md.set(Reftime::createPosition(Time(2006, 5, 4, 3, 2, 1)));
    }
};

class Tests : public FixtureTestCase<Fixture>
{
    using FixtureTestCase::FixtureTestCase;

    void register_tests() override;
} tests("arki_itemset");

template<typename T>
static inline void test_basic_itemset_ops(const std::string& vlower, const std::string& vhigher)
{
    types::Code code = types::traits<T>::type_code;
    ItemSet md;
    unique_ptr<Type> vlo = decodeString(code, vlower);
    unique_ptr<Type> vhi = decodeString(code, vhigher);

    wassert(actual(md.size()) == 0u);
    wassert(actual(md.empty()).istrue());
    wassert(actual(md.has(code)).isfalse());

    // Add one element and check that it can be retrieved
    md.set(vlo->cloneType());
    wassert(actual(md.size()) == 1u);
    wassert(actual(md.empty()).isfalse());
    wassert(actual(md.has(code)).istrue());
    wassert(actual(vlo) == md.get(code));

    // Test templated get
    const T* t = md.get<T>();
    wassert(actual(t).istrue());
    wassert(actual(vlo) == t);

    // Add the same again and check that things are still fine
    md.set(vlo->cloneType());
    wassert(actual(md.size()) == 1u);
    wassert(actual(md.empty()).isfalse());
    wassert(actual(md.has(code)).istrue());
    wassert(actual(vlo) == md.get(code));

    // Add a different element and check that it gets replaced
    md.set(vhi->cloneType());
    wassert(actual(md.size()) == 1u);
    wassert(actual(md.empty()).isfalse());
    wassert(actual(md.has(code)).istrue());
    wassert(actual(vhi) == md.get(code));

    // Test equality and comparison between ItemSets
    ItemSet md1;
    md1.set(vhi->cloneType());
    wassert(actual(md == md1).istrue());
    wassert(actual(md.compare(md1)) == 0);

    // Set a different item and verify that we do not test equal anymore
    md.set(vlo->cloneType());
    wassert(actual(md != md1).istrue());
    wassert(actual(md.compare(md1)) < 0);
    wassert(actual(md1.compare(md)) > 0);

    // Test unset
    md.unset(code);
    wassert(actual(md.size()) == 0u);
    wassert(actual(md.empty()).istrue());
    wassert(actual(md.has(code)).isfalse());

    // Test clear
    md1.clear();
    wassert(actual(md.size()) == 0u);
    wassert(actual(md.empty()).istrue());
    wassert(actual(md.has(code)).isfalse());
}

void Tests::register_tests() {

/*
 * It looks like most of these tests are just testing if std::set and
 * std::vector work.
 *
 * I thought about removing them, but then I relised that they're in fact
 * testing if all our metadata items work properly inside stl containers, so I
 * decided to keep them.
 */

// Test with several item types
add_method("basic_ops", [](Fixture& f) {
    wassert(test_basic_itemset_ops<Origin>("GRIB1(1, 2, 3)", "GRIB1(2, 3, 4)"));
    wassert(test_basic_itemset_ops<Origin>("BUFR(1, 2)", "BUFR(2, 3)"));
    wassert(test_basic_itemset_ops<Reftime>("2006-05-04T03:02:01Z", "2008-07-06T05:04:03"));
#if 0
	// Test PERIOD reference times
	md.reftime.set(Time(2007, 6, 5, 4, 3, 2), Time(2008, 7, 6, 5, 4, 3));
	ensure_equals(md.reftime.style, Reftime::PERIOD);

	Time tbegin;
	Time tend;
	md.reftime.get(tbegin, tend);
	ensure_equals(tbegin, Time(2007, 6, 5, 4, 3, 2));
	ensure_equals(tend, Time(2008, 7, 6, 5, 4, 3));

// Work-around for a bug in old gcc versions
#if __GNUC__ && __GNUC__ < 4
#else
	// Test open-ended PERIOD reference times
	md.reftime.set(tbegin, Time());
	ensure_equals(md.reftime.style, Reftime::PERIOD);

	tbegin = Time();
	tend = Time();
	md.reftime.get(tbegin, tend);

	ensure_equals(tbegin, Time(2007, 6, 5, 4, 3, 2));
	ensure_equals(tend, Time());
#endif
#endif
    wassert(test_basic_itemset_ops<Product>("GRIB1(1, 2, 3)", "GRIB1(2, 3, 4)"));
    wassert(test_basic_itemset_ops<Product>("BUFR(1, 2, 3, name=antani)", "BUFR(1, 2, 3, name=blinda)"));
    wassert(test_basic_itemset_ops<Level>("GRIB1(114, 260, 123)", "GRIB1(120,280,123)"));
    wassert(test_basic_itemset_ops<Timerange>("GRIB1(1)", "GRIB1(2, 3y, 4y)"));
});

#if 0
// Test areas
def_test(9)
{
	ValueBag test1;
	test1.set("foo", Value::create_integer(5));
	test1.set("bar", Value::create_integer(5000));
	test1.set("baz", Value::create_integer(-200));
	test1.set("moo", Value::create_integer(0x5ffffff));
	test1.set("pippo", Value::create_string("pippo"));
	test1.set("pluto", Value::create_string("12"));
	test1.set("cippo", Value::create_string(""));

	ValueBag test2;
	test2.set("antani", Value::create_integer(-1));
	test2.set("blinda", Value::create_integer(0));
	test2.set("supercazzola", Value::create_integer(-1234567));

	Item<> val(area::GRIB::create(test1));
	Item<> val1(area::GRIB::create(test2));
	ensure_stores(types::Area, md, val, val1);
}

// Test proddefs
def_test(10)
{
	ValueBag test1;
	test1.set("foo", Value::create_integer(5));
	test1.set("bar", Value::create_integer(5000));
	test1.set("baz", Value::create_integer(-200));
	test1.set("moo", Value::create_integer(0x5ffffff));
	test1.set("pippo", Value::create_string("pippo"));
	test1.set("pluto", Value::create_string("12"));
	test1.set("cippo", Value::create_string(""));

	ValueBag test2;
	test2.set("antani", Value::create_integer(-1));
	test2.set("blinda", Value::create_integer(0));
	test2.set("supercazzola", Value::create_integer(-1234567));

	Item<> val(proddef::GRIB::create(test1));
	Item<> val1(proddef::GRIB::create(test2));
	ensure_stores(types::Proddef, md, val, val1);
}

// Test dataset info
def_test(11)
{
	Item<> val(AssignedDataset::create("test", "abcdefg"));
	Item<> val1(AssignedDataset::create("test1", "abcdefgh"));
	ensure_stores(types::AssignedDataset, md, val, val1);
}

// Check compareMaps
def_test(12)
{
	using namespace utils;
	map<string, UItem<> > a;
	map<string, UItem<> > b;

	a["antani"] = b["antani"] = types::origin::GRIB1::create(1, 2, 3);
	a["pippo"] = types::run::Minute::create(12);

	ensure_equals(compareMaps(a, b), 1);
	ensure_equals(compareMaps(b, a), -1);
}
#endif

}

}
