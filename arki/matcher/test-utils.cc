#include <arki/matcher/test-utils.h>
#include <arki/matcher.h>
#include <arki/metadata.h>
#include <arki/types/origin.h>
#include <arki/types/product.h>
#include <arki/types/level.h>
#include <arki/types/timerange.h>
#include <arki/types/reftime.h>
#include <arki/types/area.h>
#include <arki/types/ensemble.h>
#include <arki/types/assigneddataset.h>
#include <arki/types/run.h>

using namespace std;
using namespace arki;
using namespace arki::types;

namespace arki {
namespace tests {

void fill(Metadata& md)
{
	ValueBag testValues;
	testValues.set("aaa", Value::createInteger(0));
	testValues.set("foo", Value::createInteger(5));
	testValues.set("bar", Value::createInteger(5000));
	testValues.set("baz", Value::createInteger(-200));
	testValues.set("moo", Value::createInteger(0x5ffffff));
	testValues.set("antani", Value::createInteger(-1));
	testValues.set("blinda", Value::createInteger(0));
	testValues.set("supercazzola", Value::createInteger(-1234567));
	testValues.set("pippo", Value::createString("pippo"));
	testValues.set("pluto", Value::createString("12"));
	testValues.set("zzz", Value::createInteger(1));

	md.set(origin::GRIB1::create(1, 2, 3));
	md.set(product::GRIB1::create(1, 2, 3));
	md.set(level::GRIB1::create(110, 12, 13));
	md.set(timerange::GRIB1::create(2, 254u, 22, 23));
	md.set(area::GRIB::create(testValues));
	md.set(ensemble::GRIB::create(testValues));
	md.notes.push_back(types::Note::create("test note"));
	md.set(AssignedDataset::create("dsname", "dsid"));
	md.set(run::Minute::create(12));
	md.set(reftime::Position::create(types::Time::create(2007, 1, 2, 3, 4, 5)));
}

void impl_ensure_matches(const wibble::tests::Location& loc, const std::string& expr, const Metadata& md, bool shouldMatch)
{
	Matcher m = Matcher::parse(expr);

	// Check that it should match as expected
	if (shouldMatch)
		inner_ensure(m(md));
	else
		inner_ensure(not m(md));

	// Check stringification and reparsing
	Matcher m1 = Matcher::parse(m.toString());

	//fprintf(stderr, "%s -> %s -> %s\n", expr.c_str(), m.toString().c_str(), m1.toString().c_str());

	inner_ensure_equals(m1.toString(), m.toString());
	inner_ensure_equals(m1(md), m(md));
}

}
}
// vim:set ts=4 sw=4:
