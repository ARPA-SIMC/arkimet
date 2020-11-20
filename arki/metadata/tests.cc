#include "arki/metadata/tests.h"
#include "arki/metadata/data.h"
#include "arki/matcher.h"
#include "arki/matcher/parser.h"
#include "arki/types/origin.h"
#include "arki/types/product.h"
#include "arki/types/level.h"
#include "arki/types/timerange.h"
#include "arki/types/reftime.h"
#include "arki/types/area.h"
#include "arki/types/proddef.h"
#include "arki/types/assigneddataset.h"
#include "arki/types/run.h"
#include "arki/types/task.h"
#include "arki/types/quantity.h"

using namespace std;
using namespace arki;
using namespace arki::types;
using arki::core::Time;

namespace arki {
namespace tests {

TestData::TestData(const std::string& format)
    : format(format)
{
}

GRIBData::GRIBData()
    : TestData("grib")
{
    skip_unless_grib();
    mds.scan_from_file("inbound/fixture.grib1", format, true);
}

BUFRData::BUFRData()
    : TestData("bufr")
{
    skip_unless_bufr();
    mds.scan_from_file("inbound/fixture.bufr", format, true);
}

VM2Data::VM2Data()
    : TestData("vm2")
{
    skip_unless_vm2();
    mds.scan_from_file("inbound/fixture.vm2", format, true);
}

ODIMData::ODIMData()
    : TestData("odimh5")
{
    mds.scan_from_file("inbound/fixture.odimh5/00.odimh5", format, true);
    mds.scan_from_file("inbound/fixture.odimh5/01.odimh5", format, true);
    mds.scan_from_file("inbound/fixture.odimh5/02.odimh5", format, true);
}

NCData::NCData()
    : TestData("nc")
{
    mds.scan_from_file("inbound/fixture.nc/00.nc", format, true);
    mds.scan_from_file("inbound/fixture.nc/01.nc", format, true);
    mds.scan_from_file("inbound/fixture.nc/02.nc", format, true);
}


Metadata make_large_mock(const std::string& format, size_t size, unsigned month, unsigned day, unsigned hour)
{
    Metadata md;
    md.set_source_inline(format, metadata::DataManager::get().to_data(format, vector<uint8_t>(size)));
    md.set("origin", "GRIB1(200, 10, 100)");
    md.set("product", "GRIB1(3, 4, 5)");
    md.set("level", "GRIB1(1, 2)");
    md.set("timerange", "GRIB1(4, 5s, 6s)");
    md.set(Reftime::createPosition(Time(2014, month, day, hour, 0, 0)));
    md.set("area", "GRIB(foo=5,bar=5000)");
    md.set("proddef", "GRIB(foo=5,bar=5000)");
    md.add_note("this is a test");
    return md;
}


void fill(Metadata& md)
{
    using namespace arki::types::values;
    ValueBag testValues;
    testValues.set("aaa", Value::create_integer(0));
    testValues.set("foo", Value::create_integer(5));
    testValues.set("bar", Value::create_integer(5000));
    testValues.set("baz", Value::create_integer(-200));
    testValues.set("moo", Value::create_integer(0x5ffffff));
    testValues.set("antani", Value::create_integer(-1));
    testValues.set("blinda", Value::create_integer(0));
    testValues.set("supercazzola", Value::create_integer(-1234567));
    testValues.set("pippo", Value::create_string("pippo"));
    testValues.set("pluto", Value::create_string("12"));
    testValues.set("zzz", Value::create_integer(1));

    md.set(Origin::createGRIB1(1, 2, 3));
    md.set(Product::createGRIB1(1, 2, 3));
    md.set(Level::createGRIB1(110, 12, 13));
    md.set(Timerange::createGRIB1(2, 254u, 22, 23));
    md.set(Area::createGRIB(testValues));
    md.set(Proddef::createGRIB(testValues));
    md.add_note("test note");
    md.set(AssignedDataset::create("dsname", "dsid"));
    md.set(Run::createMinute(12));
    md.set(Reftime::createPosition(Time(2007, 1, 2, 3, 4, 5)));

    /* metadati specifici di odimh5 */
    md.set(Task::create("task1"));
    md.set(Quantity::create("a,b,c"));
}

void ActualMetadata::operator==(const Metadata& expected) const
{
    if (_actual == expected) return;
    std::stringstream ss;
    ss << "value:" << endl;
    ss << _actual.to_yaml();
    ss << "is different than the expected:" << endl;
    ss << expected.to_yaml();
    throw TestFailed(ss.str());
}

void ActualMetadata::operator!=(const Metadata& expected) const
{
    if (_actual != expected) return;
    std::stringstream ss;
    ss << "value:" << endl;
    ss << _actual.to_yaml();
    ss << "is not different than the expected:" << endl;
    ss << expected.to_yaml();
    throw TestFailed(ss.str());
}

void ActualMetadata::contains(const std::string& field, const std::string& expected)
{
    Code code = types::parseCodeName(field);
    const Type* actual_field = _actual.get(code);

    if (!actual_field)
    {
        std::stringstream ss;
        ss << "metadata does not contain a " << field << ", but we expected: \"" << expected << "\"";
        throw TestFailed(ss.str());
    }

    unique_ptr<Type> expected_field = decodeString(code, expected);
    if (Type::nullable_equals(actual_field, expected_field.get())) return;
    std::stringstream ss;
    ss << field << " '" << *actual_field << "' is different than the expected '" << *expected_field << "'";
    throw TestFailed(ss.str());
}

void ActualMetadata::not_contains(const std::string& field, const std::string& expected)
{
    Code code = types::parseCodeName(field);
    const Type* actual_field = _actual.get(code);

    if (!actual_field)
        return;

    unique_ptr<Type> expected_field = decodeString(code, expected);
    if (!Type::nullable_equals(actual_field, expected_field.get())) return;
    std::stringstream ss;
    ss << field << " '" << *actual_field << "' is not different than the expected '" << *expected_field << "'";
    throw TestFailed(ss.str());
}

void ActualMetadata::is_similar(const Metadata& expected)
{
    for (Metadata::const_iterator i = expected.begin(); i != expected.end(); ++i)
    {
        if (i->first == TYPE_ASSIGNEDDATASET) continue;

        const Type* other = _actual.get(i->first);
        if (!other)
        {
            std::stringstream ss;
            ss << "missing metadata item " << types::formatCode(i->first) << ": " << *(i->second);
            throw TestFailed(ss.str());
        }

        if ((*i->second) != *other)
        {
            std::stringstream ss;
            ss << i->first << " differ: " << types::formatCode(i->first) << ": expected " << *(i->second) << " got " << *other;
            throw TestFailed(ss.str());
        }
    }

    for (Metadata::const_iterator i = _actual.begin(); i != _actual.end(); ++i)
    {
        if (i->first == TYPE_ASSIGNEDDATASET) continue;

        if (!expected.has(i->first))
        {
            std::stringstream ss;
            ss << "unexpected metadata item " << i->first << ": " << (*i->second);
            throw TestFailed(ss.str());
        }
    }
}

void ActualMetadata::is_set(const std::string& field)
{
    types::Code code = types::parseCodeName(field.c_str());
    const Type* item = _actual.get(code);

    if (item) return;
    std::stringstream ss;
    ss << "metadata should contain a " << field << " field, but it does not";
    throw TestFailed(ss.str());
}

void ActualMetadata::is_not_set(const std::string& field)
{
    types::Code code = types::parseCodeName(field.c_str());
    const Type* item = _actual.get(code);

    if (!item) return;
    std::stringstream ss;
    ss << "metadata should not contain a " << field << " field, but it contains \"" << *item << "\"";
    throw TestFailed(ss.str());
}

void ActualMetadata::matches(const std::string& expr)
{
    matcher::Parser parser;
    Matcher m = parser.parse(expr);

    // Check that it should match as expected
    wassert(actual(m(_actual)));
}

void ActualMetadata::not_matches(const std::string& expr)
{
    matcher::Parser parser;
    Matcher m = parser.parse(expr);

    wassert(actual(not m(_actual)));
}

}
}
