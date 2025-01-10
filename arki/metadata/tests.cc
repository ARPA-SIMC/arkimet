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
#include "arki/types/run.h"
#include "arki/types/task.h"
#include "arki/types/quantity.h"
#include "arki/types/values.h"

using namespace std;
using namespace arki;
using namespace arki::types;
using arki::core::Time;

namespace arki {
namespace tests {

TestData::TestData(DataFormat format)
    : format(format)
{
}

GRIBData::GRIBData()
    : TestData(DataFormat::GRIB)
{
    skip_unless_grib();
    mds.scan_from_file("inbound/fixture.grib1", format, true);
}

BUFRData::BUFRData()
    : TestData(DataFormat::BUFR)
{
    skip_unless_bufr();
    mds.scan_from_file("inbound/fixture.bufr", format, true);
}

VM2Data::VM2Data()
    : TestData(DataFormat::VM2)
{
    skip_unless_vm2();
    mds.scan_from_file("inbound/fixture.vm2", format, true);
}

ODIMData::ODIMData()
    : TestData(DataFormat::ODIMH5)
{
    mds.scan_from_file("inbound/fixture.odimh5/00.odimh5", format, true);
    mds.scan_from_file("inbound/fixture.odimh5/01.odimh5", format, true);
    mds.scan_from_file("inbound/fixture.odimh5/02.odimh5", format, true);
}

NCData::NCData()
    : TestData(DataFormat::NETCDF)
{
    mds.scan_from_file("inbound/fixture.nc/00.nc", format, true);
    mds.scan_from_file("inbound/fixture.nc/01.nc", format, true);
    mds.scan_from_file("inbound/fixture.nc/02.nc", format, true);
}

JPEGData::JPEGData()
    : TestData(DataFormat::JPEG)
{
    mds.scan_from_file("inbound/fixture.jpeg/00.jpg", format, true);
    mds.scan_from_file("inbound/fixture.jpeg/01.jpg", format, true);
    mds.scan_from_file("inbound/fixture.jpeg/02.jpg", format, true);
}


std::shared_ptr<Metadata> make_large_mock(DataFormat format, size_t size, unsigned month, unsigned day, unsigned hour)
{
    auto md = std::make_shared<Metadata>();
    md->set_source_inline(format, metadata::DataManager::get().to_data(format, vector<uint8_t>(size)));
    md->test_set("origin", "GRIB1(200, 10, 100)");
    md->test_set("product", "GRIB1(3, 4, 5)");
    md->test_set("level", "GRIB1(1, 2)");
    md->test_set("timerange", "GRIB1(4, 5s, 6s)");
    md->test_set(Reftime::createPosition(Time(2014, month, day, hour, 0, 0)));
    md->test_set("area", "GRIB(foo=5,bar=5000)");
    md->test_set("proddef", "GRIB(foo=5,bar=5000)");
    md->add_note("this is a test");
    return md;
}


void fill(Metadata& md)
{
    using namespace arki::types::values;
    // 100663295 == 0x5ffffff
    ValueBag testValues = ValueBag::parse("aaa=0, foo=5, bar=5000, baz=-200, moo=100663295, antani=-1, blinda=0, supercazzola=-1234567, pippo=pippo, pluto=\"12\", zzz=1");

    md.test_set(Origin::createGRIB1(1, 2, 3));
    md.test_set(Product::createGRIB1(1, 2, 3));
    md.test_set(Level::createGRIB1(110, 12, 13));
    md.test_set(Timerange::createGRIB1(2, 254u, 22, 23));
    md.test_set<area::GRIB>(testValues);
    md.test_set(Proddef::createGRIB(testValues));
    md.add_note("test note");
    md.test_set(Run::createMinute(12));
    md.test_set(Reftime::createPosition(Time(2007, 1, 2, 3, 4, 5)));

    /* metadati specifici di odimh5 */
    md.test_set(Task::create("task1"));
    md.test_set(Quantity::create("a,b,c"));
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
    _actual.diff_items(expected, [](types::Code code, const types::Type* iact, const types::Type* iexp) {
        if (code == TYPE_ASSIGNEDDATASET) return;

        if (!iexp)
        {
            std::stringstream ss;
            ss << "unexpected metadata item " << types::formatCode(code) << ": " << *iact;
            throw TestFailed(ss.str());
        }

        if (!iact)
        {
            std::stringstream ss;
            ss << "missing metadata item " << types::formatCode(code) << ": " << *iexp;
            throw TestFailed(ss.str());
        }

        std::stringstream ss;
        ss << "items differ: " << types::formatCode(code) << ": expected " << *iexp << " got " << *iact;
        throw TestFailed(ss.str());
    });
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
