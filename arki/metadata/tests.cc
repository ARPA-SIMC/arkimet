#include <arki/metadata/tests.h>
#include <arki/matcher.h>
#include <arki/types/origin.h>
#include <arki/types/product.h>
#include <arki/types/level.h>
#include <arki/types/timerange.h>
#include <arki/types/reftime.h>
#include <arki/types/area.h>
#include <arki/types/proddef.h>
#include <arki/types/assigneddataset.h>
#include <arki/types/run.h>
#include <arki/types/task.h>
#include <arki/types/quantity.h>
#include <sstream>

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
            ss << "missing metadata item " << i->first << ": " << i->second;
            throw TestFailed(ss.str());
        }

        if ((*i->second) != *other)
        {
            std::stringstream ss;
            ss << i->first << " differ: " << i->first << ": expected " << *(i->second) << " got " << *other;
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
    Matcher m = Matcher::parse(expr);

    // Check that it should match as expected
    wassert(actual(m(_actual)));
}

void ActualMetadata::not_matches(const std::string& expr)
{
    Matcher m = Matcher::parse(expr);

    wassert(actual(not m(_actual)));
}

}
}
