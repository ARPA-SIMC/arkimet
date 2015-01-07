/**
 * Copyright (C) 2012--2015  ARPA-SIM <urpsim@smr.arpa.emr.it>
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

#include <arki/metadata/tests.h>
#include <sstream>

using namespace std;
using namespace arki;
using namespace arki::types;
using namespace wibble;

namespace arki {
namespace tests {

struct TestMetadataContains : public ArkiCheck
{
    const Metadata& actual;
    const std::string& field;
    const std::string& expected;
    bool inverted;
    TestMetadataContains(const Metadata& actual, const std::string& field, const std::string& expected, bool inverted=false)
        : actual(actual), field(field), expected(expected), inverted(inverted) {}

    void check(WIBBLE_TEST_LOCPRM) const override
    {
        Code code = types::parseCodeName(field);
        const Type* actual_field = actual.get(code);

        if (!actual_field)
        {
            if (inverted) return;
            std::stringstream ss;
            ss << "metadata does not contain a " << field << ", but we expected: \"" << expected << "\"";
            wibble_test_location.fail_test(ss.str());
        }

        auto_ptr<Type> expected_field = decodeString(code, expected);
        if (!inverted)
        {
            if (Type::nullable_equals(actual_field, expected_field.get())) return;
            std::stringstream ss;
            ss << field << " '" << *actual_field << "' is different than the expected '" << *expected_field << "'";
            wibble_test_location.fail_test(ss.str());
        } else {
            if (!Type::nullable_equals(actual_field, expected_field.get())) return;
            std::stringstream ss;
            ss << field << " '" << *actual_field << "' is not different than the expected '" << *expected_field << "'";
            wibble_test_location.fail_test(ss.str());
        }
    }
};

auto_ptr<ArkiCheck> ActualMetadata::contains(const std::string& field, const std::string& expected)
{
    return auto_ptr<ArkiCheck>(new TestMetadataContains(actual, field, expected));
}


struct TestMetadataSimilar : public ArkiCheck
{
    const Metadata& actual;
    const Metadata& expected;
    bool inverted;
    TestMetadataSimilar(const Metadata& actual, const Metadata& expected, bool inverted=false)
        : actual(actual), expected(expected), inverted(inverted) {}

    void check(WIBBLE_TEST_LOCPRM) const override
    {
        for (Metadata::const_iterator i = expected.begin(); i != expected.end(); ++i)
        {
            if (i->first == types::TYPE_ASSIGNEDDATASET) continue;

            const Type* other = actual.get(i->first);
            if (!other)
            {
                std::stringstream ss;
                ss << "missing metadata item " << i->first << ": " << i->second;
                wibble_test_location.fail_test(ss.str());
            }

            if ((*i->second) != *other)
            {
                std::stringstream ss;
                ss << i->first << " differ: " << i->first << ": expected " << *(i->second) << " got " << *other;
                wibble_test_location.fail_test(ss.str());
            }
        }

        for (Metadata::const_iterator i = actual.begin(); i != actual.end(); ++i)
        {
            if (i->first == types::TYPE_ASSIGNEDDATASET) continue;

            if (!expected.has(i->first))
            {
                std::stringstream ss;
                ss << "unexpected metadata item " << i->first << ": " << (*i->second);
                wibble_test_location.fail_test(ss.str());
            }
        }
    }
};

auto_ptr<ArkiCheck> ActualMetadata::is_similar(const Metadata& expected)
{
    return auto_ptr<ArkiCheck>(new TestMetadataSimilar(actual, expected));
}

struct TestMetadataIsset : public ArkiCheck
{
    const Metadata& actual;
    const std::string& field;
    bool inverted;
    TestMetadataIsset(const Metadata& actual, const std::string& field, bool inverted=false)
        : actual(actual), field(field), inverted(inverted) {}

    TestMetadataIsset operator!() { return TestMetadataIsset(actual, field, !inverted); }
    void check(WIBBLE_TEST_LOCPRM) const override
    {
        types::Code code = types::parseCodeName(field.c_str());
        const Type* item = actual.get(code);

        if (!inverted)
        {
            if (item) return;
            std::stringstream ss;
            ss << "metadata should contain a " << field << " field, but it does not";
            wibble_test_location.fail_test(ss.str());
        } else {
            if (!item) return;
            std::stringstream ss;
            ss << "metadata should not contain a " << field << " field, but it contains \"" << *item << "\"";
            wibble_test_location.fail_test(ss.str());
        }
    }
};

auto_ptr<ArkiCheck> ActualMetadata::is_set(const std::string& field)
{
    return auto_ptr<ArkiCheck>(new TestMetadataIsset(actual, field));
}

}
}
