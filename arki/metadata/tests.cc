/**
 * Copyright (C) 2012--2013  ARPA-SIM <urpsim@smr.arpa.emr.it>
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

#include "config.h"

#include <arki/metadata/tests.h>
#include <sstream>

using namespace std;
using namespace arki;
using namespace wibble;

namespace wibble {
namespace tests {

void test_assert_md_similar(WIBBLE_TEST_LOCPRM, const Metadata& expected, const Metadata& actual)
{
    for (Metadata::const_iterator i = expected.begin(); i != expected.end(); ++i)
    {
        if (i->first == types::TYPE_ASSIGNEDDATASET) continue;

        UItem<> other = actual.get(i->first);
        if (!other.defined())
        {
            std::stringstream ss;
            ss << "missing metadata item " << i->first << ": " << i->second;
            wibble_test_location.fail_test(ss.str());
        }

        if (i->second != other)
        {
            std::stringstream ss;
            ss << i->first << " differ: " << i->first << ": expected " << i->second << " got " << other;
            wibble_test_location.fail_test(ss.str());
        }
    }

    for (Metadata::const_iterator i = actual.begin(); i != actual.end(); ++i)
    {
        if (i->first == types::TYPE_ASSIGNEDDATASET) continue;

        if (!expected.has(i->first))
        {
            std::stringstream ss;
            ss << "unexpected metadata item " << i->first << ": " << i->second;
            wibble_test_location.fail_test(ss.str());
        }
    }
}

void test_assert_md_contains(WIBBLE_TEST_LOCPRM, const std::string& type, const std::string& expected_val, const Metadata& actual)
{
    types::Code code = types::parseCodeName(type.c_str());
    UItem<> item = actual.get(code);
    if (!item.defined())
    {
        std::stringstream ss;
        ss << "metadata does not contain an item of type " << type << ": expected: \"" << expected_val << "\"";
        wibble_test_location.fail_test(ss.str());
    }

    UItem<> item1 = types::decodeString(code, expected_val);
    if (item != item1)
    {
        std::stringstream ss;
        ss << "metadata mismatch on " << type << ": expected: \"" << expected_val << "\" actual: \"" << item << "\"";
        wibble_test_location.fail_test(ss.str());
    }
}

void test_assert_md_unset(WIBBLE_TEST_LOCPRM, const std::string& type, const Metadata& actual)
{
    types::Code code = types::parseCodeName(type.c_str());
    UItem<> item = actual.get(code);
    if (item.defined())
    {
        std::stringstream ss;
        ss << "metadata should not contain an item of type " << type << ", but it contains \"" << item << "\"";
        wibble_test_location.fail_test(ss.str());
    }
}

}
}
// vim:set ts=4 sw=4:
