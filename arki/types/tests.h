/**
 * Copyright (C) 2007--2013  ARPA-SIM <urpsim@smr.arpa.emr.it>
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
#ifndef ARKI_TYPES_TESTUTILS_H
#define ARKI_TYPES_TESTUTILS_H

#include <arki/tests/tests.h>
#include <arki/types.h>
#include <wibble/string.h>
#include <arki/emitter/json.h>
#include <arki/emitter/memory.h>

using namespace arki;

namespace arki {
namespace tests {

struct TestItemSerializes
{
    Item<> act;
    types::Code code;

    TestItemSerializes(const Item<>& actual, types::Code code) : act(actual), code(code) {}

    void check(WIBBLE_TEST_LOCPRM) const;
};

struct TestItemCompares
{
    Item<> act;
    Item<> higher1;
    Item<> higher2;

    TestItemCompares(const Item<>& actual, const Item<>& higher1, const Item<>& higher2)
        : act(actual), higher1(higher1), higher2(higher2) {}

    void check(WIBBLE_TEST_LOCPRM) const;
};

struct TestSourceblobIs
{
    Item<> act;
    std::string format;
    std::string basedir;
    std::string fname;
    uint64_t ofs;
    uint64_t size;

    TestSourceblobIs(
            const Item<>& actual,
            const std::string& format,
            const std::string& basedir,
            const std::string& fname,
            uint64_t ofs,
            uint64_t size)
        : act(actual), format(format), basedir(basedir), fname(fname), ofs(ofs), size(size) {}

    void check(WIBBLE_TEST_LOCPRM) const;
};

struct TestGenericType
{
    types::Code code;
    std::string sample;
    std::vector<std::string> lower;
    std::vector<std::string> higher;

    TestGenericType(types::Code code, const std::string& sample) : code(code), sample(sample) {}

    void check(WIBBLE_TEST_LOCPRM) const;
};

template<typename T>
struct ActualItem : public wibble::tests::Actual< arki::UItem<T> >
{
    ActualItem<T>(const arki::UItem<T>& actual) : wibble::tests::Actual< arki::UItem<T> >(actual) {}

    /**
     * Check that a metadata field can be serialized and deserialized in all
     * sorts of ways
     */
    TestItemSerializes serializes()
    {
        return TestItemSerializes(this->actual, this->actual->serialisationCode());
    }

    /**
     * Check comparison operators
     */
    TestItemCompares compares(const arki::Item<T>& higher1, const arki::Item<T>& higher2)
    {
        return TestItemCompares(this->actual, higher1, higher2);
    }

    /**
     * Check all components of a source::Blob item
     */
    TestSourceblobIs sourceblob_is(
        const std::string& format,
        const std::string& basedir,
        const std::string& fname,
        uint64_t ofs,
        uint64_t size)
    {
        return TestSourceblobIs(this->actual, format, basedir, fname, ofs, size);
    }
};

}
}

namespace wibble {
namespace tests {

template<typename T>
inline arki::tests::ActualItem<T> actual(const arki::UItem<T>& actual) { return arki::tests::ActualItem<T>(actual); }
template<typename T>
inline arki::tests::ActualItem<T> actual(const arki::Item<T>& actual) { return arki::tests::ActualItem<T>(actual); }

}
}

#endif
