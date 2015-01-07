/**
 * Copyright (C) 2013  ARPA-SIM <urpsim@smr.arpa.emr.it>
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

#include "tests.h"
#include "source.h"
#include "source/blob.h"
#include "source/inline.h"
#include "source/url.h"
#include "time.h"
#include "reftime.h"
#include <arki/types.h>
#include <arki/matcher.h>
#include <arki/utils/codec.h>
#include <arki/emitter/json.h>
#include <arki/emitter/memory.h>
#include <wibble/string.h>
#include <wibble/sys/fs.h>
#include <cxxabi.h>
#include <sstream>

using namespace std;
using namespace arki;
using namespace arki::types;
using namespace wibble;
using namespace wibble::tests;

namespace arki {
namespace tests {

TestGenericType::TestGenericType(const std::string& tag, const std::string& sample) : tag(tag), sample(sample) {}

void TestGenericType::check_item(WIBBLE_TEST_LOCPRM, const std::string& encoded, std::auto_ptr<types::Type>& item) const
{
    WIBBLE_TEST_INFO(tinfo);
    tinfo() << "current: " << encoded;

    Code code = parseCodeName(tag);

    // Decode
    wrunchecked(item = types::decodeString(code, encoded));

    // Test equality to another decoded self
    wassert(actual(item) == encoded);

    // Test serialization
    wassert(actual(item->type_code()) == code);
    wassert(actual(item).serializes());

    // Test equality to a clone
    auto_ptr<Type> clone(item->cloneType());
    wassert(actual(item) == clone);
}

void TestGenericType::check(WIBBLE_TEST_LOCPRM) const
{
    WIBBLE_TEST_INFO(tinfo);

    tinfo() << "current: " << sample;

    // Decode and run all single-item tests
    auto_ptr<Type> item;
    wruntest(check_item, sample, item);

    for (vector<string>::const_iterator i = alternates.begin();
            i != alternates.end(); ++i)
    {
        tinfo() << "current: " << *i << " == " << sample;
        auto_ptr<Type> aitem;
        wruntest(check_item, *i, aitem);
        wassert(actual(item) == aitem);
    }

    // Test equality and comparisons
    for (std::vector<std::string>::const_iterator i = lower.begin();
            i != lower.end(); ++i)
    {
        tinfo() << "current (lo): " << *i << " < " << sample;

        // Decode and run all single-item tests
        auto_ptr<Type> lower_item;
        wruntest(check_item, *i, lower_item);

        // Check equality with different items
        wassert(actual(item) != lower_item);

        // Check various comparisons
        wassert(actual(lower_item).compares(*item));
    }

    for (std::vector<std::string>::const_iterator i = higher.begin();
            i != higher.end(); ++i)
    {
        tinfo() << "current (hi): " << sample << " < " << *i;

        // Decode and run all single-item tests
        auto_ptr<Type> higher_item;
        wruntest(check_item, *i, higher_item);

        // Check equality with different items
        wassert(actual(item) != higher_item);

        // Check various comparisons
        wassert(actual(item).compares(*higher_item));
    }

    if (!exact_query.empty())
    {
        wassert(actual(item->exactQuery()) == exact_query);

        Matcher m = Matcher::parse(item->tag() + ":" + item->exactQuery());
        wassert(actual(m(*item)).istrue());
    }
}


struct TestItemSerializes : public ArkiCheck
{
    Type* act;
    Code code;

    TestItemSerializes(const Type* actual, Code code) : act(actual ? actual->clone() : 0), code(code) {}
    ~TestItemSerializes() { delete act; }

    void check(WIBBLE_TEST_LOCPRM) const override
    {
        // Binary encoding, without envelope
        std::string enc;
        utils::codec::Encoder e(enc);
        act->encodeWithoutEnvelope(e);
        size_t inner_enc_size = enc.size();
        wassert(actual(decodeInner(code, (const unsigned char*)enc.data(), enc.size())) == act);

        // Binary encoding, with envelope
        enc = act->encodeBinary();
        // Rewritten in the next two lines due to, it seems, a bug in old gccs
        // inner_ensure_equals(types::decode((const unsigned char*)enc.data(), enc.size()).upcast<T>(), act);
        auto_ptr<Type> decoded = types::decode((const unsigned char*)enc.data(), enc.size());
        wassert(actual(decoded) == act);

        const unsigned char* buf = (const unsigned char*)enc.data();
        size_t len = enc.size();
        wassert(actual(decodeEnvelope(buf, len)) == code);
        wassert(actual(len) == inner_enc_size);
        wassert(actual(decodeInner(code, buf, len)) == act);

        // String encoding
        wassert(actual(types::decodeString(code, wibble::str::fmt(*act))) == *act);

        // JSON encoding
        {
            std::stringstream jbuf;
            emitter::JSON json(jbuf);
            act->serialise(json);
            jbuf.seekg(0);
            emitter::Memory parsed;
            emitter::JSON::parse(jbuf, parsed);
            wassert(actual(parsed.root().is_mapping()).istrue());
            auto_ptr<Type> iparsed = types::decodeMapping(parsed.root().get_mapping());
            wassert(actual(iparsed) == act);
        }
    }

private:
    TestItemSerializes(const TestItemSerializes&);
    TestItemSerializes& operator=(const TestItemSerializes&);
};

struct TestItemCompares : public ArkiCheck
{
    types::Type* act;
    types::Type* higher1;
    types::Type* higher2;

    TestItemCompares(const types::Type* actual, const types::Type& higher)
        : act(actual ? actual->clone() : 0), higher1(higher.clone()), higher2(higher.clone()) {}
    ~TestItemCompares() { delete act; delete higher1; delete higher2; }

    void check(WIBBLE_TEST_LOCPRM) const override
    {
        if (!act) wibble_test_location.fail_test("actual item to compare is undefined");

        wassert(actual(*act == *act).istrue());
        wassert(actual(*higher1 == *higher1).istrue());
        wassert(actual(*higher2 == *higher2).istrue());

        wassert(actual(*act < *higher1).istrue());
        wassert(actual(*act <= *higher1).istrue());
        wassert(actual(*act == *higher1).isfalse());
        wassert(actual(*act != *higher1).istrue());
        wassert(actual(*act >= *higher1).isfalse());
        wassert(actual(*act > *higher1).isfalse());

        wassert(actual(*higher1 < *higher2).isfalse());
        wassert(actual(*higher1 <= *higher2).istrue());
        wassert(actual(*higher1 == *higher2).istrue());
        wassert(actual(*higher1 != *higher2).isfalse());
        wassert(actual(*higher1 >= *higher2).istrue());
        wassert(actual(*higher1 >  *higher2).isfalse());
    }

private:
    TestItemCompares(const TestItemCompares&);
    TestItemCompares& operator=(const TestItemCompares&);
};

struct TestGenericTypeEquals : public ArkiCheck
{
    types::Type* a;
    types::Type* b;
    bool inverted;

    TestGenericTypeEquals(const types::Type* a, const types::Type* b, bool inverted=false)
        : a(a ? a->clone() : 0), b(b ? b->clone() : 0), inverted(inverted) {}
    TestGenericTypeEquals(const types::Type* a, const types::Type& b, bool inverted=false)
        : a(a ? a->clone() : 0), b(b.clone()), inverted(inverted) {}
    template<typename T>
    TestGenericTypeEquals(const types::Type* a, const std::auto_ptr<T>& b, bool inverted=false)
        : a(a ? a->clone() : 0), b(b.get() ? b->clone() : 0), inverted(inverted) {}
    ~TestGenericTypeEquals() { delete a; delete b; }

    void check(WIBBLE_TEST_LOCPRM) const override
    {
        bool res = Type::nullable_equals(a, b);

        if (inverted)
        {
            if (!res) return;
            std::stringstream ss;
            ss << "item ";
            if (a)
                ss << "'" << *a << "'";
            else
                ss << "(null)";
            ss << " is the same as item ";
            if (b)
                ss << "'" << *b << "'";
            else
                ss << "(null)";
            ss << " when it should not be";
            wibble_test_location.fail_test(ss.str());
        } else {
            if (res) return;
            std::stringstream ss;
            ss << "item ";
            if (a)
                ss << "'" << *a << "'";
            else
                ss << "(null)";
            ss << " is not the same as item ";
            if (b)
                ss << "'" << *b << "'";
            else
                ss << "(null)";
            ss << " when it should be";
            wibble_test_location.fail_test(ss.str());
        }
    }

private:
    TestGenericTypeEquals(const TestGenericTypeEquals&);
    TestGenericTypeEquals& operator=(const TestGenericTypeEquals&);
};

template<typename T>
struct TestSpecificTypeBase : public ArkiCheck
{
    types::Type* act;
    const T* item;

    TestSpecificTypeBase(const types::Type* actual)
        : act(actual ? actual->clone() : 0), item(dynamic_cast<const T*>(actual)) {}
    ~TestSpecificTypeBase() { delete act; }

    void check(WIBBLE_TEST_LOCPRM) const override
    {
        if (!act) wibble_test_location.fail_test("item to check is undefined");

        if (!item)
        {
            std::stringstream ss;
            ss << "metadata item '" << *act << "' is not a ";
            int status;
            char* name = abi::__cxa_demangle(typeid(T).name(), 0, 0, &status);
            if (status == 0)
                ss << name;
            else
                ss << "(demangling failed: " << typeid(T).name() << ")";
            free(name);
            wibble_test_location.fail_test(ss.str());
        }
    }

private:
    TestSpecificTypeBase(const TestSpecificTypeBase&);
    TestSpecificTypeBase& operator=(const TestSpecificTypeBase&);
};


auto_ptr<ArkiCheck> ActualType::operator==(const Type* expected) const
{
    return auto_ptr<ArkiCheck>(new TestGenericTypeEquals(actual, expected));
}

auto_ptr<ArkiCheck> ActualType::operator!=(const Type* expected) const
{
    return auto_ptr<ArkiCheck>(new TestGenericTypeEquals(actual, expected, true));
}

auto_ptr<ArkiCheck> ActualType::operator==(const std::string& expected) const
{
    return operator==(types::decodeString(actual->type_code(), expected));
}

auto_ptr<ArkiCheck> ActualType::operator!=(const std::string& expected) const
{
    return operator!=(types::decodeString(actual->type_code(), expected));
}

auto_ptr<ArkiCheck> ActualType::serializes() const
{
    return auto_ptr<ArkiCheck>(new TestItemSerializes(this->actual, this->actual->type_code()));
}

auto_ptr<ArkiCheck> ActualType::compares(const types::Type& higher) const
{
    return auto_ptr<ArkiCheck>(new TestItemCompares(this->actual, higher));
}

struct TestSourceBlobIs : public TestSpecificTypeBase<source::Blob>
{
    std::string format;
    std::string basedir;
    std::string fname;
    uint64_t ofs;
    uint64_t size;

    TestSourceBlobIs(
            const types::Type* actual,
            const std::string& format,
            const std::string& basedir,
            const std::string& fname,
            uint64_t ofs,
            uint64_t size)
        : TestSpecificTypeBase(actual), format(format), basedir(basedir), fname(fname), ofs(ofs), size(size) {}

    void check(WIBBLE_TEST_LOCPRM) const override
    {
        wruntest(TestSpecificTypeBase::check);
        wassert(actual(item->format) == format);
        wassert(actual(item->basedir) == basedir);
        wassert(actual(item->filename) == fname);
        wassert(actual(item->offset) == ofs);
        wassert(actual(item->size) == size);
        if (!basedir.empty())
            wassert(actual(item->absolutePathname()) == sys::fs::abspath(str::joinpath(basedir, fname)));
    }
};

std::auto_ptr<ArkiCheck> ActualType::is_source_blob(
    const std::string& format,
    const std::string& basedir,
    const std::string& fname,
    uint64_t ofs,
    uint64_t size)
{
    return auto_ptr<ArkiCheck>(new TestSourceBlobIs(this->actual, format, basedir, fname, ofs, size));
}

struct TestSourceURLIs : public TestSpecificTypeBase<source::URL>
{
    std::string format;
    std::string url;

    TestSourceURLIs(const types::Type* actual, const std::string& format, const std::string& url)
        : TestSpecificTypeBase(actual), format(format), url(url) {}

    void check(WIBBLE_TEST_LOCPRM) const override
    {
        wruntest(TestSpecificTypeBase::check);
        wassert(actual(item->format) == format);
        wassert(actual(item->url) == url);
    }
};

std::auto_ptr<ArkiCheck> ActualType::is_source_url(const std::string& format, const std::string& url)
{
    return auto_ptr<ArkiCheck>(new TestSourceURLIs(this->actual, format, url));
}

struct TestSourceInlineIs : public TestSpecificTypeBase<source::Inline>
{
    std::string format;
    uint64_t size;

    TestSourceInlineIs(const types::Type* actual, const std::string& format, uint64_t size)
        : TestSpecificTypeBase(actual), format(format), size(size) {}

    void check(WIBBLE_TEST_LOCPRM) const override
    {
        wruntest(TestSpecificTypeBase::check);
        wassert(actual(item->format) == format);
        wassert(actual(item->size) == size);
    }
};

std::auto_ptr<ArkiCheck> ActualType::is_source_inline(const std::string& format, uint64_t size)
{
    return auto_ptr<ArkiCheck>(new TestSourceInlineIs(this->actual, format, size));
}

struct TestTimeIs : public TestSpecificTypeBase<Time>
{
    Time other;

    TestTimeIs(const types::Type* actual, const Time& other)
        : TestSpecificTypeBase(actual), other(other) {}

    void check(WIBBLE_TEST_LOCPRM) const override
    {
        wruntest(TestSpecificTypeBase::check);
        wassert(actual(item->vals[0]) == other.vals[0]);
        wassert(actual(item->vals[1]) == other.vals[1]);
        wassert(actual(item->vals[2]) == other.vals[2]);
        wassert(actual(item->vals[3]) == other.vals[3]);
        wassert(actual(item->vals[4]) == other.vals[4]);
        wassert(actual(item->vals[5]) == other.vals[5]);
    }
};

std::auto_ptr<ArkiCheck> ActualType::is_time(int ye, int mo, int da, int ho, int mi, int se)
{
    return auto_ptr<ArkiCheck>(new TestTimeIs(this->actual, Time(ye, mo, da, ho, mi, se)));
}

struct TestReftimePositionIs : public TestSpecificTypeBase<reftime::Position>
{
    Time time;

    TestReftimePositionIs(const types::Type* actual, const Time& time)
        : TestSpecificTypeBase(actual), time(time) {}

    void check(WIBBLE_TEST_LOCPRM) const override
    {
        wruntest(TestSpecificTypeBase::check);
        wassert(actual(item->time) == time);
    }
};

std::auto_ptr<ArkiCheck> ActualType::is_reftime_position(const int (&time)[6])
{
    return auto_ptr<ArkiCheck>(new TestReftimePositionIs(this->actual, Time(time)));
}

struct TestReftimePeriodIs : public TestSpecificTypeBase<reftime::Period>
{
    Time begin;
    Time end;

    TestReftimePeriodIs(const types::Type* actual, const Time& begin, const Time& end)
        : TestSpecificTypeBase(actual), begin(begin), end(end) {}

    void check(WIBBLE_TEST_LOCPRM) const override
    {
        wruntest(TestSpecificTypeBase::check);
        wassert(actual(item->begin) == begin);
        wassert(actual(item->end) == end);
    }
};

std::auto_ptr<ArkiCheck> ActualType::is_reftime_period(const int (&begin)[6], const int (&end)[6])
{
    return auto_ptr<ArkiCheck>(new TestReftimePeriodIs(this->actual, Time(begin), Time(end)));
}


}
}
