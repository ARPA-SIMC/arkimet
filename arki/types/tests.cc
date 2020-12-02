#include "tests.h"
#include "source.h"
#include "source/blob.h"
#include "source/inline.h"
#include "source/url.h"
#include "time.h"
#include "reftime.h"
#include "arki/types.h"
#include "arki/matcher.h"
#include "arki/matcher/parser.h"
#include "arki/core/binary.h"
#include "arki/core/file.h"
#include "arki/utils/sys.h"
#include "arki/utils/string.h"
#include "arki/structured/json.h"
#include "arki/structured/memory.h"
#include "arki/structured/keys.h"
#include <cxxabi.h>
#include <sstream>

using namespace std;
using namespace arki;
using namespace arki::types;
using namespace arki::utils;
using namespace arki::tests;
using arki::core::Time;

namespace arki {
namespace tests {

TestGenericType::TestGenericType(const std::string& tag, const std::string& sample) : tag(tag), sample(sample) {}

void TestGenericType::check_item(const std::string& encoded, std::unique_ptr<types::Type>& item) const
{
    ARKI_UTILS_TEST_INFO(tinfo);
    tinfo() << "current: " << encoded;

    Code code = parseCodeName(tag);

    // Decode
    item = wcallchecked(types::decodeString(code, encoded));

    // Test equality to another decoded self
    wassert(actual(item) == encoded);

    // Test serialization
    wassert(actual(item->type_code()) == code);
    wassert(actual(item).serializes());

    // Test equality to a clone
    unique_ptr<Type> clone(item->cloneType());
    wassert(actual(item) == clone);
}

void TestGenericType::check() const
{
    ARKI_UTILS_TEST_INFO(tinfo);

    tinfo() << "current: " << sample;

    // Decode and run all single-item tests
    unique_ptr<Type> item;
    wassert(check_item(sample, item));

    for (vector<string>::const_iterator i = alternates.begin();
            i != alternates.end(); ++i)
    {
        tinfo() << "current: " << *i << " == " << sample;
        unique_ptr<Type> aitem;
        wassert(check_item(*i, aitem));
        wassert(actual(item) == aitem);
    }

    // Test equality and comparisons
    for (std::vector<std::string>::const_iterator i = lower.begin();
            i != lower.end(); ++i)
    {
        tinfo() << "current (lo): " << *i << " < " << sample;

        // Decode and run all single-item tests
        unique_ptr<Type> lower_item;
        wassert(check_item(*i, lower_item));

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
        unique_ptr<Type> higher_item;
        wassert(check_item(*i, higher_item));

        // Check equality with different items
        wassert(actual(item) != higher_item);

        // Check various comparisons
        wassert(actual(item).compares(*higher_item));
    }

    if (!exact_query.empty())
    {
        matcher::Parser parser;
        wassert(actual(item->exactQuery()) == exact_query);

        Matcher m = parser.parse(item->tag() + ":" + item->exactQuery());
        wassert(actual(m(*item)).istrue());
    }
}



void ActualType::operator==(const Type* expected) const
{
    bool res = Type::nullable_equals(_actual, expected);
    if (res) return;
    std::stringstream ss;
    ss << "item ";
    if (_actual)
        ss << "'" << *_actual << "'";
    else
        ss << "(null)";
    ss << " is not the same as item ";
    if (expected)
        ss << "'" << *expected << "'";
    else
        ss << "(null)";
    ss << " when it should be";
    throw TestFailed(ss.str());
}

void ActualType::operator!=(const Type* expected) const
{
    bool res = Type::nullable_equals(_actual, expected);
    if (!res) return;
    std::stringstream ss;
    ss << "item ";
    if (_actual)
        ss << "'" << *_actual << "'";
    else
        ss << "(null)";
    ss << " is the same as item ";
    if (expected)
        ss << "'" << *expected << "'";
    else
        ss << "(null)";
    ss << " when it should not be";
    throw TestFailed(ss.str());
}

void ActualType::operator==(const std::string& expected) const
{
    operator==(types::decodeString(_actual->type_code(), expected));
}

void ActualType::operator!=(const std::string& expected) const
{
    operator!=(types::decodeString(_actual->type_code(), expected));
}

void ActualType::serializes() const
{
    auto code = _actual->type_code();

    // Binary encoding, without envelope
    std::vector<uint8_t> enc;
    core::BinaryEncoder e(enc);
    _actual->encodeWithoutEnvelope(e);
    size_t inner_enc_size = enc.size();
    core::BinaryDecoder dec(enc);
    wassert(actual(decodeInner(code, dec)) == _actual);

    // Binary encoding, with envelope
    enc.clear();
    _actual->encodeBinary(e);
    // Rewritten in the next two lines due to, it seems, a bug in old gccs
    // inner_ensure_equals(types::decode((const unsigned char*)enc.data(), enc.size()).upcast<T>(), _actual);
    dec = core::BinaryDecoder(enc);
    unique_ptr<Type> decoded = types::decode(dec);
    wassert(actual(decoded) == _actual);

    dec = core::BinaryDecoder(enc);
    TypeCode dec_code;
    core::BinaryDecoder inner = dec.pop_type_envelope(dec_code);
    wassert(actual(dec_code) == code);
    wassert(actual(inner.size) == inner_enc_size);
    wassert(actual(decodeInner(code, inner)) == _actual);

    // String encoding
    stringstream ss;
    ss << *_actual;
    wassert(actual(types::decodeString(code, ss.str())) == *_actual);

    // JSON encoding
    {
        std::stringstream jbuf;
        structured::JSON json(jbuf);
        _actual->serialise(json, structured::keys_json);
        jbuf.seekg(0);
        std::string str(jbuf.str());
        auto reader = core::BufferedReader::from_string(str);
        structured::Memory parsed;
        structured::JSON::parse(*reader, parsed);
        wassert(actual(parsed.root().type()) == structured::NodeType::MAPPING);

        unique_ptr<Type> iparsed = wcallchecked(types::decode_structure(structured::keys_json, parsed.root()));
        wassert(actual(iparsed) == _actual);
    }
}

void ActualType::compares(const types::Type& higher) const
{
    if (!_actual) throw TestFailed("actual item to compare is undefined");

    auto higher2 = higher.clone();

    wassert(actual(*_actual == *_actual).istrue());
    wassert(actual(higher == higher).istrue());
    wassert(actual(*higher2 == *higher2).istrue());

    wassert(actual(*_actual < higher).istrue());
    wassert(actual(*_actual <= higher).istrue());
    wassert(actual(*_actual == higher).isfalse());
    wassert(actual(*_actual != higher).istrue());
    wassert(actual(*_actual >= higher).isfalse());
    wassert(actual(*_actual > higher).isfalse());

    wassert(actual(higher < *higher2).isfalse());
    wassert(actual(higher <= *higher2).istrue());
    wassert(actual(higher == *higher2).istrue());
    wassert(actual(higher != *higher2).isfalse());
    wassert(actual(higher >= *higher2).istrue());
    wassert(actual(higher >  *higher2).isfalse());
}

template<typename T>
static const T* get_specific_type(const types::Type* actual)
{
    if (!actual) throw TestFailed("item to check is undefined");

    const T* item = dynamic_cast<const T*>(actual);
    if (!item)
    {
        std::stringstream ss;
        ss << "metadata item '" << *actual << "' is not a ";
        int status;
        char* name = abi::__cxa_demangle(typeid(T).name(), 0, 0, &status);
        if (status == 0)
            ss << name;
        else
            ss << "(demangling failed: " << typeid(T).name() << ")";
        free(name);
        throw TestFailed(ss.str());
    }

    return item;
}

void ActualType::is_source_blob(
    const std::string& format,
    const std::string& basedir,
    const std::string& fname,
    uint64_t ofs,
    uint64_t size)
{
    const source::Blob* item = get_specific_type<source::Blob>(_actual);
    wassert(actual(item->format) == format);
    wassert(actual(item->basedir) == basedir);
    wassert(actual(item->filename) == fname);
    wassert(actual(item->offset) == ofs);
    wassert(actual(item->size) == size);
    if (!basedir.empty())
    {
        string expected;
        if (fname[0] == '/')
            expected = fname;
        else
            expected = sys::abspath(str::joinpath(basedir, fname));
        wassert(actual(item->absolutePathname()) == expected);
    }
}

void ActualType::is_source_blob(
    const std::string& format,
    const std::string& basedir,
    const std::string& fname)
{
    const source::Blob* item = get_specific_type<source::Blob>(_actual);
    wassert(actual(item->format) == format);
    wassert(actual(item->basedir) == basedir);
    wassert(actual(item->filename) == fname);
    if (!basedir.empty())
    {
        string expected;
        if (fname[0] == '/')
            expected = fname;
        else
            expected = sys::abspath(str::joinpath(basedir, fname));
        wassert(actual(item->absolutePathname()) == expected);
    }
}

void ActualType::is_source_url(const std::string& format, const std::string& url)
{
    const source::URL* item = get_specific_type<source::URL>(_actual);
    wassert(actual(item->format) == format);
    wassert(actual(item->url) == url);
}

void ActualType::is_source_inline(const std::string& format, uint64_t size)
{
    const source::Inline* item = get_specific_type<source::Inline>(_actual);
    wassert(actual(item->format) == format);
    wassert(actual(item->size) == size);
}

void ActualType::is_reftime_position(const Time& time)
{
    const reftime::Position* item = get_specific_type<reftime::Position>(_actual);
    wassert(actual(item->get_Position()) == time);
}

}
}
