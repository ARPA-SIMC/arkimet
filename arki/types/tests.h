#ifndef ARKI_TYPES_TESTUTILS_H
#define ARKI_TYPES_TESTUTILS_H

#include <arki/core/tests.h>
#include <arki/types.h>
#include <arki/core/time.h>
#include <arki/libconfig.h>
#include <vector>
#include <string>
#include <cstdint>

namespace arki {

namespace types {
class Type;
}

namespace tests {

/// Prepackaged set of tests for generic arki types
struct TestGenericType
{
    // Item code
    std::string tag;
    // Stringified sample to test
    std::string sample;
    // Alternate items that should all test equals to sample
    std::vector<std::string> alternates;
    // Optional SQL exactquery to test
    std::string exact_query;
    // List of items that test higher than sample
    std::vector<std::string> lower;
    // List of items that test lower than sample
    std::vector<std::string> higher;

    TestGenericType(const std::string& tag, const std::string& sample);

    void check() const;

    /// Decode item from encoded, running single-item checks and passing ownership back the caller
    void check_item(const std::string& encoded, std::unique_ptr<types::Type>& item) const;
};


class ActualType : public arki::utils::tests::Actual<const arki::types::Type*>
{
public:
    ActualType(const types::Type* actual) : arki::utils::tests::Actual<const types::Type*>(actual) {}

    void operator==(const types::Type* expected) const;
    void operator!=(const types::Type* expected) const;

    void operator==(const types::Type& expected) const { operator==(&expected); }
    void operator!=(const types::Type& expected) const { operator!=(&expected); }
    template<typename E> void operator==(const std::unique_ptr<E>& expected) const { operator==(expected.get()); }
    template<typename E> void operator!=(const std::unique_ptr<E>& expected) const { operator!=(expected.get()); }
    void operator==(const std::string& expected) const;
    void operator!=(const std::string& expected) const;
    /*
    template<typename E> TestIsLt<A, E> operator<(const E& expected) const { return TestIsLt<A, E>(actual, expected); }
    template<typename E> TestIsLte<A, E> operator<=(const E& expected) const { return TestIsLte<A, E>(actual, expected); }
    template<typename E> TestIsGt<A, E> operator>(const E& expected) const { return TestIsGt<A, E>(actual, expected); }
    template<typename E> TestIsGte<A, E> operator>=(const E& expected) const { return TestIsGte<A, E>(actual, expected); }
    */

    /**
     * Check that a metadata field can be serialized and deserialized in all
     * sorts of ways
     */
    void serializes() const;

    /**
     * Check comparison operators
     */
    void compares(const types::Type& higher) const;

    /// Check all components of a source::Blob item
    void is_source_blob(
        const std::string& format, const std::string& basedir, const std::string& fname,
        uint64_t ofs, uint64_t size);

    /// Check only the file name components of a source::Blob item
    void is_source_blob(
        const std::string& format, const std::string& basedir, const std::string& fname);

    /// Check all components of a source::URL item
    void is_source_url(const std::string& format, const std::string& url);

    /// Check all components of a source::Inline item
    void is_source_inline(const std::string& format, uint64_t size);

    /// Check all components of a reftime::Position item
    void is_reftime_position(const core::Time&);
};

inline arki::tests::ActualType actual_type(const arki::types::Type& actual) { return arki::tests::ActualType(&actual); }
inline arki::tests::ActualType actual_type(const arki::types::Type* actual) { return arki::tests::ActualType(actual); }
inline arki::tests::ActualType actual(const arki::types::Type& actual) { return arki::tests::ActualType(&actual); }
template<typename T>
inline arki::tests::ActualType actual(const std::unique_ptr<T>& actual) { return arki::tests::ActualType(actual.get()); }
inline arki::tests::Actual<const types::ValueBag&> actual(const types::ValueBag& actual) { return arki::tests::Actual<const types::ValueBag&>(actual); }

template<typename TYPE>
struct TypeTestCase : public TestCase
{
    using TestCase::TestCase;

    static std::unique_ptr<TYPE> parse(const std::string& encoded)
    {
        auto res = wcallchecked(types::decodeString(types::traits<TYPE>::type_code, encoded));
        return downcast<TYPE>(move(res));
    }

    void add_generic_test(const char* name, const std::vector<std::string>& lower, const std::vector<std::string>& exact, const std::vector<std::string>& higher, const std::string& exact_match=std::string())
    {
        add_method(name, [=] {
            TestGenericType t(types::traits<TYPE>::type_tag, exact[0]);
            for (size_t i = 1; i < exact.size(); ++i)
                t.alternates.push_back(exact[i]);
            t.lower = lower;
            t.higher = higher;
            t.exact_query = exact_match;
            wassert(t.check());
        });
    }

    void add_generic_test(const char* name, const std::vector<std::string>& lower, const std::string& exact, const std::vector<std::string>& higher, const std::string& exact_match=std::string())
    {
        add_generic_test(name, lower, std::vector<std::string>({ exact }), higher, exact_match);
    }
};

}
}
#endif
