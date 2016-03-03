#ifndef ARKI_CORE_TESTS_H
#define ARKI_CORE_TESTS_H

#include <arki/tests/tests.h>
#include <arki/types/time.h>
#ifdef HAVE_LUA
#include <arki/tests/lua.h>
#endif

namespace arki {
namespace tests {

class ActualTime : public arki::utils::tests::Actual<arki::types::Time>
{
public:
    ActualTime(const types::Time& actual) : arki::utils::tests::Actual<types::Time>(actual) {}

    using arki::utils::tests::Actual<arki::types::Time>::operator==;
    using arki::utils::tests::Actual<arki::types::Time>::operator!=;

    void operator==(const std::string& expected) const;
    void operator!=(const std::string& expected) const;

    /**
     * Check that a metadata field can be serialized and deserialized in all
     * sorts of ways
     */
    void serializes() const;

    /**
     * Check comparison operators
     */
    void compares(const types::Time& higher) const;

    /// Check all components of a Time item
    void is(int ye, int mo, int da, int ho, int mi, int se);
};

inline arki::tests::ActualTime actual_time(const arki::types::Time& actual) { return arki::tests::ActualTime(actual); }
inline arki::tests::ActualTime actual(const arki::types::Time& actual) { return arki::tests::ActualTime(actual); }

}
}
#endif

