#ifndef ARKI_TEST_LEGACY_H
#define ARKI_TEST_LEGACY_H

#include <arki/tests/tests.h>
#include <memory>

namespace arki {
namespace tests {

inline void ensure(bool cond)
{
    wassert(actual(cond));
}

template <class T,class Q>
inline void ensure_equals(const Q& _actual, const T& expected)
{
    wassert(actual(_actual) == expected);
}

}
}
#endif

