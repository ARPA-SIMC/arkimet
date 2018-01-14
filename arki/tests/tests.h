#ifndef ARKI_TEST_UTILS_H
#define ARKI_TEST_UTILS_H

#include <arki/utils/tests.h>
#include <memory>

namespace arki {
namespace tests {

using namespace arki::utils::tests;
using arki::utils::tests::actual;

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
