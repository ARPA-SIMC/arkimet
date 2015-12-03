#ifndef ARKI_TEST_UTILS_H
#define ARKI_TEST_UTILS_H

#include <arki/utils/tests.h>
#include <arki/wibble/tests/tut.h>
#include <arki/wibble/tests/tut_reporter.h>
#include <memory>

#define TESTGRP(name) \
typedef test_group<name ## _shar> tg; \
typedef tg::object to; \
tg name ## _tg (#name);

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

#define wruntest(func, ...) wassert(func(__VA_ARGS__))

}
}
#endif
