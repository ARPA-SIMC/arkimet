#ifndef ARKI_TEST_UTILS_H
#define ARKI_TEST_UTILS_H

#include <arki/utils/tests.h>
#include <arki/wibble/tests/tut.h>
#include <memory>

#define TESTGRP(name) \
typedef test_group<name ## _shar> tg; \
typedef tg::object to; \
tg name ## _tg (#name);

namespace arki {

namespace utils {
namespace sys {
struct NamedFileDescriptor;
}
}

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

/**
 * Creates a tempfile, runs body and returns the contents of the temp file after that.
 *
 * The temp file is created in the current directory with a fixed name; this is
 * ok for tests that run on a temp dir, and is not to be used outside tests.
 */
std::string tempfile_to_string(std::function<void(arki::utils::sys::NamedFileDescriptor& out)> body);

#define def_test(num) template<> template<> void to::test<num>()

#define def_tests(name) \
  class Tests : public TestCase { \
      using TestCase::TestCase; \
      void register_tests() override; \
  } test##name(#name);

}
}
#endif
