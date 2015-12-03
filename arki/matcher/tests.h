#ifndef ARKI_MATCHER_TESTUTILS_H
#define ARKI_MATCHER_TESTUTILS_H

#include <arki/metadata/tests.h>

namespace arki {
struct Metadata;

namespace tests {

#define ensure_matches(expr, md) wassert(arki::tests::impl_ensure_matches((expr), (md), true))
#define ensure_not_matches(expr, md) wassert(arki::tests::impl_ensure_matches((expr), (md), false))
void impl_ensure_matches(const std::string& expr, const Metadata& md, bool shouldMatch=true);

}
}

#endif
