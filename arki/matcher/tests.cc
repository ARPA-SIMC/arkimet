#include "tests.h"
#include <arki/matcher.h>
#include <arki/metadata.h>

using namespace std;
using namespace arki::tests;
using namespace arki;
using namespace arki::types;

namespace arki {
namespace tests {

void impl_ensure_matches(const std::string& expr, const Metadata& md, bool shouldMatch)
{
    Matcher m = Matcher::parse(expr);

    // Check that it should match as expected
    if (shouldMatch)
        wassert(actual(m(md)));
    else
        wassert(actual(not m(md)));

    // Check stringification and reparsing
    Matcher m1 = Matcher::parse(m.toString());

    //fprintf(stderr, "%s -> %s -> %s\n", expr.c_str(), m.toString().c_str(), m1.toString().c_str());

    wassert(actual(m1.toString()) == m.toString());
    wassert(actual(m1(md)) == m(md));

    // Retry with an expanded stringification
    Matcher m2 = Matcher::parse(m.toStringExpanded());
    wassert(actual(m2.toStringExpanded()) == m.toStringExpanded());
    wassert(actual(m2(md)) == m(md));
}

}
}
