#include "tests.h"
#include <arki/matcher.h>
#include <arki/metadata.h>

using namespace std;
using namespace arki::tests;
using namespace arki;
using namespace arki::types;

namespace arki {
namespace tests {

void ActualMatcher::matches(const Metadata& md) const
{
    wassert(actual(_actual(md)).istrue());

    // Check stringification and reparsing
    Matcher m1 = Matcher::parse(_actual.toString());

    //fprintf(stderr, "%s -> %s -> %s\n", expr.c_str(), _actual.toString().c_str(), m1.toString().c_str());

    wassert(actual(m1.toString()) == _actual.toString());
    wassert(actual(m1(md)) == _actual(md));

    // Retry with an expanded stringification
    Matcher m2 = Matcher::parse(_actual.toStringExpanded());
    wassert(actual(m2.toStringExpanded()) == _actual.toStringExpanded());
    wassert(actual(m2(md)) == _actual(md));
}

void ActualMatcher::not_matches(const Metadata& md) const
{
    wassert(actual(_actual(md)).isfalse());

    // Check stringification and reparsing
    Matcher m1 = Matcher::parse(_actual.toString());

    //fprintf(stderr, "%s -> %s -> %s\n", expr.c_str(), _actual.toString().c_str(), m1.toString().c_str());

    wassert(actual(m1.toString()) == _actual.toString());
    wassert(actual(m1(md)) == _actual(md));

    // Retry with an expanded stringification
    Matcher m2 = Matcher::parse(_actual.toStringExpanded());
    wassert(actual(m2.toStringExpanded()) == _actual.toStringExpanded());
    wassert(actual(m2(md)) == _actual(md));
}

}
}
