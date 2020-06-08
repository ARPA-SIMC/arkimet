#include "tests.h"
#include "arki/matcher.h"
#include "arki/matcher/parser.h"
#include "arki/metadata.h"

using namespace std;
using namespace arki::tests;
using namespace arki;
using namespace arki::types;

namespace arki {
namespace tests {

ActualMatcher::ActualMatcher(const std::string& actual)
    : arki::utils::tests::Actual<Matcher>(matcher::Parser().parse(actual)), orig(actual)
{
}

ActualMatcher::ActualMatcher(const matcher::Parser& parser, const std::string& actual)
    : arki::utils::tests::Actual<Matcher>(parser.parse(actual)), parser(&parser), orig(actual)
{
}

Matcher ActualMatcher::parse(const std::string& str) const
{
    if (parser)
        return parser->parse(str);
    else
        return matcher::Parser().parse(str);
}

void ActualMatcher::matches(const Metadata& md) const
{
    wassert(actual(_actual(md)).istrue());

    // Check stringification and reparsing
    Matcher m1 = parse(_actual.toString());

    //fprintf(stderr, "%s -> %s -> %s\n", expr.c_str(), _actual.toString().c_str(), m1.toString().c_str());

    wassert(actual(m1.toString()) == _actual.toString());
    wassert(actual(m1(md)) == _actual(md));

    // Retry with an expanded stringification
    Matcher m2 = parse(_actual.toStringExpanded());
    wassert(actual(m2.toStringExpanded()) == _actual.toStringExpanded());
    wassert(actual(m2(md)) == _actual(md));
}

void ActualMatcher::not_matches(const Metadata& md) const
{
    wassert(actual(_actual(md)).isfalse());

    // Check stringification and reparsing
    Matcher m1 = parse(_actual.toString());

    //fprintf(stderr, "%s -> %s -> %s\n", expr.c_str(), _actual.toString().c_str(), m1.toString().c_str());

    wassert(actual(m1.toString()) == _actual.toString());
    wassert(actual(m1(md)) == _actual(md));

    // Retry with an expanded stringification
    Matcher m2 = parse(_actual.toStringExpanded());
    wassert(actual(m2.toStringExpanded()) == _actual.toStringExpanded());
    wassert(actual(m2(md)) == _actual(md));
}

static std::unique_ptr<Type> from_string(const std::string& s)
{
    auto pos = s.find(":");
    auto type = s.substr(0, pos);
    auto desc = s.substr(pos + 1);
    return decodeString(parseCodeName(type.c_str()), desc);
}


void ActualMatcher::matches(const std::string& sitem) const
{
    auto item = from_string(sitem);

    wassert(actual(_actual(*item)).istrue());

    // Check stringification and reparsing
    Matcher m1 = wcallchecked(parse(_actual.toString()));

    //fprintf(stderr, "%s -> %s -> %s\n", expr.c_str(), _actual.toString().c_str(), m1.toString().c_str());

    wassert(actual(m1.toString()) == _actual.toString());
    wassert(actual(m1(*item)) == _actual(*item));

    // Retry with an expanded stringification
    Matcher m2 = wcallchecked(parse(_actual.toStringExpanded()));
    wassert(actual(m2.toStringExpanded()) == _actual.toStringExpanded());
    wassert(actual(m2(*item)) == _actual(*item));
}

void ActualMatcher::not_matches(const std::string& sitem) const
{
    auto item = from_string(sitem);

    wassert(actual(_actual(*item)).isfalse());

    // Check stringification and reparsing
    Matcher m1 = wcallchecked(parse(_actual.toString()));

    //fprintf(stderr, "%s -> %s -> %s\n", expr.c_str(), _actual.toString().c_str(), m1.toString().c_str());

    wassert(actual(m1.toString()) == _actual.toString());
    wassert(actual(m1(*item)) == _actual(*item));

    // Retry with an expanded stringification
    Matcher m2 = wcallchecked(parse(_actual.toStringExpanded()));
    wassert(actual(m2.toStringExpanded()) == _actual.toStringExpanded());
    wassert(actual(m2(*item)) == _actual(*item));
}

}
}
