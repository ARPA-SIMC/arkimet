#ifndef ARKI_MATCHER_TESTUTILS_H
#define ARKI_MATCHER_TESTUTILS_H

#include <arki/metadata/tests.h>
#include <arki/matcher.h>

namespace arki {
struct Metadata;

namespace tests {

class ActualMatcher : public arki::utils::tests::Actual<Matcher>
{
public:
    ActualMatcher(const Matcher& actual) : arki::utils::tests::Actual<Matcher>(actual) {}

    /// Ensure that the matcher matches this metadata
    void matches(const Metadata& md) const;

    /// Ensure that the matcher does not match this metadata
    void not_matches(const Metadata& md) const;
};

inline arki::tests::ActualMatcher actual_matcher(const std::string& actual) { return arki::tests::ActualMatcher(Matcher::parse(actual)); }
inline arki::tests::ActualMatcher actual(const arki::Matcher& actual) { return arki::tests::ActualMatcher(actual); }


#define ensure_matches(expr, md) wassert(arki::tests::impl_ensure_matches((expr), (md), true))
#define ensure_not_matches(expr, md) wassert(arki::tests::impl_ensure_matches((expr), (md), false))
void impl_ensure_matches(const std::string& expr, const Metadata& md, bool shouldMatch=true);

}
}

#endif
