#ifndef ARKI_MATCHER_TESTUTILS_H
#define ARKI_MATCHER_TESTUTILS_H

#include <arki/metadata/tests.h>
#include <arki/matcher.h>

namespace arki {
struct Metadata;

namespace tests {

class ActualMatcher : public arki::utils::tests::Actual<Matcher>
{
    const matcher::Parser* parser = nullptr;
    std::string orig;

    Matcher parse(const std::string& str) const;

public:
    ActualMatcher(const std::string& actual);
    ActualMatcher(const matcher::Parser& parser, const std::string& actual);

    /// Ensure that the matcher matches this metadata
    void matches(const Metadata& md) const;
    void matches(std::shared_ptr<Metadata> md) const { matches(*md); }

    /// Ensure that the matcher does not match this metadata
    void not_matches(const Metadata& md) const;
    void not_matches(std::shared_ptr<Metadata> md) const { matches(*md); }

    /// Ensure that the matcher matches this metadata item
    void matches(const std::string& item) const;

    /// Ensure that the matcher does not match this metadata item
    void not_matches(const std::string& item) const;
};

inline arki::tests::ActualMatcher actual_matcher(const std::string& actual)
{
    return arki::tests::ActualMatcher(actual);
}

inline arki::tests::ActualMatcher actual_matcher(const matcher::Parser& parser, const std::string& actual)
{
    return arki::tests::ActualMatcher(parser, actual);
}

#define ensure_matches(expr, md) wassert(arki::tests::actual_matcher(expr).matches(md))
#define ensure_not_matches(expr, md) wassert(arki::tests::actual_matcher(expr).not_matches(md))

}
}

#endif
