#ifndef ARKI_MATCHER_PRODDEF_H
#define ARKI_MATCHER_PRODDEF_H

#include <arki/matcher/utils.h>
#include <arki/types/values.h>

namespace arki {
namespace matcher {

/**
 * Match Proddefs
 */
struct MatchProddef : public Implementation
{
    MatchProddef* clone() const override = 0;
    std::string name() const override;

    static Implementation* parse(const std::string& pattern);
    static void init();
};

struct MatchProddefGRIB : public MatchProddef
{
    types::ValueBagMatcher expr;

    MatchProddefGRIB(const types::ValueBagMatcher& expr);
    MatchProddefGRIB(const std::string& pattern);
    MatchProddefGRIB* clone() const override;
    bool matchItem(const types::Type& o) const override;
    bool match_buffer(types::Code code, const uint8_t* data,
                      unsigned size) const override;
    std::string toString() const override;
};

} // namespace matcher
} // namespace arki

#endif
