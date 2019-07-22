#ifndef ARKI_MATCHER_PRODDEF_H
#define ARKI_MATCHER_PRODDEF_H

#include <arki/matcher.h>
#include <arki/matcher/utils.h>
#include <arki/types/proddef.h>

namespace arki {
namespace matcher {

/**
 * Match Proddefs
 */
struct MatchProddef : public Implementation
{
    std::string name() const override;

    static std::unique_ptr<MatchProddef> parse(const std::string& pattern);
    static void init();
};

struct MatchProddefGRIB : public MatchProddef
{
    ValueBag expr;

    MatchProddefGRIB(const std::string& pattern);
    bool matchItem(const types::Type& o) const override;
    std::string toString() const override;
};

}
}

// vim:set ts=4 sw=4:
#endif
