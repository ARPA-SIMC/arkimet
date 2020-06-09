#ifndef ARKI_MATCHER_QUANTITY
#define ARKI_MATCHER_QUANTITY

/// Radar quantity matcher
#include <arki/matcher.h>
#include <arki/matcher/utils.h>
#include <arki/types/quantity.h>
#include <set>
#include <string>

namespace arki {
namespace matcher {

/**
 * Match quantities
 */
struct MatchQuantity : public Implementation
{
    std::string name() const override;

    std::set<std::string> values;

    MatchQuantity(const std::string& pattern);
    bool matchItem(const types::Type& o) const override;
    std::string toString() const override;

    static Implementation* parse(const std::string& pattern);
    static void init();
};

}
}
#endif
