#ifndef ARKI_MATCHER_QUANTITY
#define ARKI_MATCHER_QUANTITY

/// Radar quantity matcher
#include <arki/matcher/utils.h>
#include <set>
#include <string>

namespace arki {
namespace matcher {

/**
 * Match quantities
 */
struct MatchQuantity : public Implementation
{
    std::set<std::string> values;

    MatchQuantity(const MatchQuantity& o);
    MatchQuantity(const std::string& pattern);

    MatchQuantity* clone() const override;
    std::string name() const override;

    bool matchItem(const types::Type& o) const override;
    std::string toString() const override;

    static Implementation* parse(const std::string& pattern);
    static void init();
};

}
}
#endif
