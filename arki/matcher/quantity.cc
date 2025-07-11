#include "quantity.h"
#include "arki/types/quantity.h"
#include "arki/types/utils.h"
#include <set>
#include <string>

using namespace std;
using namespace arki::types;

namespace arki {
namespace matcher {

/*============================================================================*/

std::string MatchQuantity::name() const { return "quantity"; }

MatchQuantity::MatchQuantity(const MatchQuantity& o)
    : Implementation(o), values(o.values)
{
}

MatchQuantity::MatchQuantity(const std::string& pattern)
{
    arki::types::split(pattern, values);
}

MatchQuantity* MatchQuantity::clone() const { return new MatchQuantity(*this); }

bool MatchQuantity::matchItem(const Type& o) const
{
    const types::Quantity* v = dynamic_cast<const types::Quantity*>(&o);
    if (!v)
        return false;

    // If the matcher has values to match
    if (values.size())
    {
        auto vvalues = v->get();
        // allora tutti i valori indicati devono essere presenti nell'oggetto
        for (const auto& i : values)
            if (vvalues.find(i) == vvalues.end())
                return false;
    }

    return true;
}

std::string MatchQuantity::toString() const
{
    CommaJoiner res;
    for (std::set<std::string>::const_iterator i = values.begin();
         i != values.end(); ++i)
        res.add(*i);
    return res.join();
}

Implementation* MatchQuantity::parse(const std::string& pattern)
{
    return new MatchQuantity(pattern);
}

void MatchQuantity::init()
{
    MatcherType::register_matcher(
        "quantity", TYPE_QUANTITY,
        (MatcherType::subexpr_parser)MatchQuantity::parse);
}

} // namespace matcher
} // namespace arki
