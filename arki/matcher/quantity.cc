#include "config.h"

#include <arki/matcher/quantity.h>
#include <arki/matcher/utils.h>
#include <arki/types/utils.h>

#include <set>
#include <string>

using namespace std;
using namespace arki::types;

namespace arki {
namespace matcher {

/*============================================================================*/

std::string MatchQuantity::name() const { return "quantity"; }

MatchQuantity::MatchQuantity(const std::string& pattern)
{
	arki::types::split(pattern, values);
}

bool MatchQuantity::matchItem(const Type& o) const
{
	const types::Quantity* v = dynamic_cast<const types::Quantity*>(&o);
	if (!v) return false;

	//se il matche specifica dei valori
	if (values.size())
	{
        //allora tutti i valori indicati devono essere presenti nell'oggetto
        for (std::set<std::string>::const_iterator i = values.begin();
                i != values.end(); ++i)
        {
            if (v->values.find(*i) == v->values.end())
                return false;
        }
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

unique_ptr<MatchQuantity> MatchQuantity::parse(const std::string& pattern)
{
    return unique_ptr<MatchQuantity>(new MatchQuantity(pattern));
}

void MatchQuantity::init()
{
    Matcher::register_matcher("quantity", types::TYPE_QUANTITY, (MatcherType::subexpr_parser)MatchQuantity::parse);
}

}
}
