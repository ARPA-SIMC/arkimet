#include "config.h"

#include <arki/matcher/proddef.h>
#include <arki/matcher/utils.h>

using namespace std;
using namespace arki::types;
using namespace arki::utils;

namespace arki {
namespace matcher {

std::string MatchProddef::name() const { return "proddef"; }

MatchProddefGRIB::MatchProddefGRIB(const std::string& pattern)
{
	expr = ValueBag::parse(pattern);
}

bool MatchProddefGRIB::matchItem(const Type& o) const
{
    const types::proddef::GRIB* v = dynamic_cast<const types::proddef::GRIB*>(&o);
    if (!v) return false;
    return v->values().contains(expr);
}

std::string MatchProddefGRIB::toString() const
{
	return "GRIB:" + expr.toString();
}

unique_ptr<MatchProddef> MatchProddef::parse(const std::string& pattern)
{
    size_t beg = 0;
    size_t pos = pattern.find(':', beg);
    string name;
    string rest;
    if (pos == string::npos)
        name = str::strip(pattern.substr(beg));
    else {
        name = str::strip(pattern.substr(beg, pos-beg));
        rest = pattern.substr(pos+1);
    }
    switch (types::Proddef::parseStyle(name))
    {
        case types::Proddef::GRIB: return unique_ptr<MatchProddef>(new MatchProddefGRIB(rest));
        default: throw runtime_error("cannot parse type of proddef to match: unsupported proddef style: " + name);
    }
}

void MatchProddef::init()
{
    Matcher::register_matcher("proddef", TYPE_PRODDEF, (MatcherType::subexpr_parser)MatchProddef::parse);
}

}
}
