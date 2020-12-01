#include "config.h"
#include "proddef.h"

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
    return v->get_GRIB().contains(expr);
}

std::string MatchProddefGRIB::toString() const
{
	return "GRIB:" + expr.toString();
}

Implementation* MatchProddef::parse(const std::string& pattern)
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
        case types::Proddef::Style::GRIB: return new MatchProddefGRIB(rest);
        default: throw invalid_argument("cannot parse type of proddef to match: unsupported proddef style: " + name);
    }
}

void MatchProddef::init()
{
    MatcherType::register_matcher("proddef", TYPE_PRODDEF, (MatcherType::subexpr_parser)MatchProddef::parse);
}

}
}
