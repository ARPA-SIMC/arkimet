#include "config.h"

#include <arki/matcher/product.h>
#include <arki/matcher/utils.h>
#include <limits>
#include <algorithm>

#ifdef HAVE_VM2
#include <arki/utils/vm2.h>
#endif

using namespace std;
using namespace arki::types;
using namespace arki::utils;

namespace arki {
namespace matcher {

std::string MatchProduct::name() const { return "product"; }

MatchProductGRIB1::MatchProductGRIB1(const std::string& pattern)
{
	OptionalCommaList args(pattern);
	origin = args.getInt(0, -1);
	table = args.getInt(1, -1);
	product = args.getInt(2, -1);
}

bool MatchProductGRIB1::matchItem(const Type& o) const
{
    const types::product::GRIB1* v = dynamic_cast<const types::product::GRIB1*>(&o);
    if (!v) return false;
	if (origin != -1 && (unsigned)origin != v->origin()) return false;
	if (table != -1 && (unsigned)table != v->table()) return false;
	if (product != -1 && (unsigned)product != v->product()) return false;
	return true;
}

std::string MatchProductGRIB1::toString() const
{
	CommaJoiner res;
	res.add("GRIB1");
	if (origin != -1) res.add(origin); else res.addUndef();
	if (table != -1) res.add(table); else res.addUndef();
	if (product != -1) res.add(product); else res.addUndef();
	return res.join();
}

MatchProductGRIB2::MatchProductGRIB2(const std::string& pattern)
{
	OptionalCommaList args(pattern);
	centre = args.getInt(0, -1);
	discipline = args.getInt(1, -1);
	category = args.getInt(2, -1);
	number = args.getInt(3, -1);
    table_version = args.getInt(4, -1);
    local_table_version = args.getInt(5, -1);
}

bool MatchProductGRIB2::matchItem(const Type& o) const
{
    const types::product::GRIB2* v = dynamic_cast<const types::product::GRIB2*>(&o);
    if (!v) return false;
	if (centre != -1 && (unsigned)centre != v->centre()) return false;
	if (discipline != -1 && (unsigned)discipline != v->discipline()) return false;
	if (category != -1 && (unsigned)category != v->category()) return false;
    if (number != -1 && (unsigned)number != v->number()) return false;
    if (table_version != -1 && (unsigned)table_version != v->table_version()) return false;
    if (local_table_version != -1 && (unsigned)local_table_version != v->local_table_version()) return false;
	return true;
}

std::string MatchProductGRIB2::toString() const
{
	CommaJoiner res;
	res.add("GRIB2");
	if (centre != -1) res.add(centre); else res.addUndef();
	if (discipline != -1) res.add(discipline); else res.addUndef();
	if (category != -1) res.add(category); else res.addUndef();
	if (number != -1) res.add(number); else res.addUndef();
    if (table_version != -1) res.add(table_version); else res.addUndef();
    if (local_table_version != -1) res.add(local_table_version); else res.addUndef();
	return res.join();
}

MatchProductBUFR::MatchProductBUFR(const std::string& pattern)
{
	OptionalCommaList args(pattern, true);
	type = args.getInt(0, -1);
	subtype = args.getInt(1, -1);
	localsubtype = args.getInt(2, -1);
	values = ValueBag::parse(args.tail);
}

bool MatchProductBUFR::matchItem(const Type& o) const
{
    const types::product::BUFR* v = dynamic_cast<const types::product::BUFR*>(&o);
    if (!v) return false;
	if (type != -1 && (unsigned)type != v->type()) return false;
	if (subtype != -1 && (unsigned)subtype != v->subtype()) return false;
	if (localsubtype != -1 && (unsigned)localsubtype != v->localsubtype()) return false;
	return v->values().contains(values);
}

std::string MatchProductBUFR::toString() const
{
	stringstream res;
	res << "BUFR";
	if (type != -1 || subtype != -1 || localsubtype != -1)
	{
		res << ",";
		if (type != -1) res << type;
		if (subtype != -1 || localsubtype != -1)
		{
			res << ",";
			if (subtype != -1) res << subtype;
			if (localsubtype != -1) res << "," << localsubtype;
		}
	}
	if (!values.empty())
		res << ":" << values.toString();
	return res.str();
}

static const double DOUBLENAN = std::numeric_limits<double>::quiet_NaN();

MatchProductODIMH5::MatchProductODIMH5(const std::string& pattern)
{
	OptionalCommaList args(pattern, true);
	obj		= args.getString(0, "");
	prod		= args.getString(1, "");
	/*REMOVED:prodpar1	= args.getDouble(2, DOUBLENAN); */
	/*REMOVED:prodpar2	= args.getDouble(3, DOUBLENAN); */
}

bool MatchProductODIMH5::matchItem(const Type& o) const
{
    const types::product::ODIMH5* v = dynamic_cast<const types::product::ODIMH5*>(&o);
    if (!v) return false;
	if (obj.size() && 		obj != v->obj()) 	return false;
	if (prod.size() && 		prod != v->prod()) 	return false;
	/*REMOVED:if (!isnan(prodpar1) && 	prodpar1 != v->prodpar1()) 	return false; */
	/*REMOVED:if (!isnan(prodpar2) && 	prodpar2 != v->prodpar2()) 	return false; */
	return true;
}

std::string MatchProductODIMH5::toString() const
{
	CommaJoiner res;
	res.add("ODIMH5");
	if (obj.size()) 	res.add(obj); 		else res.addUndef();
	if (prod.size()) 	res.add(prod); 		else res.addUndef();
	/*REMOVED:if (!isnan(prodpar1)) 	res.add(prodpar1); 	else res.addUndef();*/
	/*REMOVED:if (!isnan(prodpar2)) 	res.add(prodpar2); 	else res.addUndef();*/
	return res.join();
}

MatchProductVM2::MatchProductVM2(const std::string& pattern)
{
    OptionalCommaList args(pattern, true);
	variable_id = args.getInt(0, -1);
    expr = ValueBag::parse(args.tail);
#ifdef HAVE_VM2
    if (!expr.empty())
        idlist = utils::vm2::find_variables(expr);
#endif
}
bool MatchProductVM2::matchItem(const Type& o) const
{
    const types::product::VM2* v = dynamic_cast<const types::product::VM2*>(&o);
    if (!v) return false;

    if (variable_id != -1 && (unsigned)variable_id != v->variable_id()) return false;
    if (!expr.empty() && 
        std::find(idlist.begin(), idlist.end(), v->variable_id()) == idlist.end())
            return false;
    return true;
}
std::string MatchProductVM2::toString() const
{
	stringstream res;
	res << "VM2";
	if (variable_id != -1)
	{
		res << "," << variable_id;
	}
	if (!expr.empty())
		res << ":" << expr.toString();
	return res.str();
}

unique_ptr<MatchProduct> MatchProduct::parse(const std::string& pattern)
{
    size_t beg = 0;
    size_t pos = pattern.find_first_of(":,", beg);
    string name;
    string rest;
    if (pos == string::npos)
        name = str::strip(pattern.substr(beg));
    else {
        name = str::strip(pattern.substr(beg, pos-beg));
        rest = pattern.substr(pos+(pattern[pos] == ',' ? 1 : 0));
    }
    switch (types::Product::parseStyle(name))
    {
        case types::Product::GRIB1: return unique_ptr<MatchProduct>(new MatchProductGRIB1(rest));
        case types::Product::GRIB2: return unique_ptr<MatchProduct>(new MatchProductGRIB2(rest));
        case types::Product::BUFR: return unique_ptr<MatchProduct>(new MatchProductBUFR(rest));
        case types::Product::ODIMH5: return unique_ptr<MatchProduct>(new MatchProductODIMH5(rest));
        case types::Product::VM2: return unique_ptr<MatchProduct>(new MatchProductVM2(rest));
        default: throw std::runtime_error("cannot parse type of product to match: unsupported product style: " + name);
    }
}

void MatchProduct::init()
{
    Matcher::register_matcher("product", TYPE_PRODUCT, (MatcherType::subexpr_parser)MatchProduct::parse);
}

}
}
