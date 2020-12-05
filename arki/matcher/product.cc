#include "config.h"

#include "product.h"
#include "arki/types/product.h"
#include <limits>
#include <algorithm>

#ifdef HAVE_VM2
#include "arki/utils/vm2.h"
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
    const types::Product* v = dynamic_cast<const types::Product*>(&o);
    if (!v) return false;
    if (v->style() != product::Style::GRIB1) return false;
    unsigned ori, tab, pro;
    v->get_GRIB1(ori, tab, pro);
    if (origin != -1 && (unsigned)origin != ori) return false;
    if (table != -1 && (unsigned)table != tab) return false;
    if (product != -1 && (unsigned)product != pro) return false;
    return true;
}

bool MatchProductGRIB1::match_buffer(types::Code code, const uint8_t* data, unsigned size) const
{
    if (code != TYPE_PRODUCT) return false;
    if (size < 1) return false;
    if (Product::style(data, size) != product::Style::GRIB1) return false;
    unsigned ori, tab, pro;
    Product::get_GRIB1(data, size, ori, tab, pro);
    if (origin != -1 && (unsigned)origin != ori) return false;
    if (table != -1 && (unsigned)table != tab) return false;
    if (product != -1 && (unsigned)product != pro) return false;
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
    const types::Product* v = dynamic_cast<const types::Product*>(&o);
    if (!v) return false;
    if (v->style() != product::Style::GRIB2) return false;
    unsigned ce, di, ca, nu, ta, lo;
    v->get_GRIB2(ce, di, ca, nu, ta, lo);
    if (centre != -1 && (unsigned)centre != ce) return false;
    if (discipline != -1 && (unsigned)discipline != di) return false;
    if (category != -1 && (unsigned)category != ca) return false;
    if (number != -1 && (unsigned)number != nu) return false;
    if (table_version != -1 && (unsigned)table_version != ta) return false;
    if (local_table_version != -1 && (unsigned)local_table_version != lo) return false;
    return true;
}

bool MatchProductGRIB2::match_buffer(types::Code code, const uint8_t* data, unsigned size) const
{
    if (code != TYPE_PRODUCT) return false;
    if (size < 1) return false;
    if (Product::style(data, size) != product::Style::GRIB2) return false;
    unsigned ce, di, ca, nu, ta, lo;
    Product::get_GRIB2(data, size, ce, di, ca, nu, ta, lo);
    if (centre != -1 && (unsigned)centre != ce) return false;
    if (discipline != -1 && (unsigned)discipline != di) return false;
    if (category != -1 && (unsigned)category != ca) return false;
    if (number != -1 && (unsigned)number != nu) return false;
    if (table_version != -1 && (unsigned)table_version != ta) return false;
    if (local_table_version != -1 && (unsigned)local_table_version != lo) return false;
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
    const types::Product* v = dynamic_cast<const types::Product*>(&o);
    if (!v) return false;
    if (v->style() != product::Style::BUFR) return false;
    unsigned ty, su, lo;
    ValueBag va;
    v->get_BUFR(ty, su, lo, va);
    if (type != -1 && (unsigned)type != ty) return false;
    if (subtype != -1 && (unsigned)subtype != su) return false;
    if (localsubtype != -1 && (unsigned)localsubtype != lo) return false;
    return va.contains(values);
}

bool MatchProductBUFR::match_buffer(types::Code code, const uint8_t* data, unsigned size) const
{
    if (code != TYPE_PRODUCT) return false;
    if (size < 1) return false;
    if (Product::style(data, size) != product::Style::BUFR) return false;
    unsigned ty, su, lo;
    ValueBag va;
    Product::get_BUFR(data, size, ty, su, lo, va);
    if (type != -1 && (unsigned)type != ty) return false;
    if (subtype != -1 && (unsigned)subtype != su) return false;
    if (localsubtype != -1 && (unsigned)localsubtype != lo) return false;
    return va.contains(values);
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
    const types::Product* v = dynamic_cast<const types::Product*>(&o);
    if (!v) return false;
    if (v->style() != product::Style::ODIMH5) return false;
    std::string ob, pr;
    v->get_ODIMH5(ob, pr);
    if (obj.size() &&  obj != ob)  return false;
    if (prod.size() && prod != pr) return false;
    return true;
}

bool MatchProductODIMH5::match_buffer(types::Code code, const uint8_t* data, unsigned size) const
{
    if (code != TYPE_PRODUCT) return false;
    if (size < 1) return false;
    if (Product::style(data, size) != product::Style::ODIMH5) return false;
    std::string ob, pr;
    Product::get_ODIMH5(data, size, ob, pr);
    if (obj.size() &&  obj != ob)  return false;
    if (prod.size() && prod != pr) return false;
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
    const types::Product* v = dynamic_cast<const types::Product*>(&o);
    if (!v) return false;
    if (v->style() != product::Style::VM2) return false;
    unsigned vi;
    v->get_VM2(vi);

    if (variable_id != -1 && (unsigned)variable_id != vi) return false;
    if (!expr.empty() &&
        std::find(idlist.begin(), idlist.end(), vi) == idlist.end())
            return false;
    return true;
}

bool MatchProductVM2::match_buffer(types::Code code, const uint8_t* data, unsigned size) const
{
    if (code != TYPE_PRODUCT) return false;
    if (size < 1) return false;
    if (Product::style(data, size) != product::Style::VM2) return false;
    unsigned vi;
    Product::get_VM2(data, size, vi);

    if (variable_id != -1 && (unsigned)variable_id != vi) return false;
    if (!expr.empty() &&
        std::find(idlist.begin(), idlist.end(), vi) == idlist.end())
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

Implementation* MatchProduct::parse(const std::string& pattern)
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
        case types::Product::Style::GRIB1: return new MatchProductGRIB1(rest);
        case types::Product::Style::GRIB2: return new MatchProductGRIB2(rest);
        case types::Product::Style::BUFR: return new MatchProductBUFR(rest);
        case types::Product::Style::ODIMH5: return new MatchProductODIMH5(rest);
        case types::Product::Style::VM2: return new MatchProductVM2(rest);
        default: throw std::invalid_argument("cannot parse type of product to match: unsupported product style: " + name);
    }
}

void MatchProduct::init()
{
    MatcherType::register_matcher("product", TYPE_PRODUCT, (MatcherType::subexpr_parser)MatchProduct::parse);
}

}
}
