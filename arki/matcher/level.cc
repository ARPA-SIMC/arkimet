#include "level.h"
#include "arki/types/level.h"
#include "arki/utils/string.h"
#include <stdexcept>
#include <sstream>
#include <vector>
#include <string>

using namespace std;
using namespace arki::types;
using namespace arki::utils;

namespace arki {
namespace matcher {

std::string MatchLevel::name() const { return "level"; }

MatchLevelGRIB1::MatchLevelGRIB1(const std::string& pattern)
{
	OptionalCommaList args(pattern);
	type = args.getInt(0, -1);
	l1 = args.getInt(1, -1);
	l2 = args.getInt(2, -1);
}

bool MatchLevelGRIB1::match_data(unsigned vtype, unsigned vl1, unsigned vl2) const
{
    if (type != -1 && (unsigned)type != vtype) return false;
    switch (Level::GRIB1_type_vals(vtype))
    {
        case 0:
            if (l1 >= 0) return false;
            if (l2 >= 0) return false;
            break;
        case 1:
            if (l1 >= 0 && (unsigned)l1 != vl1) return false;
            if (l2 >= 0) return false;
            break;
        case 2:
            if (l1 >= 0 && (unsigned)l1 != vl1) return false;
            if (l2 >= 0 && (unsigned)l2 != vl2) return false;
            break;
    }
    return true;
}

bool MatchLevelGRIB1::matchItem(const Type& o) const
{
    const types::Level* v = dynamic_cast<const types::Level*>(&o);
    if (!v) return false;
    if (v->style() != level::Style::GRIB1) return false;
    unsigned vtype, vl1, vl2;
    v->get_GRIB1(vtype, vl1, vl2);
    return match_data(vtype, vl1, vl2);
}

bool MatchLevelGRIB1::match_buffer(types::Code code, const uint8_t* data, unsigned size) const
{
    if (code != TYPE_LEVEL) return false;
    if (size < 1) return false;
    if (Level::style(data, size) != level::Style::GRIB1) return false;
    unsigned vtype, vl1, vl2;
    Level::get_GRIB1(data, size, vtype, vl1, vl2);
    return match_data(vtype, vl1, vl2);
}


std::string MatchLevelGRIB1::toString() const
{
	CommaJoiner res;
	res.add("GRIB1");
	if (type != -1) res.add(type); else res.addUndef();
	if (l1 != -1) res.add(l1); else res.addUndef();
	if (l2 != -1) res.add(l2); else res.addUndef();
	return res.join();
}


MatchLevelGRIB2S::MatchLevelGRIB2S(const std::string& pattern)
{
    OptionalCommaList args(pattern);
    type = args.getUnsignedWithMissing(0, Level::GRIB2_MISSING_TYPE, has_type);
    scale = args.getUnsignedWithMissing(1, Level::GRIB2_MISSING_SCALE, has_scale);
    value = args.getUnsignedWithMissing(2, Level::GRIB2_MISSING_VALUE, has_value);
}

bool MatchLevelGRIB2S::matchItem(const Type& o) const
{
    const types::Level* v = dynamic_cast<const types::Level*>(&o);
    if (!v) return false;
    if (v->style() != level::Style::GRIB2S) return false;
    unsigned ty, sc, va;
    v->get_GRIB2S(ty, sc, va);
    if (has_type && type != ty) return false;
    if (has_scale && scale != sc) return false;
    if (has_value && value != va) return false;
    return true;
}

bool MatchLevelGRIB2S::match_buffer(types::Code code, const uint8_t* data, unsigned size) const
{
    if (code != TYPE_LEVEL) return false;
    if (size < 1) return false;
    if (Level::style(data, size) != level::Style::GRIB2S) return false;
    unsigned ty, sc, va;
    Level::get_GRIB2S(data, size, ty, sc, va);
    if (has_type && type != ty) return false;
    if (has_scale && scale != sc) return false;
    if (has_value && value != va) return false;
    return true;
}

std::string MatchLevelGRIB2S::toString() const
{
    CommaJoiner res;
    res.add("GRIB2S");
    if (has_type) res.add((unsigned)type, (unsigned)Level::GRIB2_MISSING_TYPE); else res.addUndef();
    if (has_scale) res.add((unsigned)scale, (unsigned)Level::GRIB2_MISSING_SCALE); else res.addUndef();
    if (has_value) res.add(value, Level::GRIB2_MISSING_VALUE); else res.addUndef();
    return res.join();
}


MatchLevelGRIB2D::MatchLevelGRIB2D(const std::string& pattern)
{
    OptionalCommaList args(pattern);
    type1 = args.getUnsignedWithMissing(0, Level::GRIB2_MISSING_TYPE, has_type1);
    scale1 = args.getUnsignedWithMissing(1, Level::GRIB2_MISSING_SCALE, has_scale1);
    value1 = args.getUnsignedWithMissing(2, Level::GRIB2_MISSING_VALUE, has_value1);
    type2 = args.getUnsignedWithMissing(3, Level::GRIB2_MISSING_TYPE, has_type2);
    scale2 = args.getUnsignedWithMissing(4, Level::GRIB2_MISSING_SCALE, has_scale2);
    value2 = args.getUnsignedWithMissing(5, Level::GRIB2_MISSING_VALUE, has_value2);
}

bool MatchLevelGRIB2D::matchItem(const Type& o) const
{
    const types::Level* v = dynamic_cast<const types::Level*>(&o);
    if (!v) return false;
    if (v->style() != level::Style::GRIB2D) return false;
    unsigned ty1, sc1, va1, ty2, sc2, va2;
    v->get_GRIB2D(ty1, sc1, va1, ty2, sc2, va2);
    if (has_type1 && type1 != ty1) return false;
    if (has_scale1 && scale1 != sc1) return false;
    if (has_value1 && value1 != va1) return false;
    if (has_type2 && type2 != ty2) return false;
    if (has_scale2 && scale2 != sc2) return false;
    if (has_value2 && value2 != va2) return false;
    return true;
}

bool MatchLevelGRIB2D::match_buffer(types::Code code, const uint8_t* data, unsigned size) const
{
    if (code != TYPE_LEVEL) return false;
    if (size < 1) return false;
    if (Level::style(data, size) != level::Style::GRIB2D) return false;
    unsigned ty1, sc1, va1, ty2, sc2, va2;
    Level::get_GRIB2D(data, size, ty1, sc1, va1, ty2, sc2, va2);
    if (has_type1 && type1 != ty1) return false;
    if (has_scale1 && scale1 != sc1) return false;
    if (has_value1 && value1 != va1) return false;
    if (has_type2 && type2 != ty2) return false;
    if (has_scale2 && scale2 != sc2) return false;
    if (has_value2 && value2 != va2) return false;
    return true;
}

std::string MatchLevelGRIB2D::toString() const
{
    CommaJoiner res;
    res.add("GRIB2D");
    if (has_type1) res.add((unsigned)type1, (unsigned)Level::GRIB2_MISSING_TYPE); else res.addUndef();
    if (has_scale1) res.add((unsigned)scale1, (unsigned)Level::GRIB2_MISSING_SCALE); else res.addUndef();
    if (has_value1) res.add(value1, Level::GRIB2_MISSING_VALUE); else res.addUndef();
    if (has_type2) res.add((unsigned)type2, (unsigned)Level::GRIB2_MISSING_TYPE); else res.addUndef();
    if (has_scale2) res.add((unsigned)scale2, (unsigned)Level::GRIB2_MISSING_SCALE); else res.addUndef();
    if (has_value2) res.add(value2, Level::GRIB2_MISSING_VALUE); else res.addUndef();
    return res.join();
}

static void split(const std::string& str, std::vector<std::string>& result)
{
	const std::string& delimiters = " ,";

	std::string::size_type lastPos = str.find_first_not_of(delimiters, 0);	// Skip delimiters at beginning.
	std::string::size_type pos     = str.find_first_of(delimiters, lastPos);	// Find first "non-delimiter".
	while (std::string::npos != pos || std::string::npos != lastPos)
	{
		std::string val = str.substr(lastPos, pos - lastPos);
		if (!val.empty())
			result.push_back(val);
		lastPos = str.find_first_not_of(delimiters, pos);	// Skip delimiters.  Note the "not_of"
		pos = str.find_first_of(delimiters, lastPos);		// Find next "non-delimiter"
	}
}

static void order(double& a, double& b)
{
	if (a > b)
	{
		double t = a;
		a = b;
		b = t;
	}
}

static bool outside(double minA, double maxA, double minB, double maxB)
{
	if (minA > maxB) return true;
	if (maxA < minB) return true;
	return false;
}

static double parsedouble(const std::string& val)
{
	double result;
	std::istringstream ss(val);
	ss >> result;
	if (ss.fail())
		throw std::logic_error(val + "is not a double value");
	return result;
}


MatchLevelODIMH5::MatchLevelODIMH5(const std::string& pattern)
{
	vals.clear();
	vals_offset 	= 0;
	range_min 	= -360.;
	range_max 	= 360.;

	std::vector<std::string> tokens;

	split(pattern, tokens);

	if (tokens.empty())
		return;

	if (tokens[0] == "range")
	{
		if (tokens.size() != 3)
			throw std::logic_error("'" + pattern + "' is not a valid pattern");
		range_min = parsedouble(tokens[1]);
		range_max = parsedouble(tokens[2]);

		order(range_min, range_max);
	}
	else
	{
		for (size_t i=0; i<tokens.size(); i++)
		{
			if (tokens[i] == "offset")
			{
				i++;
				if (i < tokens.size())
					vals_offset = parsedouble(tokens[i]);
			}
			else
			{
				vals.push_back(parsedouble(tokens[i]));
			}
		}
	}
}

bool MatchLevelODIMH5::match_data(double vmin, double vmax) const
{
    if (vals.size())
    {
        for (size_t i=0; i < vals.size(); i++)
        {
            double min = vals[i] - vals_offset;
            double max = vals[i] + vals_offset;
            order(min, max);
            if (!outside(vmin, vmax, min, max))
                return true;
        }
    }
    else
    {
        if (!outside(vmin, vmax, range_min, range_max))
            return true;
    }

    return false;
}

bool MatchLevelODIMH5::matchItem(const Type& o) const
{
    const types::Level* v = dynamic_cast<const types::Level*>(&o);
    if (!v) return false;
    if (v->style() != level::Style::ODIMH5) return false;
    double vmin, vmax;
    v->get_ODIMH5(vmin, vmax);
    return match_data(vmin, vmax);
}

bool MatchLevelODIMH5::match_buffer(types::Code code, const uint8_t* data, unsigned size) const
{
    if (code != TYPE_LEVEL) return false;
    if (size < 1) return false;
    if (Level::style(data, size) != level::Style::ODIMH5) return false;
    double vmin, vmax;
    Level::get_ODIMH5(data, size, vmin, vmax);
    return match_data(vmin, vmax);
}

std::string MatchLevelODIMH5::toString() const
{
	std::ostringstream ss;

	if (vals.size())
	{
		ss << "ODIMH5,";
		for (size_t i=0; i<vals.size(); i++)
		{
			if (i) ss << " ";
			ss << vals[i];
		}
		if (vals_offset)
			ss << " offset " << vals_offset;
	}
	else
	{
		ss << "ODIMH5,range " << range_min << " " << range_max;
	}

	return ss.str();
}

Implementation* MatchLevel::parse(const std::string& pattern)
{
    size_t beg = 0;
    size_t pos = pattern.find(',', beg);
    string name;
    string rest;
    if (pos == string::npos)
        name = str::strip(pattern.substr(beg));
    else {
        name = str::strip(pattern.substr(beg, pos-beg));
        rest = pattern.substr(pos+1);
    }
    switch (types::Level::parseStyle(name))
    {
        case types::Level::Style::GRIB1: return new MatchLevelGRIB1(rest);
        case types::Level::Style::GRIB2S: return new MatchLevelGRIB2S(rest);
        case types::Level::Style::GRIB2D: return new MatchLevelGRIB2D(rest);
        case types::Level::Style::ODIMH5: return new MatchLevelODIMH5(rest);
        default: throw std::invalid_argument("cannot parse type of level to match:  unsupported level style: " + name);
    }
}

void MatchLevel::init()
{
    MatcherType::register_matcher("level", TYPE_LEVEL, (MatcherType::subexpr_parser)MatchLevel::parse);
}

}
}
