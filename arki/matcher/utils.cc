#include "config.h"
#include "utils.h"

using namespace std;
using namespace arki::utils;

namespace arki {
namespace matcher {

OptionalCommaList::OptionalCommaList(const std::string& pattern, bool has_tail)
{
	string p;
	if (has_tail)
	{
		size_t pos = pattern.find(":");
		if (pos == string::npos)
			p = pattern;
        else
        {
            p = str::strip(pattern.substr(0, pos));
            tail = str::strip(pattern.substr(pos+1));
        }
    } else
        p = pattern;
    str::Split split(p, ",");
    for (str::Split::const_iterator i = split.begin(); i != split.end(); ++i)
        push_back(*i);
    while (!empty() && (*this)[size() - 1].empty())
        resize(size() - 1);
}

bool OptionalCommaList::has(size_t pos) const
{
	if (pos >= size()) return false;
	if ((*this)[pos].empty()) return false;
	return true;
}

int OptionalCommaList::getInt(size_t pos, int def) const
{
    if (!has(pos)) return def;
    const char* beg = (*this)[pos].c_str();
    char* end;
    unsigned long res = strtoul(beg, &end, 10);
    if ((unsigned)(end-beg) < (*this)[pos].size())
    {
        stringstream ss;
        ss << "cannot parse matcher: '" << (*this)[pos] << "' is not a number";
        throw std::invalid_argument(ss.str());
    }
    return res;
}

unsigned OptionalCommaList::getUnsigned(size_t pos, unsigned def) const
{
	if (!has(pos)) return def;
	return strtoul((*this)[pos].c_str(), 0, 10);
}

uint32_t OptionalCommaList::getUnsignedWithMissing(size_t pos, uint32_t missing, bool& has_val) const
{
    if ((has_val = has(pos)))
    {
        if ((*this)[pos] == "-")
            return missing;
        else
            return strtoul((*this)[pos].c_str(), 0, 10);
    }
    return missing;
}

double OptionalCommaList::getDouble(size_t pos, double def) const
{
	if (!has(pos)) return def;
	return strtod((*this)[pos].c_str(), 0);
}

const std::string& OptionalCommaList::getString(size_t pos, const std::string& def) const
{
	if (!has(pos)) return def;
	return (*this)[pos];
}


#if 0
	bool matchInt(size_t pos, int val) const
	{
		if (pos >= size()) return true;
		if ((*this)[pos].empty()) return true;
		int req = strtoul((*this)[pos].c_str(), 0, 10);
		return req == val;
	}

	bool matchDouble(size_t pos, double val) const
	{
		if (pos >= size()) return true;
		if ((*this)[pos].empty()) return true;
		int req = strtod((*this)[pos].c_str(), 0);
		return req == val;
	}

	std::string toString() const
	{
		string res;
		for (vector<string>::const_iterator i = begin(); i != end(); ++i)
			if (res.empty())
				res += *i;
			else
				res += "," + *i;
		return res;
	}

	template<typename V>
	bool matchVals(const V* vals, size_t size) const
	{
		for (size_t i = 0; i < size; ++i)
			if (this->size() > i && !(*this)[i].empty() && (V)strtoul((*this)[i].c_str(), 0, 10) != vals[i])
				return false;
		return true;
	}

	std::string make_sql(const std::string& column, const std::string& style, size_t maxvals) const
	{
		string res = column + " LIKE '" + style;
		for (vector<string>::const_iterator i = begin(); i != end(); ++i)
		{
			res += ',';
			if (i->empty())
				res += '%';
			else
				res += *i;
		}
		if (size() < maxvals)
			res += ",%";
		return res + '\'';
	}

	std::vector< std::pair<int, bool> > getIntVector() const
	{
		vector< pair<int, bool> > res;

		for (vector<string>::const_iterator i = begin(); i != end(); ++i)
			if (i->empty())
				res.push_back(make_pair(0, false));
			else
				res.push_back(make_pair(strtoul(i->c_str(), 0, 10), true));

		return res;
	}
#endif

}
}
