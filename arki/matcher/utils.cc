#include "utils.h"
#include "aliases.h"
#include "reftime.h"
#include "reftime/parser.h"
#include "arki/core/binary.h"
#include "arki/types/itemset.h"
#include "arki/metadata.h"
#include "arki/types.h"
#include "arki/utils/string.h"
#include "arki/utils/regexp.h"
#include <cassert>

using namespace std;
using namespace arki::utils;

namespace arki {
namespace matcher {

// Registry of matcher information
static std::map<std::string, MatcherType*>* matchers = 0;

MatcherType::MatcherType(const std::string& name, types::Code code, subexpr_parser parse_func)
    : name(name), code(code), parse_func(parse_func)
{
    // Ensure that the map is created before we add items to it
    if (!matchers)
    {
        matchers = new map<string, MatcherType*>;
    }

    (*matchers)[name] = this;
}

MatcherType::~MatcherType()
{
    if (!matchers)
        return;
    
    matchers->erase(name);
}

MatcherType* MatcherType::find(const std::string& name)
{
    if (!matchers) return 0;
    std::map<std::string, MatcherType*>::const_iterator i = matchers->find(name);
    if (i == matchers->end()) return 0;
    return i->second;
}

std::vector<std::string> MatcherType::matcherNames()
{
    vector<string> res;
    for (std::map<std::string, MatcherType*>::const_iterator i = matchers->begin();
            i != matchers->end(); ++i)
        res.push_back(i->first);
    return res;
}

void MatcherType::register_matcher(const std::string& name, types::Code code, matcher::MatcherType::subexpr_parser parse_func)
{
    // TODO: when we are done getting rid of static constructors, reorganize
    // the code not to need this awkward new
    new matcher::MatcherType(name, code, parse_func);
}


bool Implementation::match_buffer(types::Code code, const uint8_t* data, unsigned size) const
{
    core::BinaryDecoder dec(data, size);
    // TODO: avoid a copy of the buffer if possible
    auto t = types::Type::decodeInner(code, dec);
    return matchItem(*t);
}


OR::~OR() {}

OR* OR::clone() const
{
    std::unique_ptr<OR> res(new OR(unparsed));
    for (const auto& c: components)
        // Matcher components are considered immutable, so there's no need to
        // clone them here and we can reuse them
        res->components.emplace_back(c);
    return res.release();
}

std::shared_ptr<OR> OR::merge(const OR& o) const
{
    auto res = std::make_shared<OR>("");
    for (const auto& c: components)
        res->components.emplace_back(c);
    for (const auto& c: o.components)
        res->components.emplace_back(c);
    res->unparsed = res->toStringValueOnlyExpanded();
    return res;
}

std::string OR::name() const
{
    if (components.empty()) return string();
    return components.front()->name();
}

bool OR::matchItem(const types::Type& t) const
{
    if (components.empty()) return true;

    for (auto i: components)
        if (i->matchItem(t))
            return true;
    return false;
}

bool OR::match_buffer(types::Code code, const uint8_t* data, unsigned size) const
{
    if (components.empty()) return true;

    for (auto i: components)
        if (i->match_buffer(code, data, size))
            return true;
    return false;
}

bool OR::match_interval(const core::Interval& t) const
{
    if (components.empty()) return true;

    for (auto i: components)
        if (static_pointer_cast<matcher::MatchReftime>(i)->match_interval(t))
            return true;
    return false;
}

std::string OR::toString() const
{
    if (components.empty()) return string();
    return components.front()->name() + ":" + toStringValueOnly();
}

std::string OR::toStringExpanded() const
{
    if (components.empty()) return string();
    return components.front()->name() + ":" + toStringValueOnlyExpanded();
}

std::string OR::toStringValueOnly() const
{
    if (not unparsed.empty()) return unparsed;
    return toStringValueOnlyExpanded();
}

std::string OR::toStringValueOnlyExpanded() const
{
    string res;
    for (auto i: components)
    {
        if (!res.empty()) res += " or ";
        res += i->toString();
    }
    return res;
}

bool OR::intersect_interval(core::Interval& interval) const
{
    for (auto i: components)
    {
        const matcher::MatchReftime* rt = dynamic_cast<const matcher::MatchReftime*>(i.get());
        assert(rt != nullptr);
        if (!rt->intersect_interval(interval))
            return false;
    }
    return true;
}

std::string OR::toReftimeSQL(const std::string& column) const
{
    if (components.empty())
        return std::string();

    if (components.size() == 1)
    {
        const matcher::MatchReftime* mr = dynamic_cast<const matcher::MatchReftime*>(components[0].get());
        return mr->sql(column);
    } else {
        string res = "(";
        bool first = true;

        for (const auto& i: components)
        {
            const matcher::MatchReftime* mr = dynamic_cast<const matcher::MatchReftime*>(i.get());
            if (!mr) throw std::runtime_error("arkimet bug: toReftimeSQL called on non-reftime matchers");
            if (first)
                first = false;
            else
                res += " OR ";
            res += mr->sql(column);
        }

        res += ")";
        return res;
    }
}

unique_ptr<OR> OR::parse(const Aliases* aliases, const MatcherType& mt, const std::string& pattern)
{
    std::unique_ptr<OR> res(new OR(pattern));

    // Split 'patterns' on /\s*or\s*/i
    Splitter splitter("[ \t]+or[ \t]+", REG_EXTENDED | REG_ICASE);

    for (Splitter::const_iterator i = splitter.begin(pattern); i != splitter.end(); ++i)
    {
        if (shared_ptr<OR> exprs = aliases ? aliases->get(str::lower(*i)) : nullptr)
            for (auto j: exprs->components)
                res->components.push_back(j);
        else
            res->components.emplace_back(mt.parse_func(*i));
    }

    return res;
}

std::unique_ptr<OR> OR::wrap(std::unique_ptr<Implementation> impl)
{
    std::unique_ptr<OR> res(new OR(std::string()));
    res->components.emplace_back(std::move(impl));
    return res;
}


AND::~AND() {}

AND* AND::clone() const
{
    std::unique_ptr<AND> res(new AND);
    for (const auto& c: components)
        res->components.emplace(std::make_pair(c.first, std::shared_ptr<OR>(c.second->clone())));
    return res.release();
}

std::string AND::name() const
{
    return "matcher";
}

void AND::merge(const AND& o)
{
    auto i1 = components.begin();
    auto i2 = o.components.begin();
    while (i1 != components.end() || i2 != o.components.end())
    {
        if (i2 == o.components.end() || (i1 != components.end() && i1->first < i2->first)) {
            // The other doesn't match this typecode: remove it on this side
            auto o = i1;
            ++i1;
            components.erase(o);
        } else if (i1 == components.end() || (i2 != o.components.end() && i1->first > i2->first)) {
            // Skip this typecode: we match everything already
            ++i2;
        } else {
            // Merge ORs
            i1->second = i1->second->merge(*i2->second);
            ++i1;
            ++i2;
        }
    }
}

bool AND::matchItem(const types::Type& t) const
{
    if (empty()) return true;

    auto i = components.find(t.type_code());
    if (i == components.end()) return true;

    return i->second->matchItem(t);
}

bool AND::match_buffer(types::Code code, const uint8_t* data, unsigned size) const
{
    if (empty()) return true;

    auto i = components.find(code);
    if (i == components.end()) return true;

    return i->second->match_buffer(code, data, size);
}


template<typename COLL>
static bool mdmatch(const Implementation& matcher, const COLL& c)
{
	for (typename COLL::const_iterator i = c.begin(); i != c.end(); ++i)
		if (matcher.matchItem(*i))
			return true;
	return false;
}

bool AND::matchItemSet(const types::ItemSet& md) const
{
    if (empty()) return true;

    for (const auto& i: components)
    {
        if (!i.second) return false;
        const types::Type* item = md.get(i.first);
        if (!item) return false;
        if (!i.second->matchItem(*item)) return false;
    }
    return true;
}

bool AND::matchMetadata(const Metadata& md) const
{
    if (empty()) return true;

    for (const auto& i: components)
    {
        if (!i.second) return false;
        const types::Type* item = md.get(i.first);
        if (!item) return false;
        if (!i.second->matchItem(*item)) return false;
    }
    return true;
}

bool AND::match_interval(const core::Interval& interval) const
{
    if (empty()) return true;

    auto i = components.find(TYPE_REFTIME);
    if (i == components.end()) return true;

    return i->second->match_interval(interval);
}

shared_ptr<OR> AND::get(types::Code code) const
{
    auto i = components.find(code);
    if (i == components.end()) return nullptr;
    return i->second;
}

void AND::foreach_type(std::function<void(types::Code, const OR&)> dest) const
{
    for (const auto& i: components)
        dest(i.first, *i.second);
}

std::string AND::toString() const
{
    if (components.empty()) return string();

    std::string res;
    for (const auto& i: components)
    {
        if (!res.empty()) res += "; ";
        res += i.second->toString();
    }
    return res;
}

std::string AND::toStringExpanded() const
{
    if (components.empty()) return string();

    std::string res;
    for (const auto& i: components)
    {
        if (!res.empty()) res += "; ";
        res += i.second->toStringExpanded();
    }
    return res;
}

void AND::split(const std::set<types::Code>& codes, AND& with, AND& without) const
{
    for (const auto& i: components)
    {
        if (codes.find(i.first) != codes.end())
        {
            with.components.insert(i);
        } else {
            without.components.insert(i);
        }
    }
}

unique_ptr<AND> AND::parse(const AliasDatabase& aliases, const std::string& pattern)
{
    unique_ptr<AND> res(new AND);

    // Split on newlines or semicolons
    Tokenizer tok(pattern, "[^\n;]+", REG_EXTENDED);

    for (Tokenizer::const_iterator i = tok.begin(); i != tok.end(); ++i)
    {
        string expr = str::strip(*i);
        if (expr.empty()) continue;
        size_t pos = expr.find(':');
        if (pos == string::npos)
            throw std::invalid_argument("cannot parse matcher subexpression '" + expr + "' does not contain a colon (':')");

        std::string type = str::lower(str::strip(expr.substr(0, pos)));
        std::string patterns = str::strip(expr.substr(pos+1));

        MatcherType* mt = MatcherType::find(type);
        if (mt == 0)
            throw std::invalid_argument("cannot parse matcher subexpression: unknown match type: '" + type + "'");

        res->components.insert(make_pair(mt->code, OR::parse(aliases.get(type), *mt, patterns)));
    }

    return res;
}

std::unique_ptr<AND> AND::for_interval(const core::Interval& interval)
{
    std::unique_ptr<matcher::MatchReftime> reftime(new matcher::MatchReftime);
    reftime->tests.push_back(matcher::reftime::DTMatch::createInterval(interval));

    std::unique_ptr<AND> res(new AND);
    res->components.insert(make_pair(TYPE_REFTIME, OR::wrap(std::move(reftime))));

    return res;
}


OptionalCommaList::OptionalCommaList(const std::string& pattern, bool has_tail)
{
    std::string p;
	if (has_tail)
	{
		size_t pos = pattern.find(":");
        if (pos == std::string::npos)
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

Optional<uint32_t> OptionalCommaList::getUnsignedWithMissing(size_t pos, uint32_t missing) const
{
    if (!has(pos))
        return Optional<uint32_t>();

    if ((*this)[pos] == "-")
        return missing;
    else
        return strtoul((*this)[pos].c_str(), 0, 10);
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
