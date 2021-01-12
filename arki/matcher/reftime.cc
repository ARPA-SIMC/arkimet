#include "reftime.h"
#include "reftime/parser.h"
#include "arki/core/time.h"
#include "arki/types/reftime.h"

using namespace std;
using namespace arki::types;
using namespace arki::matcher::reftime;
using arki::core::Time;

namespace arki {
namespace matcher {

MatchReftime::MatchReftime()
{
}

MatchReftime::MatchReftime(const std::string& pattern)
{
    // TODO: error reporting needs work
    reftime::Parser p;
    p.parse(pattern);

    // Copy the results into tests
    tests = p.res;
    p.res.clear();
}

MatchReftime::~MatchReftime()
{
    for (auto& i: tests)
        delete i;
}

MatchReftime* MatchReftime::clone() const
{
    std::unique_ptr<MatchReftime> res(new MatchReftime);
    for (const auto* d: tests)
        res->tests.push_back(d->clone());
    return res.release();
}

std::string MatchReftime::name() const { return "reftime"; }

bool MatchReftime::matchItem(const Type& o) const
{
    if (const types::reftime::Position* po = dynamic_cast<const types::reftime::Position*>(&o))
    {
        core::Time t = po->get_Position();
        for (const auto& i: tests)
            if (!i->match(t))
                return false;
        return true;
    }
    return false;
}

bool MatchReftime::match_buffer(types::Code code, const uint8_t* data, unsigned size) const
{
    if (code != TYPE_REFTIME) return false;
    if (size < 1) return false;
    if (types::Reftime::style(data, size) != types::reftime::Style::POSITION) return false;
    core::Time t = Reftime::get_Position(data, size);
    for (const auto& i: tests)
        if (!i->match(t))
            return false;
    return true;
}

bool MatchReftime::match_interval(const core::Interval& o) const
{
    for (const auto& i: tests)
        if (!i->match(o))
            return false;
    return true;
}

std::string MatchReftime::sql(const std::string& column) const
{
	bool first = true;
	string res = "(";
	for (vector<DTMatch*>::const_iterator i = tests.begin(); i < tests.end(); ++i)
	{
		if (first)
			first = false;
		else
			res += " AND ";
		res += (*i)->sql(column);
	}
	return res + ")";
}

bool MatchReftime::intersect_interval(core::Interval& interval) const
{
    for (const auto& i: tests)
        if (!i->intersect_interval(interval))
            return false;
    return true;
}

std::string MatchReftime::toString() const
{
	string res;
	for (vector<DTMatch*>::const_iterator i = tests.begin(); i < tests.end(); ++i)
	{
        if (!res.empty())
        {
           if ((*i)->isLead())
               res += ",";
           else
               res += " ";
        }
        res += (*i)->toString();
	}
	return res;
}

Implementation* MatchReftime::parse(const std::string& pattern)
{
    return new MatchReftime(pattern);
}


void MatchReftime::init()
{
    MatcherType::register_matcher("reftime", TYPE_REFTIME, (MatcherType::subexpr_parser)MatchReftime::parse);
}

}
}
