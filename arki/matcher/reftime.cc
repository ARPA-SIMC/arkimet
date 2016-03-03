#include <arki/matcher/reftime.h>
#include <arki/matcher/reftime/parser.h>
#include <arki/matcher/utils.h>
#include <arki/core/time.h>
#include <cctype>

using namespace std;
using namespace arki::types;
using namespace arki::matcher::reftime;
using arki::core::Time;

namespace arki {
namespace matcher {

MatchReftime::MatchReftime(const std::string& pattern)
{
	// TODO: error reporting needs work
	Parser p;
	p.parse(pattern);

	// Copy the results into tests
	tests = p.res;
	p.res.clear();
}

MatchReftime::~MatchReftime()
{
	for (vector<DTMatch*>::iterator i = tests.begin(); i != tests.end(); ++i)
		delete *i;
}

std::string MatchReftime::name() const { return "reftime"; }

bool MatchReftime::matchItem(const Type& o) const
{
    if (const types::reftime::Position* po = dynamic_cast<const types::reftime::Position*>(&o))
    {
        for (vector<DTMatch*>::const_iterator i = tests.begin(); i < tests.end(); ++i)
            if (!(*i)->match(po->time))
                return false;
        return true;
    }
    else if (const types::reftime::Period* pe = dynamic_cast<const types::reftime::Period*>(&o))
    {
        for (vector<DTMatch*>::const_iterator i = tests.begin(); i < tests.end(); ++i)
            if (!(*i)->match(pe->begin, pe->end))
                return false;
        return true;
    }
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

bool MatchReftime::restrict_date_range(unique_ptr<Time>& begin, unique_ptr<Time>& end) const
{
    for (vector<DTMatch*>::const_iterator i = tests.begin(); i < tests.end(); ++i)
        if (!(*i)->restrict_date_range(begin, end))
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

std::unique_ptr<MatchReftime> MatchReftime::parse(const std::string& pattern)
{
    return unique_ptr<MatchReftime>(new MatchReftime(pattern));
}


void MatchReftime::init()
{
    Matcher::register_matcher("reftime", TYPE_REFTIME, (MatcherType::subexpr_parser)MatchReftime::parse);
}

}
}
