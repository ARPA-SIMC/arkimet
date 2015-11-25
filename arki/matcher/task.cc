#include "config.h"

#include <arki/matcher/task.h>
#include <arki/matcher/utils.h>
#include <arki/metadata.h>

#include <arki/utils/string.h>

using namespace std;
using namespace arki::types;

namespace arki {
namespace matcher {

/*============================================================================*/

std::string MatchTask::name() const { return "task"; }

MatchTask::MatchTask(const std::string& pattern)
{
	OptionalCommaList args(pattern);
	task = wibble::str::toupper(args.getString(0, ""));
}

bool MatchTask::matchItem(const Type& o) const
{
    const types::Task* v = dynamic_cast<const types::Task*>(&o);
    if (!v) return false;
	if (task.size())
	{
		std::string utask = wibble::str::toupper(v->task);
		if (utask.find(task) == std::string::npos)
			return false;
	}
	return true;
}

std::string MatchTask::toString() const
{
    CommaJoiner res;
    if (task.size()) res.add(task); else res.addUndef();
    return res.join();
}

MatchTask* MatchTask::parse(const std::string& pattern)
{
	return new MatchTask(pattern);
}

void MatchTask::init()
{
    Matcher::register_matcher("task", types::TYPE_TASK, (MatcherType::subexpr_parser)MatchTask::parse);
}

}
}

// vim:set ts=4 sw=4:
