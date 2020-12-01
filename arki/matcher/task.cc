#include "config.h"

#include "task.h"
#include "arki/utils/string.h"

using namespace std;
using namespace arki::types;
using namespace arki::utils;

namespace arki {
namespace matcher {

/*============================================================================*/

std::string MatchTask::name() const { return "task"; }

MatchTask::MatchTask(const std::string& pattern)
{
    OptionalCommaList args(pattern);
    task = str::upper(args.getString(0, ""));
}

bool MatchTask::matchItem(const Type& o) const
{
    const types::Task* v = dynamic_cast<const types::Task*>(&o);
    if (!v) return false;
    if (task.size())
    {
        std::string utask = str::upper(v->get());
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

Implementation* MatchTask::parse(const std::string& pattern)
{
    return new MatchTask(pattern);
}

void MatchTask::init()
{
    MatcherType::register_matcher("task", TYPE_TASK, (MatcherType::subexpr_parser)MatchTask::parse);
}

}
}
