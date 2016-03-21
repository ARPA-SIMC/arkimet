#ifndef ARKI_MATCHER_TASK
#define ARKI_MATCHER_TASK

/// Radar task matcher
#include <arki/matcher.h>
#include <arki/types/task.h>

namespace arki {
namespace matcher {

/**
 * Match tasks
 */
struct MatchTask : public Implementation
{
    std::string name() const override;

    std::string task;

    MatchTask(const std::string& pattern);
    bool matchItem(const types::Type& o) const override;
    std::string toString() const override;

    static std::unique_ptr<MatchTask> parse(const std::string& pattern);
    static void init();
};

}
}
#endif
