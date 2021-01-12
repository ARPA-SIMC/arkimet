#ifndef ARKI_MATCHER_TASK
#define ARKI_MATCHER_TASK

/// Radar task matcher
#include <arki/matcher/utils.h>

namespace arki {
namespace matcher {

/**
 * Match tasks
 */
struct MatchTask : public Implementation
{
    std::string task;

    MatchTask(const MatchTask& o);
    MatchTask(const std::string& pattern);
    MatchTask* clone() const override;
    std::string name() const override;

    bool matchItem(const types::Type& o) const override;
    std::string toString() const override;

    static Implementation* parse(const std::string& pattern);
    static void init();
};

}
}
#endif
