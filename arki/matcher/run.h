#ifndef ARKI_MATCHER_RUN
#define ARKI_MATCHER_RUN

#include <arki/matcher/utils.h>
#include <arki/types/run.h>

namespace arki {
namespace matcher {

/**
 * Match Runs
 */
struct MatchRun : public Implementation
{
    MatchRun* clone() const override = 0;
    std::string name() const override;

    static Implementation* parse(const std::string& pattern);
    static void init();
};

struct MatchRunMinute : public MatchRun
{
    // This is -1 when it should be ignored in the match
    int minute;

    MatchRunMinute(int minute);
    MatchRunMinute(const std::string& pattern);
    MatchRunMinute* clone() const override;
    bool matchItem(const types::Type& o) const override;
    bool match_buffer(types::Code code, const uint8_t* data,
                      unsigned size) const override;
    std::string toString() const override;
};

} // namespace matcher
} // namespace arki

#endif
