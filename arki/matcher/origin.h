#ifndef ARKI_MATCHER_ORIGIN
#define ARKI_MATCHER_ORIGIN

#include <arki/matcher.h>
#include <arki/matcher/utils.h>
#include <arki/types/origin.h>

namespace arki {
namespace matcher {

/**
 * Match Origins
 */
struct MatchOrigin : public Implementation
{
    std::string name() const override;

    static std::unique_ptr<MatchOrigin> parse(const std::string& pattern);
    static void init();
};

struct MatchOriginGRIB1 : public MatchOrigin
{
	// These are -1 when they should be ignored in the match
	int centre;
	int subcentre;
	int process;

    MatchOriginGRIB1(const std::string& pattern);
    bool matchItem(const types::Type& o) const override;
    std::string toString() const override;
};

struct MatchOriginGRIB2 : public MatchOrigin
{
	// These are -1 when they should be ignored in the match
	int centre;
	int subcentre;
	int processtype;
	int bgprocessid;
	int processid;

    MatchOriginGRIB2(const std::string& pattern);
    bool matchItem(const types::Type& o) const override;
    std::string toString() const override;
};

struct MatchOriginBUFR : public MatchOrigin
{
	// These are -1 when they should be ignored in the match
	int centre;
	int subcentre;

    MatchOriginBUFR(const std::string& pattern);
    bool matchItem(const types::Type& o) const override;
    std::string toString() const override;
};

struct MatchOriginODIMH5 : public MatchOrigin
{
	// These are -1 when they should be ignored in the match
	std::string WMO;
	std::string RAD;
	std::string PLC;

    MatchOriginODIMH5(const std::string& pattern);
    bool matchItem(const types::Type& o) const override;
    std::string toString() const override;
};

}
}

// vim:set ts=4 sw=4:
#endif
