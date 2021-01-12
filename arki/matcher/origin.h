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
    MatchOrigin* clone() const override = 0;
    std::string name() const override;

    static Implementation* parse(const std::string& pattern);
    static void init();
};

struct MatchOriginGRIB1 : public MatchOrigin
{
	// These are -1 when they should be ignored in the match
	int centre;
	int subcentre;
	int process;

    MatchOriginGRIB1(int centre, int subcentre, int process);
    MatchOriginGRIB1(const std::string& pattern);
    MatchOriginGRIB1* clone() const override;
    bool matchItem(const types::Type& o) const override;
    bool match_buffer(types::Code code, const uint8_t* data, unsigned size) const override;
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

    MatchOriginGRIB2(int centre, int subcentre, int processtype, int bgprocessid, int processid);
    MatchOriginGRIB2(const std::string& pattern);
    MatchOriginGRIB2* clone() const override;
    bool matchItem(const types::Type& o) const override;
    bool match_buffer(types::Code code, const uint8_t* data, unsigned size) const override;
    std::string toString() const override;
};

struct MatchOriginBUFR : public MatchOrigin
{
	// These are -1 when they should be ignored in the match
	int centre;
	int subcentre;

    MatchOriginBUFR(int centre, int subcentre);
    MatchOriginBUFR(const std::string& pattern);
    MatchOriginBUFR* clone() const override;
    bool matchItem(const types::Type& o) const override;
    bool match_buffer(types::Code code, const uint8_t* data, unsigned size) const override;
    std::string toString() const override;
};

struct MatchOriginODIMH5 : public MatchOrigin
{
	// These are -1 when they should be ignored in the match
	std::string WMO;
	std::string RAD;
	std::string PLC;

    MatchOriginODIMH5(const std::string& WMO, const std::string& RAD, const std::string& PLC);
    MatchOriginODIMH5(const std::string& pattern);
    MatchOriginODIMH5* clone() const override;
    bool matchItem(const types::Type& o) const override;
    bool match_buffer(types::Code code, const uint8_t* data, unsigned size) const override;
    std::string toString() const override;
};

}
}

// vim:set ts=4 sw=4:
#endif
