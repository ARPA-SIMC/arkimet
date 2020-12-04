#ifndef ARKI_MATCHER_LEVEL
#define ARKI_MATCHER_LEVEL

#include <arki/matcher.h>
#include <arki/matcher/utils.h>
#include <arki/types/level.h>

namespace arki {
namespace matcher {

/**
 * Match Levels
 */
struct MatchLevel : public Implementation
{
    std::string name() const;

    static Implementation* parse(const std::string& pattern);
    static void init();
};

struct MatchLevelGRIB1 : public MatchLevel
{
	// This is -1 when it should be ignored in the match
	int type;
	int l1;
	int l2;

    MatchLevelGRIB1(const std::string& pattern);
    bool match_data(unsigned vtype, unsigned vl1, unsigned vl2) const;
    bool matchItem(const types::Type& o) const override;
    bool match_buffer(types::Code code, const uint8_t* data, unsigned size) const override;
    std::string toString() const override;
};

struct MatchLevelGRIB2S : public MatchLevel
{
    uint8_t type;
    bool has_type;
    uint8_t scale;
    bool has_scale;
    uint32_t value;
    bool has_value;

    MatchLevelGRIB2S(const std::string& pattern);
    bool matchItem(const types::Type& o) const override;
    bool match_buffer(types::Code code, const uint8_t* data, unsigned size) const override;
    std::string toString() const override;
};

struct MatchLevelGRIB2D : public MatchLevel
{
    uint8_t type1;
    bool has_type1;
    uint8_t scale1;
    bool has_scale1;
    uint32_t value1;
    bool has_value1;
    uint8_t type2;
    bool has_type2;
    uint8_t scale2;
    bool has_scale2;
    uint32_t value2;
    bool has_value2;

    MatchLevelGRIB2D(const std::string& pattern);
    bool matchItem(const types::Type& o) const override;
    bool match_buffer(types::Code code, const uint8_t* data, unsigned size) const override;
    std::string toString() const override;
};

struct MatchLevelODIMH5 : public MatchLevel
{
	std::vector<double> vals;
	double vals_offset;

	double range_min;
	double range_max;

    MatchLevelODIMH5(const std::string& pattern);
    bool match_data(double vmin, double vmax) const;
    bool matchItem(const types::Type& o) const override;
    bool match_buffer(types::Code code, const uint8_t* data, unsigned size) const override;
    std::string toString() const override;
};


}
}

// vim:set ts=4 sw=4:
#endif
