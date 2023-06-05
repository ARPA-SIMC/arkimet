#ifndef ARKI_MATCHER_LEVEL
#define ARKI_MATCHER_LEVEL

#include <arki/matcher/utils.h>
#include <cstdint>

namespace arki {
namespace matcher {

/**
 * Match Levels
 */
struct MatchLevel : public Implementation
{
    MatchLevel* clone() const override = 0;
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

    MatchLevelGRIB1(int type, int l1, int l2);
    MatchLevelGRIB1(const std::string& pattern);
    MatchLevelGRIB1* clone() const override;
    bool match_data(unsigned vtype, unsigned vl1, unsigned vl2) const;
    bool matchItem(const types::Type& o) const override;
    bool match_buffer(types::Code code, const uint8_t* data, unsigned size) const override;
    std::string toString() const override;
};

struct MatchLevelGRIB2S : public MatchLevel
{
    Optional<uint8_t> type;
    Optional<uint8_t> scale;
    Optional<uint32_t> value;

    MatchLevelGRIB2S(const Optional<uint8_t>& type, const Optional<uint8_t>& scale, const Optional<uint32_t>& value);
    MatchLevelGRIB2S(const std::string& pattern);
    MatchLevelGRIB2S* clone() const override;
    bool matchItem(const types::Type& o) const override;
    bool match_buffer(types::Code code, const uint8_t* data, unsigned size) const override;
    std::string toString() const override;
};

struct MatchLevelGRIB2D : public MatchLevel
{
    Optional<uint8_t> type1;
    Optional<uint8_t> scale1;
    Optional<uint32_t> value1;
    Optional<uint8_t> type2;
    Optional<uint8_t> scale2;
    Optional<uint32_t> value2;

    MatchLevelGRIB2D(
            const Optional<uint8_t>& type1, const Optional<uint8_t>& scale1, const Optional<uint32_t>& value1,
            const Optional<uint8_t>& type2, const Optional<uint8_t>& scale2, const Optional<uint32_t>& value2);
    MatchLevelGRIB2D(const std::string& pattern);
    MatchLevelGRIB2D* clone() const override;
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

    MatchLevelODIMH5(const std::vector<double>& vals, double vals_offset, double range_min, double range_max);
    MatchLevelODIMH5(const std::string& pattern);
    MatchLevelODIMH5* clone() const override;
    bool match_data(double vmin, double vmax) const;
    bool matchItem(const types::Type& o) const override;
    bool match_buffer(types::Code code, const uint8_t* data, unsigned size) const override;
    std::string toString() const override;
};


}
}
#endif
