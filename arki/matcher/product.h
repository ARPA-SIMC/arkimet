#ifndef ARKI_MATCHER_PRODUCT
#define ARKI_MATCHER_PRODUCT

#include <arki/matcher.h>
#include <arki/matcher/utils.h>
#include <arki/types/values.h>

namespace arki {
namespace matcher {

/**
 * Match Products
 */
struct MatchProduct : public Implementation
{
    std::string name() const override;

    static Implementation* parse(const std::string& pattern);
    static void init();
};

struct MatchProductGRIB1 : public MatchProduct
{
	// These are -1 when they should be ignored in the match
	int origin;
	int table;
	int product;

    MatchProductGRIB1(const std::string& pattern);
    bool matchItem(const types::Type& o) const override;
    bool match_buffer(types::Code code, const uint8_t* data, unsigned size) const override;
    std::string toString() const override;
};

struct MatchProductGRIB2 : public MatchProduct
{
	// These are -1 when they should be ignored in the match
	int centre;
	int discipline;
	int category;
	int number;
    int table_version;
    int local_table_version;

    MatchProductGRIB2(const std::string& pattern);
    bool matchItem(const types::Type& o) const override;
    bool match_buffer(types::Code code, const uint8_t* data, unsigned size) const override;
    std::string toString() const override;
};

struct MatchProductBUFR : public MatchProduct
{
    // These are -1 when they should be ignored in the match
    int type;
    int subtype;
    int localsubtype;
    types::ValueBagMatcher values;

    MatchProductBUFR(const std::string& pattern);
    bool matchItem(const types::Type& o) const override;
    bool match_buffer(types::Code code, const uint8_t* data, unsigned size) const override;
    std::string toString() const override;
};

struct MatchProductODIMH5 : public MatchProduct
{
	std::string 	obj;		// '' when should be ignored in the match
	std::string 	prod;		// '' when should be ignored in the match
	/*REMOVED:double		prodpar1;	// NAN when should be ignored in the match */
	/*REMOVED:double		prodpar2;	// NAN when should be ignored in the match */

    MatchProductODIMH5(const std::string& pattern);
    bool matchItem(const types::Type& o) const override;
    bool match_buffer(types::Code code, const uint8_t* data, unsigned size) const override;
    std::string toString() const override;
};

struct MatchProductVM2 : public MatchProduct
{
    // This is -1 when should be ignored
    int variable_id;
    types::ValueBagMatcher expr;
    std::vector<int> idlist;

    MatchProductVM2(const std::string& pattern);
    bool matchItem(const types::Type& o) const override;
    bool match_buffer(types::Code code, const uint8_t* data, unsigned size) const override;
    std::string toString() const override;
};

}
}

// vim:set ts=4 sw=4:
#endif
