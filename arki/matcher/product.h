#ifndef ARKI_MATCHER_PRODUCT
#define ARKI_MATCHER_PRODUCT

/*
 * matcher/product - Product matcher
 *
 * Copyright (C) 2007--2012  ARPA-SIM <urpsim@smr.arpa.emr.it>
 *
 * This program is free software; you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation; either version 2 of the License, or
 * (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License along
 * with this program; if not, write to the Free Software Foundation, Inc.,
 * 51 Franklin Street, Fifth Floor, Boston, MA 02110-1301 USA.
 *
 * Author: Enrico Zini <enrico@enricozini.com>
 */

#include <arki/matcher.h>
#include <arki/types/product.h>

namespace arki {
namespace matcher {

/**
 * Match Products
 */
struct MatchProduct : public Implementation
{
    std::string name() const override;

    static MatchProduct* parse(const std::string& pattern);
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
    std::string toString() const override;
};

struct MatchProductBUFR : public MatchProduct
{
	// These are -1 when they should be ignored in the match
	int type;
	int subtype;
	int localsubtype;
	ValueBag values;

    MatchProductBUFR(const std::string& pattern);
    bool matchItem(const types::Type& o) const override;
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
    std::string toString() const override;
};

struct MatchProductVM2 : public MatchProduct
{
    // This is -1 when should be ignored
    int variable_id;
    ValueBag expr;
    std::vector<int> idlist;

    MatchProductVM2(const std::string& pattern);
    bool matchItem(const types::Type& o) const override;
    std::string toString() const override;
};

}
}

// vim:set ts=4 sw=4:
#endif
