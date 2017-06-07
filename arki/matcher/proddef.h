#ifndef ARKI_MATCHER_PRODDEF_H
#define ARKI_MATCHER_PRODDEF_H

/*
 * matcher/proddef - Product definition matcher
 *
 * Copyright (C) 2007--2011  ARPAE-SIMC <simc-urp@arpae.it>
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
#include <arki/types/proddef.h>

namespace arki {
namespace matcher {

/**
 * Match Proddefs
 */
struct MatchProddef : public Implementation
{
    std::string name() const override;

    static std::unique_ptr<MatchProddef> parse(const std::string& pattern);
    static void init();
};

struct MatchProddefGRIB : public MatchProddef
{
    ValueBag expr;

    MatchProddefGRIB(const std::string& pattern);
    bool matchItem(const types::Type& o) const override;
    std::string toString() const override;
};

}
}

// vim:set ts=4 sw=4:
#endif
