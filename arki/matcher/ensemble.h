#ifndef ARKI_MATCHER_ENSEMBLE
#define ARKI_MATCHER_ENSEMBLE

/*
 * matcher/ensemble - Ensemble matcher
 *
 * Copyright (C) 2007,2008  ARPA-SIM <urpsim@smr.arpa.emr.it>
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
#include <arki/types/ensemble.h>

namespace arki {
namespace matcher {

/**
 * Match Ensembles
 */
struct MatchEnsemble : public Implementation
{
	//MatchType type() const { return MATCH_ENSEMBLE; }
	std::string name() const;

	static MatchEnsemble* parse(const std::string& pattern);
};

struct MatchEnsembleGRIB : public MatchEnsemble
{
	ValueBag expr;

	MatchEnsembleGRIB(const std::string& pattern);
	bool matchItem(const Item<>& o) const;
	std::string toString() const;
};

}
}

// vim:set ts=4 sw=4:
#endif
