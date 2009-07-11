/**
 * Copyright (C) 2009  ARPA-SIM <urpsim@smr.arpa.emr.it>
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
#ifndef ARKI_DATASET_ONDISK2_TESTUTILS_H
#define ARKI_DATASET_ONDISK2_TESTUTILS_H

#include <arki/dataset/test-utils.h>
#include <arki/dataset/ondisk2/maintenance.h>

namespace arki {
namespace dataset {
namespace ondisk2 {
namespace writer {

struct MaintenanceCollector : public MaintFileVisitor
{
	std::map <std::string, State> fileStates;
	size_t counts[STATE_MAX];
	static const char* names[];
	std::set<State> checked;

	MaintenanceCollector();

	void clear();
	bool isClean() const;
	virtual void operator()(const std::string& file, State state);
	void dump(std::ostream& out) const;
	size_t count(State s);
	std::string remaining() const;
};

}
}
}
}

#endif
