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
#include <arki/dataset/ondisk2/test-utils.h>
#include <wibble/string.h>
#include <strings.h>

using namespace std;
using namespace arki;
using namespace arki::types;
using namespace wibble;

namespace arki {
namespace dataset {
namespace ondisk2 {
namespace writer {

MaintenanceCollector::MaintenanceCollector()
{
	bzero(counts, sizeof(counts));
}

void MaintenanceCollector::clear()
{
	bzero(counts, sizeof(counts));
	fileStates.clear();
	checked.clear();
}

bool MaintenanceCollector::isClean() const
{
	for (size_t i = 0; i < STATE_MAX; ++i)
		if (i != OK && i != ARC_OK && counts[i])
			return false;
	return true;
}

void MaintenanceCollector::operator()(const std::string& file, State state)
{
	fileStates[file] = state;
	++counts[state];
}

size_t MaintenanceCollector::count(State s)
{
	checked.insert(s);
	return counts[s];
}

std::string MaintenanceCollector::remaining() const
{
	std::vector<std::string> res;
	for (size_t i = 0; i < MaintFileVisitor::STATE_MAX; ++i)
	{
		if (checked.find((State)i) != checked.end())
			continue;
		if (counts[i] == 0)
			continue;
		res.push_back(str::fmtf("%s: %d", names[i], counts[i]));
	}
	return str::join(res.begin(), res.end());
}

void MaintenanceCollector::dump(std::ostream& out) const
{
	using namespace std;
	out << "Results:" << endl;
	for (size_t i = 0; i < STATE_MAX; ++i)
		out << " " << names[i] << ": " << counts[i] << endl;
	for (std::map<std::string, State>::const_iterator i = fileStates.begin();
			i != fileStates.end(); ++i)
		out << "   " << i->first << ": " << names[i->second] << endl;
}

const char* MaintenanceCollector::names[] = {
	"ok",
	"to archive",
	"to delete",
	"to pack",
	"to index",
	"to rescan",
	"deleted",
	"arc ok",
	"arc to delete",
	"arc to index",
	"arc to rescan",
	"arc deleted",
	"state max",
};

}
}
}
}
// vim:set ts=4 sw=4:
