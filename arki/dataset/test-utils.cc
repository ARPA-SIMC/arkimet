/**
 * Copyright (C) 2007,2008,2009  ARPA-SIM <urpsim@smr.arpa.emr.it>
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
#include <arki/dataset/test-utils.h>
#include <arki/metadata.h>
#include <arki/metadata/collection.h>
#include <arki/dispatcher.h>
#include <wibble/string.h>
#include <wibble/regexp.h>
#include <fstream>
#include <strings.h>

using namespace std;
using namespace arki;
using namespace wibble;
using namespace arki::types;

namespace arki {
namespace tests {

void impl_ensure_dispatches(const wibble::tests::Location& loc, Dispatcher& dispatcher, Metadata& md, metadata::Consumer& mdc)
{
	metadata::Collection c;
	Dispatcher::Outcome res = dispatcher.dispatch(md, c);
	// If dispatch fails, print the notes
	if (res != Dispatcher::DISP_OK)
	{
		for (vector<Metadata>::iterator i = c.begin(); i != c.end(); ++i)
		{
			cerr << "Failed dispatch notes:" << endl;
			std::vector< Item<types::Note> > notes = i->notes();
			for (std::vector< Item<types::Note> >::const_iterator j = notes.begin();
					j != notes.end(); ++j)
				cerr << "   " << *j << endl;
		}
	}
	inner_ensure_equals(res, Dispatcher::DISP_OK);
	for (vector<Metadata>::iterator i = c.begin(); i != c.end(); ++i)
		mdc(*i);
}

OutputChecker::OutputChecker() : split(false) {}

void OutputChecker::splitIfNeeded()
{
	if (split) return;
	Splitter splitter("[\n\r]+", REG_EXTENDED);
	for (Splitter::const_iterator i = splitter.begin(str()); i != splitter.end(); ++i)
		lines.push_back(" " + *i);
	split = true;
}

std::string OutputChecker::join() const
{
	return str::join(lines.begin(), lines.end(), "\n");
}

void OutputChecker::impl_ensure_line_contains(const wibble::tests::Location& loc, const std::string& needle)
{
	splitIfNeeded();

	bool found = false;
	for (vector<string>::iterator i = lines.begin();
			!found && i != lines.end(); ++i)
	{
		if ((*i)[0] == '!') continue;

		if (i->find(needle) != std::string::npos )
		{
			(*i)[0] = '!';
			found = true;
		}
	}
	
	if (!found)
	{
		std::stringstream ss;
		ss << "'" << join() << "' does not contain '" << needle << "'";
		throw tut::failure(loc.msg(ss.str()));
	}
}

void OutputChecker::impl_ensure_all_lines_seen(const wibble::tests::Location& loc)
{
	splitIfNeeded();

	for (vector<string>::const_iterator i = lines.begin();
			i != lines.end(); ++i)
	{
		if ((*i)[0] != '!')
		{
			std::stringstream ss;
			ss << "'" << join() << "' still contains unchecked lines";
			throw tut::failure(loc.msg(ss.str()));
		}
	}
}

}

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
	"arc to index",
	"arc to rescan",
	"arc deleted",
	"state max",
};

}
// vim:set ts=4 sw=4:
