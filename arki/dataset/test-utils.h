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
#ifndef ARKI_DATASET_TESTUTILS_H
#define ARKI_DATASET_TESTUTILS_H

#include <arki/tests/test-utils.h>
#include <arki/metadata.h>
#include <arki/metadata/consumer.h>
#include <arki/dataset/maintenance.h>
#include <vector>
#include <string>
#include <sstream>

namespace arki {
struct Metadata;
struct Dispatcher;

namespace tests{
#define ensure_dispatches(x, y, z) arki::tests::impl_ensure_dispatches(wibble::tests::Location(__FILE__, __LINE__, #x ", " #y), (x), (y), (z))
void impl_ensure_dispatches(const wibble::tests::Location& loc, Dispatcher& dispatcher, Metadata& md, metadata::Consumer& mdc);

struct OutputChecker : public std::stringstream
{
	std::vector<std::string> lines;
	bool split;

	// Split the output into lines if it has not been done yet
	void splitIfNeeded();

	// Join the split and marked lines
	std::string join() const;
	
	OutputChecker();

	void ignore_line_containing(const std::string& needle);

#define ensure_line_contains(x) impl_ensure_line_contains(wibble::tests::Location(__FILE__, __LINE__, "look for " #x), (x))
#define inner_ensure_line_contains(x) impl_ensure_line_contains(wibble::tests::Location(loc, __FILE__, __LINE__, "look for " #x), (x))
	void impl_ensure_line_contains(const wibble::tests::Location& loc, const std::string& needle);

#define ensure_all_lines_seen() impl_ensure_all_lines_seen(wibble::tests::Location(__FILE__, __LINE__, "all lines seen"))
#define inner_ensure_all_lines_seen() impl_ensure_all_lines_seen(wibble::tests::Location(loc, __FILE__, __LINE__, "all lines seen"))
	void impl_ensure_all_lines_seen(const wibble::tests::Location& loc);
};

}

struct MaintenanceCollector : public dataset::maintenance::MaintFileVisitor
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

#endif
