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

namespace arki {
struct Metadata;
struct Dispatcher;

struct MetadataCounter : public metadata::Consumer
{
	size_t count;
	MetadataCounter() : count(0) {}
	bool operator()(Metadata& md)
	{
		++count;
		return true;
	}
};

namespace tests{
#define ensure_dispatches(x, y, z) arki::tests::impl_ensure_dispatches(wibble::tests::Location(__FILE__, __LINE__, #x ", " #y), (x), (y), (z))
void impl_ensure_dispatches(const wibble::tests::Location& loc, Dispatcher& dispatcher, Metadata& md, metadata::Consumer& mdc);
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
