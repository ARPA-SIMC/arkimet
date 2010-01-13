/*
 * dataset/gridspace - Filter another dataset over a dense data grid
 *
 * Copyright (C) 2010  ARPA-SIM <urpsim@smr.arpa.emr.it>
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

#include <arki/dataset/gridspace.h>
#include <arki/metadata.h>
#include <arki/matcher.h>
#include <arki/summary.h>
#include <arki/sort.h>

// #include <iostream>

using namespace std;
using namespace wibble;

namespace arki {
namespace dataset {

Gridspace::Gridspace()
{
}

Gridspace::~Gridspace()
{
}

void Gridspace::queryData(const dataset::DataQuery& q, MetadataConsumer& consumer)
{
#if 0
	datasets[0]->queryData(q, consumer);
#endif
}

void Gridspace::querySummary(const Matcher& matcher, Summary& summary)
{
	using namespace wibble::str;

#if 0
	// Handle the trivial case of only one dataset
	if (datasets.size() == 1)
	{
		datasets[0]->querySummary(matcher, summary);
		return;
	}
#endif
}

void Gridspace::queryBytes(const dataset::ByteQuery& q, std::ostream& out)
{
#if 0
	// Here we must serialize, as we do not know how to merge raw data streams
	//
	// We cannot just wrap queryData because some subdatasets could be
	// remote, and that would mean doing postprocessing on the client side,
	// potentially transferring terabytes of data just to produce a number

	for (std::vector<ReadonlyDataset*>::iterator i = datasets.begin();
			i != datasets.end(); ++i)
		(*i)->queryBytes(q, out);
#endif
}

}
}
// vim:set ts=4 sw=4:
