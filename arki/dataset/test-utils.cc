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
#include <arki/dispatcher.h>
#include <arki/utils/metadata.h>
#include <fstream>

using namespace std;
using namespace arki;
using namespace arki::types;

namespace arki {

size_t countDeletedMetadata(const std::string& fname)
{
	size_t count = 0;
	vector<Metadata> mds = Metadata::readFile(fname);
	for (vector<Metadata>::const_iterator i = mds.begin(); i != mds.end(); ++i)
		if (i->deleted)
			++count;
	return count;
}

namespace tests {

void impl_ensure_dispatches(const wibble::tests::Location& loc, Dispatcher& dispatcher, Metadata& md, MetadataConsumer& mdc)
{
	utils::metadata::Collector c;
	Dispatcher::Outcome res = dispatcher.dispatch(md, c);
	// If dispatch fails, print the notes
	if (res != Dispatcher::DISP_OK)
	{
		for (vector<Metadata>::iterator i = c.begin(); i != c.end(); ++i)
		{
			cerr << "Failed dispatch notes:" << endl;
			for (std::vector< Item<types::Note> >::const_iterator j = i->notes.begin();
					j != i->notes.end(); ++j)
				cerr << "   " << *j << endl;
		}
	}
	inner_ensure_equals(res, Dispatcher::DISP_OK);
	for (vector<Metadata>::iterator i = c.begin(); i != c.end(); ++i)
		mdc(*i);
}

}
}
// vim:set ts=4 sw=4:
