/*
 * Copyright (C) 2007,2008  Enrico Zini <enrico@enricozini.org>
 *
 * This library is free software; you can redistribute it and/or
 * modify it under the terms of the GNU Lesser General Public
 * License as published by the Free Software Foundation; either
 * version 2.1 of the License, or (at your option) any later version.
 *
 * This library is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the GNU
 * Lesser General Public License for more details.
 *
 * You should have received a copy of the GNU Lesser General Public
 * License along with this library; if not, write to the Free Software
 * Foundation, Inc., 59 Temple Place, Suite 330, Boston, MA 02111-1307  USA
 */

#include <arki/tests/test-utils.h>
#include <arki/configfile.h>
#include <arki/metadata.h>
#include <arki/matcher.h>
#include <arki/dataset/empty.h>

#include <sstream>
#include <iostream>

namespace tut {
using namespace std;
using namespace arki;

struct arki_dataset_empty_shar {
};
TESTGRP(arki_dataset_empty);

struct MetadataCollector : public vector<Metadata>, public MetadataConsumer
{
	bool operator()(Metadata& md)
	{
		push_back(md);
		return true;
	}
};

// Test accessing the data
template<> template<>
void to::test<1>()
{
	ConfigFile cfg;
	dataset::Empty ds(cfg);

	MetadataCollector mdc;
	ds.queryMetadata(Matcher::parse("origin:GRIB1 or BUFR or GRIB2"), false, mdc);
	ensure_equals(mdc.size(), 0u);
}

}

// vim:set ts=4 sw=4:
