/*
 * Copyright (C) 2010--2011  Enrico Zini <enrico@enricozini.org>
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
#include <arki/metadatagrid.h>
#include <arki/metadata.h>
#include <arki/matcher.h>
#include <arki/types/origin.h>
#include <arki/types/product.h>

#include <sstream>
#include <iostream>

namespace tut {
using namespace std;
using namespace arki;

struct arki_dataset_metadatagrid_shar {
	arki_dataset_metadatagrid_shar()
	{
	}

	~arki_dataset_metadatagrid_shar()
	{
	}
};
TESTGRP(arki_dataset_metadatagrid);


// Test querying the datasets
template<> template<>
void to::test<1>()
{
	// Create a 2x2 metadata grid
	MetadataGrid mdg;
	mdg.add(types::origin::GRIB1::create(200, 0, 101));
	mdg.add(types::origin::GRIB1::create(200, 0, 102));
	mdg.add(types::product::GRIB1::create(200, 140, 229));
	mdg.add(types::product::GRIB1::create(200, 140, 230));
	mdg.consolidate();

    //ensure_equals(mdg.maxidx, 4u);

	Metadata md;
	md.set(types::origin::GRIB1::create(200, 0, 101));
	md.set(types::product::GRIB1::create(200, 140, 229));

	ensure_equals(mdg.index(md), 0);
}

}

// vim:set ts=4 sw=4:
