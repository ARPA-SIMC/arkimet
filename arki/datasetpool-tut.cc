/*
 * Copyright (C) 2007--2011  ARPA-SIM <urpsim@smr.arpa.emr.it>
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

#include "config.h"

#include <arki/tests/test-utils.h>
#include <arki/datasetpool.h>
#include <arki/configfile.h>

namespace tut {
using namespace std;
using namespace arki;

struct arki_datasetpool_shar {
	ConfigFile config;

	arki_datasetpool_shar()
	{
		// In-memory dataset configuration
		string conf =
			"[test200]\n"
			"type = test\n"
			"step = daily\n"
			"filter = origin: GRIB1,200\n"
			"index = origin, reftime\n"
			"name = test200\n"
			"path = test200\n"
			"\n"
			"[test80]\n"
			"type = test\n"
			"step = daily\n"
			"filter = origin: GRIB1,80\n"
			"index = origin, reftime\n"
			"name = test80\n"
			"path = test80\n"
			"\n"
			"[error]\n"
			"type = error\n"
			"step = daily\n"
			"name = error\n"
			"path = error\n";
		stringstream incfg(conf);
		config.parse(incfg, "(memory)");
	}
};
TESTGRP(arki_datasetpool);

// Test instantiating readonly datasets
template<> template<>
void to::test<1>()
{
	ReadonlyDatasetPool pool(config);
	ensure(pool.get("error") != 0);
	ensure(pool.get("test200") != 0);
	ensure(pool.get("test80") != 0);
	ensure(pool.get("duplicates") == 0);
}

// Test instantiating writable datasets
template<> template<>
void to::test<2>()
{
	WritableDatasetPool pool(config);
	ensure(pool.get("error") != 0);
	ensure(pool.get("test200") != 0);
	ensure(pool.get("test80") != 0);
	ensure(pool.get("duplicates") == 0);
}

}

// vim:set ts=4 sw=4:
