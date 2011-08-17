/*
 * Copyright (C) 2007--2011  Enrico Zini <enrico@enricozini.org>
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

#include "config.h"

#include <arki/tests/test-utils.h>
#include <arki/configfile.h>
#include <arki/metadata.h>
#include <arki/metadata/collection.h>
#include <arki/scan/grib.h>
#include <arki/dispatcher.h>

#include <sstream>
#include <iostream>

namespace tut {
using namespace std;
using namespace arki;

struct arki_dataset_discard_shar {
	ConfigFile config;

	arki_dataset_discard_shar()
	{
		// Cleanup the test datasets
		system("rm -rf test200/*");
		system("rm -rf test80/*");
		system("rm -rf error/*");

		// In-memory dataset configuration
		string conf =
			"[test200]\n"
			"type = discard\n"
			"filter = origin: GRIB1,200\n"
			"name = test200\n"
			"path = test200\n"
			"\n"
			"[test80]\n"
			"type = discard\n"
			"filter = origin: GRIB1,80\n"
			"name = test80\n"
			"path = test80\n"
			"\n"
			"[error]\n"
			"type = discard\n"
			"name = error\n"
			"path = error\n";
		stringstream incfg(conf);
		config.parse(incfg, "(memory)");
	}
};
TESTGRP(arki_dataset_discard);

// Test acquiring the data
template<> template<>
void to::test<1>()
{
	// Import data into the datasets
	Metadata md;
	metadata::Collection mdc;
	scan::Grib scanner;
	RealDispatcher dispatcher(config);
	scanner.open("inbound/test.grib1");
	ensure(scanner.next(md));
	ensure_equals(dispatcher.dispatch(md, mdc), Dispatcher::DISP_OK);
	ensure_equals(dispatcher.outboundFailures(), 0u);
	ensure_equals(mdc.size(), 1u);
	ensure(scanner.next(md));
	ensure_equals(dispatcher.dispatch(md, mdc), Dispatcher::DISP_OK);
	ensure_equals(dispatcher.outboundFailures(), 0u);
	ensure_equals(mdc.size(), 2u);
	ensure(scanner.next(md));
	ensure_equals(dispatcher.dispatch(md, mdc), Dispatcher::DISP_ERROR);
	ensure_equals(dispatcher.outboundFailures(), 0u);
	ensure_equals(mdc.size(), 3u);
	ensure(!scanner.next(md));
	dispatcher.flush();
}

}

// vim:set ts=4 sw=4:
