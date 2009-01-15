/*
 * Copyright (C) 2007  ARPA-SIM <urpsim@smr.arpa.emr.it>
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

#include <arki/tests/test-utils.h>
#include <arki/dispatcher.h>
#include <arki/dataset.h>
#include <arki/configfile.h>
#include <arki/metadata.h>
#include <arki/matcher.h>
#include <arki/types/assigneddataset.h>
#include <arki/scan/grib.h>

namespace tut {
using namespace std;
using namespace arki;

struct arki_dispatcher_shar {
	ConfigFile config;

	arki_dispatcher_shar()
	{
		// Cleanup the test datasets
		system("rm -rf test200/*");
		system("rm -rf test80/*");
		system("rm -rf error/*");

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
TESTGRP(arki_dispatcher);

struct MetadataCollector : public vector<Metadata>, public MetadataConsumer
{
	bool operator()(Metadata& md)
	{
		push_back(md);
		return true;
	}
};

static inline std::string dsname(const Metadata& md)
{
	return md.get(types::TYPE_ASSIGNEDDATASET).upcast<types::AssignedDataset>()->name;
}

// Test simple dispatching
template<> template<>
void to::test<1>()
{
	Metadata md;
	MetadataCollector mdc;
	scan::Grib scanner;
	RealDispatcher dispatcher(config);
	scanner.open("inbound/test.grib1");
	ensure(scanner.next(md));
	ensure_equals(dispatcher.dispatch(md, mdc), Dispatcher::DISP_OK);
	ensure_equals(dsname(mdc.back()), "test200");
	ensure(scanner.next(md));
	ensure_equals(dispatcher.dispatch(md, mdc), Dispatcher::DISP_OK);
	ensure_equals(dsname(mdc.back()), "test80");
	ensure(scanner.next(md));
	ensure_equals(dispatcher.dispatch(md, mdc), Dispatcher::DISP_ERROR);
	ensure_equals(dsname(mdc.back()), "error");
	ensure(!scanner.next(md));

	dispatcher.flush();
}

}

// vim:set ts=4 sw=4:
