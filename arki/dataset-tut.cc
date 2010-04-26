/*
 * Copyright (C) 2007,2008  ARPA-SIM <urpsim@smr.arpa.emr.it>
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
#include <arki/dataset.h>
#include <arki/dataset/ondisk2.h>
#include <arki/dataset/outbound.h>
#include <arki/dataset/discard.h>
#include <arki/dataset/empty.h>
#include <arki/dataset/simple/reader.h>
#include <arki/dataset/simple/writer.h>
#include <arki/configfile.h>

#include <memory>
#include <sstream>
#include <iostream>

namespace tut {
using namespace std;
using namespace arki;

struct arki_dataset_shar {
	ConfigFile config;

	arki_dataset_shar()
	{
		system("rm -rf test200");
		system("rm -rf test80");
		system("rm -rf test98");
		system("rm -rf testds");
		// In-memory dataset configuration
		string conf =
			"[test]\n"
			"type = test\n"
			"step = daily\n"
			"filter = origin: GRIB1,200\n"
			"index = origin, reftime\n"
			"name = test200\n"
			"path = test200\n"
			"\n"
			"[local]\n"
			"type = local\n"
			"step = daily\n"
			"filter = origin: GRIB1,80\n"
			"index = origin, reftime\n"
			"name = test80\n"
			"path = test80\n"
			"\n"
			"[simple]\n"
			"type = simple\n"
			"step = daily\n"
			"filter = origin: GRIB1,1\n"
			"name = testds\n"
			"path = testds\n"
			"\n"
			"[ondisk2]\n"
			"type = ondisk2\n"
			"step = daily\n"
			"filter = origin: GRIB1,98\n"
			"index = origin, reftime\n"
			"name = test98\n"
			"path = test98\n"
			"\n"
			"[error]\n"
			"type = error\n"
			"step = daily\n"
			"name = error\n"
			"path = error\n"
			"\n"
			"[duplicates]\n"
			"type = duplicates\n"
			"step = daily\n"
			"name = error\n"
			"path = error\n"
			"\n"
			"[outbound]\n"
			"type = outbound\n"
			"step = daily\n"
			"name = outbound\n"
			"path = outbound\n"
			"\n"
			"[discard]\n"
			"type = discard\n"
			"name = discard\n"
			"path = discard\n";
		stringstream incfg(conf);
		config.parse(incfg, "(memory)");
	}
};
TESTGRP(arki_dataset);

template<> template<>
void to::test<1>()
{
	auto_ptr<ReadonlyDataset> testds(ReadonlyDataset::create(*config.section("error")));
	ensure(dynamic_cast<dataset::simple::Reader*>(testds.get()) != 0);
}

template<> template<>
void to::test<2>()
{
	auto_ptr<WritableDataset> testds(WritableDataset::create(*config.section("error")));
	ensure(dynamic_cast<dataset::simple::Writer*>(testds.get()) != 0);
}

template<> template<>
void to::test<3>()
{
	auto_ptr<ReadonlyDataset> testds(ReadonlyDataset::create(*config.section("duplicates")));
	ensure(dynamic_cast<dataset::simple::Reader*>(testds.get()) != 0);
}

template<> template<>
void to::test<4>()
{
	auto_ptr<WritableDataset> testds(WritableDataset::create(*config.section("duplicates")));
	ensure(dynamic_cast<dataset::simple::Writer*>(testds.get()) != 0);
}

template<> template<>
void to::test<5>()
{
	auto_ptr<ReadonlyDataset> testds(ReadonlyDataset::create(*config.section("outbound")));
	ensure(dynamic_cast<dataset::Empty*>(testds.get()) != 0);
}

template<> template<>
void to::test<6>()
{
	auto_ptr<WritableDataset> testds(WritableDataset::create(*config.section("outbound")));
	ensure(dynamic_cast<dataset::Outbound*>(testds.get()) != 0);
}

template<> template<>
void to::test<7>()
{
	auto_ptr<ReadonlyDataset> testds(ReadonlyDataset::create(*config.section("discard")));
	ensure(dynamic_cast<dataset::Empty*>(testds.get()) != 0);
}

template<> template<>
void to::test<8>()
{
	auto_ptr<WritableDataset> testds(WritableDataset::create(*config.section("discard")));
	ensure(dynamic_cast<dataset::Discard*>(testds.get()) != 0);
}

template<> template<>
void to::test<9>()
{
	auto_ptr<ReadonlyDataset> testds(ReadonlyDataset::create(*config.section("simple")));
	ensure(dynamic_cast<dataset::simple::Reader*>(testds.get()) != 0);
}

template<> template<>
void to::test<10>()
{
	auto_ptr<WritableDataset> testds(WritableDataset::create(*config.section("simple")));
	ensure(dynamic_cast<dataset::simple::Writer*>(testds.get()) != 0);
}

template<> template<>
void to::test<11>()
{
	auto_ptr<ReadonlyDataset> testds(ReadonlyDataset::create(*config.section("ondisk2")));
	ensure(dynamic_cast<dataset::ondisk2::Reader*>(testds.get()) != 0);
}

template<> template<>
void to::test<12>()
{
	auto_ptr<WritableDataset> testds(WritableDataset::create(*config.section("ondisk2")));
	ensure(dynamic_cast<dataset::ondisk2::Writer*>(testds.get()) != 0);
}

template<> template<>
void to::test<13>()
{
	auto_ptr<WritableDataset> testds(WritableDataset::create(*config.section("test")));
	ensure(dynamic_cast<dataset::ondisk2::Writer*>(testds.get()) != 0);
}

}

// vim:set ts=4 sw=4:
