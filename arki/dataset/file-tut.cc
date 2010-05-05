/*
 * Copyright (C) 2008  Enrico Zini <enrico@enricozini.org>
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
#include <arki/dataset/file.h>
#include <arki/metadata/collection.h>

#include <sstream>
#include <iostream>
#include <memory>

namespace tut {
using namespace std;
using namespace arki;
using namespace arki::utils;

struct arki_dataset_file_shar {
};
TESTGRP(arki_dataset_file);

template<> template<>
void to::test<1>()
{
	ConfigFile cfg;
	dataset::File::readConfig("inbound/test.grib1", cfg);

	ConfigFile* s = cfg.section("test.grib1");
	ensure(s);

	ensure_equals(s->value("name"), "test.grib1");
	ensure_equals(s->value("type"), "file");
	ensure_equals(s->value("format"), "grib");
}

template<> template<>
void to::test<2>()
{
	ConfigFile cfg;
	dataset::File::readConfig("bUFr:inbound/test.grib1", cfg);

	ConfigFile* s = cfg.section("test.grib1");
	ensure(s);

	ensure_equals(s->value("name"), "test.grib1");
	ensure_equals(s->value("type"), "file");
	ensure_equals(s->value("format"), "bufr");
}

template<> template<>
void to::test<3>()
{
	ConfigFile cfg;
	system("cp inbound/test.grib1 strangename");
	dataset::File::readConfig("GRIB:strangename", cfg);

	ConfigFile* s = cfg.section("strangename");
	ensure(s);

	ensure_equals(s->value("name"), "strangename");
	ensure_equals(s->value("type"), "file");
	ensure_equals(s->value("format"), "grib");

	// Scan it to be sure it can be read
	auto_ptr<ReadonlyDataset> ds(ReadonlyDataset::create(*s));
	dataset::DataQuery q(Matcher::parse(""), true);
	metadata::Collection mdc;
	ds->queryData(q, mdc);
	ensure_equals(mdc.size(), 3u);
}

}

// vim:set ts=4 sw=4:
