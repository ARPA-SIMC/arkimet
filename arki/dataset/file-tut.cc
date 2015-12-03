#include "config.h"

#include <arki/tests/tests.h>
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
using namespace arki::tests;

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
    unique_ptr<ReadonlyDataset> ds(ReadonlyDataset::create(*s));
    metadata::Collection mdc(*ds, Matcher());
    ensure_equals(mdc.size(), 3u);
}

template<> template<>
void to::test<4>()
{
    ConfigFile cfg;
    dataset::File::readConfig("inbound/odim1.arkimet", cfg);

    ConfigFile* s = cfg.section("odim1.arkimet");
    ensure(s);

    ensure_equals(s->value("name"), "odim1.arkimet");
    ensure_equals(s->value("type"), "file");
    ensure_equals(s->value("format"), "arkimet");

    // Scan it to be sure it can be read
    unique_ptr<ReadonlyDataset> ds(ReadonlyDataset::create(*s));
    metadata::Collection mdc(*ds, Matcher());
    ensure_equals(mdc.size(), 1u);
}

}
