#include "arki/core/tests.h"
#include "arki/configfile.h"
#include "arki/metadata.h"
#include "arki/matcher.h"
#include "arki/dataset/file.h"
#include "arki/metadata/collection.h"
#include <sstream>
#include <iostream>
#include <memory>

namespace {
using namespace std;
using namespace arki;
using namespace arki::tests;
using arki::core::Time;

class Tests : public TestCase
{
    using TestCase::TestCase;
    void register_tests() override;
} test("arki_dataset_file");

void Tests::register_tests() {

add_method("grib", [] {
	ConfigFile cfg;
	dataset::File::readConfig("inbound/test.grib1", cfg);

	ConfigFile* s = cfg.section("test.grib1");
	ensure(s);

	ensure_equals(s->value("name"), "test.grib1");
	ensure_equals(s->value("type"), "file");
	ensure_equals(s->value("format"), "grib");
});

add_method("grib_as_bufr", [] {
	ConfigFile cfg;
	dataset::File::readConfig("bUFr:inbound/test.grib1", cfg);

	ConfigFile* s = cfg.section("test.grib1");
	ensure(s);

	ensure_equals(s->value("name"), "test.grib1");
	ensure_equals(s->value("type"), "file");
	ensure_equals(s->value("format"), "bufr");
});

add_method("grib_strangename", [] {
	ConfigFile cfg;
	system("cp inbound/test.grib1 strangename");
	dataset::File::readConfig("GRIB:strangename", cfg);

	ConfigFile* s = cfg.section("strangename");
	ensure(s);

	ensure_equals(s->value("name"), "strangename");
	ensure_equals(s->value("type"), "file");
	ensure_equals(s->value("format"), "grib");

    // Scan it to be sure it can be read
    unique_ptr<dataset::Reader> ds(dataset::Reader::create(*s));
    metadata::Collection mdc(*ds, Matcher());
    ensure_equals(mdc.size(), 3u);
});

add_method("metadata", [] {
    ConfigFile cfg;
    dataset::File::readConfig("inbound/odim1.arkimet", cfg);

    ConfigFile* s = cfg.section("odim1.arkimet");
    ensure(s);

    ensure_equals(s->value("name"), "odim1.arkimet");
    ensure_equals(s->value("type"), "file");
    ensure_equals(s->value("format"), "arkimet");

    // Scan it to be sure it can be read
    unique_ptr<dataset::Reader> ds(dataset::Reader::create(*s));
    metadata::Collection mdc(*ds, Matcher());
    ensure_equals(mdc.size(), 1u);
});

add_method("yaml", [] {
    ConfigFile cfg;
    dataset::File::readConfig("inbound/issue107.yaml", cfg);

    ConfigFile* s = cfg.section("issue107.yaml");
    wassert(actual(s).istrue());

    wassert(actual(s->value("name")) == "issue107.yaml");
    wassert(actual(s->value("type")) == "file");
    wassert(actual(s->value("format")) == "yaml");

    // Scan it to be sure it can be read
    unique_ptr<dataset::Reader> ds(dataset::Reader::create(*s));
    metadata::Collection mdc(*ds, Matcher());
    wassert(actual(mdc.size()) == 1u);
});

}

}
