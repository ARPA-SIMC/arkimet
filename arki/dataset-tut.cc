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
#include <arki/dataset.h>
#include <arki/dataset/ondisk2.h>
#include <arki/dataset/outbound.h>
#include <arki/dataset/discard.h>
#include <arki/dataset/empty.h>
#include <arki/dataset/simple/reader.h>
#include <arki/dataset/simple/writer.h>
#include <arki/metadata/collection.h>
#include <arki/scan/any.h>
#include <arki/configfile.h>
#include <wibble/string.h>

#include <memory>
#include <sstream>
#include <iostream>

namespace tut {
using namespace std;
using namespace arki;
using namespace wibble;

struct TestDataInfo
{
    WritableDataset::AcquireResult import_outcome;
    string destfile;
    Matcher matcher;

    TestDataInfo(WritableDataset::AcquireResult ar, const std::string& df, const std::string& matcher)
        : import_outcome(ar), destfile(df), matcher(Matcher::parse(matcher)) {}
};

struct TestData
{
    std::string fname;
    std::vector<TestDataInfo> info;
};


struct arki_dataset_shar {
	ConfigFile config;
    TestData tdata_grib;
    TestData tdata_bufr;
    TestData tdata_vm2;

	arki_dataset_shar()
	{
        tdata_grib.fname = "inbound/test.grib1";
        tdata_grib.info.push_back(TestDataInfo(WritableDataset::ACQ_OK, "2007/07-08.grib1", "reftime:=2007-07-08"));
        tdata_grib.info.push_back(TestDataInfo(WritableDataset::ACQ_OK, "2007/07-07.grib1", "reftime:=2007-07-07"));
        tdata_grib.info.push_back(TestDataInfo(WritableDataset::ACQ_OK, "2007/10-09.grib1", "reftime:=2007-10-09"));

        tdata_bufr.fname = "inbound/test.bufr";
        tdata_bufr.info.push_back(TestDataInfo(WritableDataset::ACQ_OK, "2005/12-01.bufr", "reftime:=2005-12-01"));
        tdata_bufr.info.push_back(TestDataInfo(WritableDataset::ACQ_OK, "2004/11-30.bufr", "reftime:=2004-11-30; proddef:GRIB:blo=60"));
        tdata_bufr.info.push_back(TestDataInfo(WritableDataset::ACQ_OK, "2004/11-30.bufr", "reftime:=2004-11-30; proddef:GRIB:blo=6"));

        tdata_vm2.fname = "inbound/test.vm2";
        tdata_vm2.info.push_back(TestDataInfo(WritableDataset::ACQ_OK, "1987/10-31.vm2", "reftime:=1987-10-31; product:VM2,227"));
        tdata_vm2.info.push_back(TestDataInfo(WritableDataset::ACQ_OK, "1987/10-31.vm2", "reftime:=1987-10-31; product:VM2,228"));
        tdata_vm2.info.push_back(TestDataInfo(WritableDataset::ACQ_OK, "2011/01-01.vm2", "reftime:=2011-01-01; product:VM2,1"));
        tdata_vm2.info.push_back(TestDataInfo(WritableDataset::ACQ_OK, "2011/01-01.vm2", "reftime:=2011-01-01; product:VM2,2"));

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

struct TestDataset
{
    const TestData& td;
    ConfigFile config;
    ConfigFile* cfgtest;
    std::string path;
    metadata::Collection input_data;

    TestDataset(const TestData& td, const std::string& conf)
        : td(td)
    {
        stringstream incfg(conf);
        config.parse(incfg, "(memory)");
        cfgtest = config.section("test");
        path = cfgtest->value("path");
    }

    void test_import(LOCPRM)
    {
        using namespace arki::tests;

        auto_ptr<WritableDataset> ds(WritableDataset::create(*cfgtest));

        ensure(scan::scan(td.fname, input_data));
        ensure_equals(input_data.size(), td.info.size());

        for (unsigned i = 0; i < input_data.size(); ++i)
        {
            ensure_equals(ds->acquire(input_data[i]), td.info[i].import_outcome);
            ensure_file_exists(ILOC, str::joinpath(path, td.info[i].destfile));
        }
    }

    void test_dataquery(LOCPRM)
    {
        using namespace arki::tests;

        auto_ptr<ReadonlyDataset> ds(ReadonlyDataset::create(*cfgtest));

        for (unsigned i = 0; i < input_data.size(); ++i)
        {
            metadata::Collection mdc;
            ds->queryData(dataset::DataQuery(td.info[i].matcher, false), mdc);
            ensure_equals(mdc.size(), 1u);
        }

        // // Check that the source record that comes out is ok
        // UItem<Source> source = mdc[0].source;
        // ensure_equals(source->style(), Source::BLOB);
        // ensure_equals(source->format, "grib1");
        // UItem<source::Blob> blob = source.upcast<source::Blob>();
        // ensure_equals(blob->filename, sys::fs::abspath("test200/2007/07-08.grib1"));
        // ensure_equals(blob->offset, 0u);
        // ensure_equals(blob->size, 7218u);

        // mdc.clear();
        // testds->queryData(dataset::DataQuery(Matcher::parse("origin:GRIB1,80"), false), mdc);
        // ensure_equals(mdc.size(), 0u);

        // mdc.clear();
        // testds->queryData(dataset::DataQuery(Matcher::parse("origin:GRIB1,98"), false), mdc);
        // ensure_equals(mdc.size(), 0u);
    }


};

template<> template<>
void to::test<14>()
{
    TestDataset tds(tdata_grib,
        "[test]\n"
        "type = ondisk2\n"
        "step = daily\n"
        "filter = origin: GRIB1,200\n"
        "index = origin, reftime\n"
        "unique = reftime, origin, product, level, timerange, area\n"
        "name = test\n"
        "path = test\n"
    );
    tds.test_import(LOC);
}

template<> template<>
void to::test<15>()
{
    TestDataset tds(tdata_bufr,
        "[test]\n"
        "type = ondisk2\n"
        "step = daily\n"
        "filter = origin: GRIB1,200\n"
        "index = origin, reftime\n"
        "unique = reftime, origin, product, level, timerange, area\n"
        "name = test\n"
        "path = test\n"
    );
    tds.test_import(LOC);
}

template<> template<>
void to::test<16>()
{
    TestDataset tds(tdata_vm2,
        "[test]\n"
        "type = ondisk2\n"
        "step = daily\n"
        "filter = origin: GRIB1,200\n"
        "index = origin, reftime\n"
        "unique = reftime, origin, product, level, timerange, area\n"
        "name = test\n"
        "path = test\n"
    );
    tds.test_import(LOC);
}

}

// vim:set ts=4 sw=4:
