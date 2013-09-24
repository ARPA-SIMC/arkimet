/*
 * Copyright (C) 2007--2012  ARPA-SIM <urpsim@smr.arpa.emr.it>
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
#include <arki/dataset/test-utils.h>
#include <arki/metadata/tests.h>
#include <arki/dataset.h>
#include <arki/dataset/ondisk2.h>
#include <arki/dataset/outbound.h>
#include <arki/dataset/discard.h>
#include <arki/dataset/empty.h>
#include <arki/dataset/simple/reader.h>
#include <arki/dataset/simple/writer.h>
#include <arki/metadata/collection.h>
#include <arki/summary.h>
#include <arki/types/source.h>
#include <arki/scan/any.h>
#include <arki/configfile.h>
#include <arki/utils/files.h>
#include <wibble/string.h>
#include <wibble/sys/fs.h>

#include <memory>
#include <sstream>
#include <iostream>

namespace tut {
using namespace std;
using namespace arki;
using namespace arki::utils;
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
    std::string format;
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
        tdata_grib.format = "grib";
        tdata_grib.info.push_back(TestDataInfo(WritableDataset::ACQ_OK, "2007/07-08.grib1", "reftime:=2007-07-08"));
        tdata_grib.info.push_back(TestDataInfo(WritableDataset::ACQ_OK, "2007/07-07.grib1", "reftime:=2007-07-07"));
        tdata_grib.info.push_back(TestDataInfo(WritableDataset::ACQ_OK, "2007/10-09.grib1", "reftime:=2007-10-09"));

        tdata_bufr.fname = "inbound/test.bufr";
        tdata_bufr.format = "bufr";
        tdata_bufr.info.push_back(TestDataInfo(WritableDataset::ACQ_OK, "2005/12-01.bufr", "reftime:=2005-12-01"));
        tdata_bufr.info.push_back(TestDataInfo(WritableDataset::ACQ_OK, "2004/11-30.bufr", "reftime:=2004-11-30; proddef:GRIB:blo=60"));
        tdata_bufr.info.push_back(TestDataInfo(WritableDataset::ACQ_OK, "2004/11-30.bufr", "reftime:=2004-11-30; proddef:GRIB:blo=6"));

        tdata_vm2.fname = "inbound/test.vm2";
        tdata_vm2.format = "vm2";
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

    void test_import(WIBBLE_TEST_LOCPRM)
    {
        // Clear everything
        if (sys::fs::isdir(path)) sys::fs::rmtree(path);

        auto_ptr<WritableDataset> ds(WritableDataset::create(*cfgtest));

        wtest(istrue, scan::scan(td.fname, input_data));
        wtest(equals, td.info.size(), input_data.size());

        for (unsigned i = 0; i < input_data.size(); ++i)
        {
            wtest(equals, td.info[i].import_outcome, ds->acquire(input_data[i]));
            wtest(file_exists, str::joinpath(path, td.info[i].destfile));
        }
    }

    void test_querydata(WIBBLE_TEST_LOCPRM)
    {
        auto_ptr<ReadonlyDataset> ds(ReadonlyDataset::create(*cfgtest));

        for (unsigned i = 0; i < input_data.size(); ++i)
        {
            using namespace arki::types;

            // Check that what we imported can be queried
            metadata::Collection mdc;
            ds->queryData(dataset::DataQuery(td.info[i].matcher, false), mdc);
            wtest(equals, 1u, mdc.size());

            // Check that the result matches what was imported
            UItem<source::Blob> s1 = input_data[i].source.upcast<source::Blob>();
            UItem<source::Blob> s2 = mdc[0].source.upcast<source::Blob>();
            wtest(equals, s1->format, s2->format);
            wtest(equals, s1->size, s2->size);

            wtest(istrue, td.info[i].matcher(mdc[0]));
        }
    }

    void test_querysummary(WIBBLE_TEST_LOCPRM)
    {
        auto_ptr<ReadonlyDataset> ds(ReadonlyDataset::create(*cfgtest));

        // Query summary of everything
        {
            Summary s;
            ds->querySummary(Matcher(), s);
            wtest(equals, input_data.size(), s.count());
        }

        for (unsigned i = 0; i < input_data.size(); ++i)
        {
            using namespace arki::types;

            // Query summary of single items
            Summary s;
            ds->querySummary(td.info[i].matcher, s);

            wtest(equals, 1u, s.count());

            UItem<source::Blob> s1 = input_data[i].source.upcast<source::Blob>();
            wtest(equals, s1->size, s.size());
        }
    }

    void test_querybytes(WIBBLE_TEST_LOCPRM)
    {
        auto_ptr<ReadonlyDataset> ds(ReadonlyDataset::create(*cfgtest));

        for (unsigned i = 0; i < input_data.size(); ++i)
        {
            using namespace arki::types;

            dataset::ByteQuery bq;
            bq.setData(td.info[i].matcher);
            std::stringstream os;
            ds->queryBytes(bq, os);

            // Write it out and rescan
            files::write_file("testdata", os.str());
            metadata::Collection tmp;
            wtest(istrue, scan::scan("testdata", tmp, input_data[i].source->format));

            // Ensure that what we rescanned is what was imported
            wtest(equals, 1u, tmp.size());
            wtest(md_similar, input_data[i], tmp[0]);

            //UItem<source::Blob> s1 = input_data[i].source.upcast<source::Blob>();
            //wtest(equals, s1->size, os.str().size());
        }
    }

    void test_querybytes_integrity(WIBBLE_TEST_LOCPRM)
    {
        auto_ptr<ReadonlyDataset> ds(ReadonlyDataset::create(*cfgtest));

        // Query everything
        dataset::ByteQuery bq;
        bq.setData(Matcher());
        std::stringstream os;
        ds->queryBytes(bq, os);

        // Check that what we got matches the total size of what we imported
        size_t total_size = 0;
        for (unsigned i = 0; i < input_data.size(); ++i)
        {
            using namespace arki::types;
            UItem<source::Blob> s1 = input_data[i].source.upcast<source::Blob>();
            total_size += s1->size;
        }
        wtest(gte, total_size, os.str().size());

        // Write the results to disk
        utils::files::write_file("tempdata", os.str());

        // Check that they can be scanned again
        metadata::Collection mdc;
        wtest(istrue, scan::scan("tempdata", mdc, td.format));

        sys::fs::unlink("tempdata");
    }

    void test_locked(WIBBLE_TEST_LOCPRM)
    {
        // Lock a dataset for writing
        auto_ptr<WritableDataset> wds(WritableDataset::create(*cfgtest));
        Pending p = wds->test_writelock();

        // Try to read from it, it should still work with WAL
        auto_ptr<ReadonlyDataset> rds(ReadonlyDataset::create(*cfgtest));
        dataset::ByteQuery bq;
        bq.setData(Matcher());
        std::stringstream os;
        rds->queryBytes(bq, os);
    }

    void test_all(WIBBLE_TEST_LOCPRM)
    {
        ftest(test_import);
        ftest(test_querydata);
        ftest(test_querysummary);
        ftest(test_querybytes);
        ftest(test_querybytes_integrity);
        ftest(test_locked);
    }
};

// ondisk2 GRIB
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
    ftest(tds.test_all);
}

// ondisk2 BUFR
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
    ftest(tds.test_all);
}

// ondisk2 VM2
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
    ftest(tds.test_all);
}

// ondisk2 ODIM
template<> template<>
void to::test<17>()
{
#if 0
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
    ftest(tds.test_all);
#endif
}

// simple plainmft GRIB
template<> template<>
void to::test<18>()
{
    TestDataset tds(tdata_grib,
        "[test]\n"
        "type = simple\n"
        "step = daily\n"
        "filter = origin: GRIB1,200\n"
        "index = origin, reftime\n"
        "unique = reftime, origin, product, level, timerange, area\n"
        "name = test\n"
        "path = test\n"
    );
    ftest(tds.test_all);
}

// simple sqlitemft GRIB
template<> template<>
void to::test<19>()
{
    arki::tests::ForceSqlite fs;

    TestDataset tds(tdata_grib,
        "[test]\n"
        "type = simple\n"
        "step = daily\n"
        "filter = origin: GRIB1,200\n"
        "index = origin, reftime\n"
        "unique = reftime, origin, product, level, timerange, area\n"
        "name = test\n"
        "path = test\n"
    );
    ftest(tds.test_all);
}

// simple plainmft BUFR
template<> template<>
void to::test<20>()
{
    TestDataset tds(tdata_bufr,
        "[test]\n"
        "type = simple\n"
        "step = daily\n"
        "filter = origin: GRIB1,200\n"
        "index = origin, reftime\n"
        "unique = reftime, origin, product, level, timerange, area\n"
        "name = test\n"
        "path = test\n"
    );
    ftest(tds.test_all);
}

// simple sqlitemft BUFR
template<> template<>
void to::test<21>()
{
    arki::tests::ForceSqlite fs;

    TestDataset tds(tdata_bufr,
        "[test]\n"
        "type = simple\n"
        "step = daily\n"
        "filter = origin: GRIB1,200\n"
        "index = origin, reftime\n"
        "unique = reftime, origin, product, level, timerange, area\n"
        "name = test\n"
        "path = test\n"
    );
    ftest(tds.test_all);
}

// simple plainmft VM2
template<> template<>
void to::test<22>()
{
    TestDataset tds(tdata_vm2,
        "[test]\n"
        "type = simple\n"
        "step = daily\n"
        "filter = origin: GRIB1,200\n"
        "index = origin, reftime\n"
        "unique = reftime, origin, product, level, timerange, area\n"
        "name = test\n"
        "path = test\n"
    );
    ftest(tds.test_all);
}

// simple sqlitemft VM2
template<> template<>
void to::test<23>()
{
    arki::tests::ForceSqlite fs;

    TestDataset tds(tdata_vm2,
        "[test]\n"
        "type = simple\n"
        "step = daily\n"
        "filter = origin: GRIB1,200\n"
        "index = origin, reftime\n"
        "unique = reftime, origin, product, level, timerange, area\n"
        "name = test\n"
        "path = test\n"
    );
    ftest(tds.test_all);
}

// simple plainmft ODIM
template<> template<>
void to::test<24>()
{
#if 0
    TestDataset tds(tdata_vm2,
        "[test]\n"
        "type = simple\n"
        "step = daily\n"
        "filter = origin: GRIB1,200\n"
        "index = origin, reftime\n"
        "unique = reftime, origin, product, level, timerange, area\n"
        "name = test\n"
        "path = test\n"
    );
    ftest(tds.test_all);
#endif
}

// simple sqlitemft ODIM
template<> template<>
void to::test<25>()
{
#if 0
    arki::tests::ForceSqlite fs;

    TestDataset tds(tdata_vm2,
        "[test]\n"
        "type = simple\n"
        "step = daily\n"
        "filter = origin: GRIB1,200\n"
        "index = origin, reftime\n"
        "unique = reftime, origin, product, level, timerange, area\n"
        "name = test\n"
        "path = test\n"
    );
    ftest(tds.test_all);
#endif
}

}

// vim:set ts=4 sw=4:
