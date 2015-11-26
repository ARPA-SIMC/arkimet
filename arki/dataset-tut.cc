#include "arki/tests/tests.h"
#include "arki/dataset/tests.h"
#include "arki/metadata/tests.h"
#include "arki/dataset.h"
#include "arki/dataset/ondisk2.h"
#include "arki/dataset/outbound.h"
#include "arki/dataset/discard.h"
#include "arki/dataset/empty.h"
#include "arki/dataset/simple/reader.h"
#include "arki/dataset/simple/writer.h"
#include "arki/metadata/collection.h"
#include "arki/summary.h"
#include "arki/types/source.h"
#include "arki/types/source/blob.h"
#include "arki/scan/any.h"
#include "arki/configfile.h"
#include "arki/utils/files.h"
#include "arki/utils/accounting.h"
#include "arki/utils/string.h"
#include "arki/utils/sys.h"
#include <wibble/stream/posix.h>

#include <memory>
#include <sstream>
#include <iostream>

namespace tut {
using namespace std;
using namespace arki;
using namespace arki::types;
using namespace arki::utils;
using namespace wibble::tests;

struct arki_dataset_shar {
    ConfigFile config;
    testdata::GRIBData tdata_grib;
    testdata::BUFRData tdata_bufr;
    testdata::VM2Data tdata_vm2;
    testdata::ODIMData tdata_odim;

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
	unique_ptr<ReadonlyDataset> testds(ReadonlyDataset::create(*config.section("error")));
	ensure(dynamic_cast<dataset::simple::Reader*>(testds.get()) != 0);
}

template<> template<>
void to::test<2>()
{
	unique_ptr<WritableDataset> testds(WritableDataset::create(*config.section("error")));
	ensure(dynamic_cast<dataset::simple::Writer*>(testds.get()) != 0);
}

template<> template<>
void to::test<3>()
{
	unique_ptr<ReadonlyDataset> testds(ReadonlyDataset::create(*config.section("duplicates")));
	ensure(dynamic_cast<dataset::simple::Reader*>(testds.get()) != 0);
}

template<> template<>
void to::test<4>()
{
	unique_ptr<WritableDataset> testds(WritableDataset::create(*config.section("duplicates")));
	ensure(dynamic_cast<dataset::simple::Writer*>(testds.get()) != 0);
}

template<> template<>
void to::test<5>()
{
	unique_ptr<ReadonlyDataset> testds(ReadonlyDataset::create(*config.section("outbound")));
	ensure(dynamic_cast<dataset::Empty*>(testds.get()) != 0);
}

template<> template<>
void to::test<6>()
{
	unique_ptr<WritableDataset> testds(WritableDataset::create(*config.section("outbound")));
	ensure(dynamic_cast<dataset::Outbound*>(testds.get()) != 0);
}

template<> template<>
void to::test<7>()
{
	unique_ptr<ReadonlyDataset> testds(ReadonlyDataset::create(*config.section("discard")));
	ensure(dynamic_cast<dataset::Empty*>(testds.get()) != 0);
}

template<> template<>
void to::test<8>()
{
	unique_ptr<WritableDataset> testds(WritableDataset::create(*config.section("discard")));
	ensure(dynamic_cast<dataset::Discard*>(testds.get()) != 0);
}

template<> template<>
void to::test<9>()
{
	unique_ptr<ReadonlyDataset> testds(ReadonlyDataset::create(*config.section("simple")));
	ensure(dynamic_cast<dataset::simple::Reader*>(testds.get()) != 0);
}

template<> template<>
void to::test<10>()
{
	unique_ptr<WritableDataset> testds(WritableDataset::create(*config.section("simple")));
	ensure(dynamic_cast<dataset::simple::Writer*>(testds.get()) != 0);
}

template<> template<>
void to::test<11>()
{
	unique_ptr<ReadonlyDataset> testds(ReadonlyDataset::create(*config.section("ondisk2")));
	ensure(dynamic_cast<dataset::ondisk2::Reader*>(testds.get()) != 0);
}

template<> template<>
void to::test<12>()
{
	unique_ptr<WritableDataset> testds(WritableDataset::create(*config.section("ondisk2")));
	ensure(dynamic_cast<dataset::ondisk2::Writer*>(testds.get()) != 0);
}

template<> template<>
void to::test<13>()
{
	unique_ptr<WritableDataset> testds(WritableDataset::create(*config.section("test")));
	ensure(dynamic_cast<dataset::ondisk2::Writer*>(testds.get()) != 0);
}

struct TestDataset
{
    const testdata::Fixture& td;
    ConfigFile config;
    ConfigFile* cfgtest;
    std::string path;
    bool smallfiles;

    TestDataset(const testdata::Fixture& td, const std::string& conf)
        : td(td)
    {
        stringstream incfg(conf);
        config.parse(incfg, "(memory)");
        cfgtest = config.section("test");
        path = cfgtest->value("path");
        smallfiles = ConfigFile::boolValue(cfgtest->value("smallfiles")) ||
            (td.format == "vm2" && cfgtest->value("type") == "simple");
    }

    void test_import(WIBBLE_TEST_LOCPRM)
    {
        // Clear everything
        if (sys::isdir(path)) sys::rmtree(path);

        unique_ptr<WritableDataset> ds(WritableDataset::create(*cfgtest));

        for (unsigned i = 0; i < 3; ++i)
        {
            Metadata md = td.test_data[i].md;
            wassert(actual(ds->acquire(md)) == WritableDataset::ACQ_OK);
            wassert(actual(str::joinpath(path, td.test_data[i].destfile)).fileexists());
            wassert(actual(md.sourceBlob().filename).endswith(td.test_data[i].destfile));
        }
    }

    void test_querydata(WIBBLE_TEST_LOCPRM)
    {
        unique_ptr<ReadonlyDataset> ds(ReadonlyDataset::create(*cfgtest));

        acct::plain_data_read_count.reset();

        // Query everything, we should get 3 results
        metadata::Collection mdc;
        ds->queryData(dataset::DataQuery(Matcher::parse("")), mdc);
        wassert(actual(mdc.size()) == 3);

        // No data should have been read in this query
        wassert(actual(acct::plain_data_read_count.val()) == 0);

        for (unsigned i = 0; i < 3; ++i)
        {
            using namespace arki::types;

            // Check that what we imported can be queried
            metadata::Collection mdc;
            ds->queryData(dataset::DataQuery(td.test_data[i].matcher), mdc);
            wassert(actual(mdc.size()) == 1u);

            // Check that the result matches what was imported
            const source::Blob& s1 = td.test_data[i].md.sourceBlob();
            const source::Blob& s2 = mdc[0].sourceBlob();
            wassert(actual(s2.format) == s1.format);
            wassert(actual(s2.size) == s1.size);
            wassert(actual(mdc[0]).is_similar(td.test_data[i].md));

            wassert(actual(td.test_data[i].matcher(mdc[0])).istrue());

            // Check that the data can be loaded
            const auto& data = mdc[0].getData();
            wassert(actual(data.size()) == s1.size);
        }

        // 3 data items should have been read
        if (smallfiles)
            wassert(actual(acct::plain_data_read_count.val()) == 0);
        else
            wassert(actual(acct::plain_data_read_count.val()) == 3);
    }

    void test_querysummary(WIBBLE_TEST_LOCPRM)
    {
        unique_ptr<ReadonlyDataset> ds(ReadonlyDataset::create(*cfgtest));

        // Query summary of everything
        {
            Summary s;
            ds->querySummary(Matcher(), s);
            wassert(actual(s.count()) == 3);
        }

        for (unsigned i = 0; i < 3; ++i)
        {
            using namespace arki::types;

            // Query summary of single items
            Summary s;
            ds->querySummary(td.test_data[i].matcher, s);

            wassert(actual(s.count()) == 1u);

            const source::Blob& s1 = td.test_data[i].md.sourceBlob();
            wassert(actual(s.size()) == s1.size);
        }
    }

    void test_querybytes(WIBBLE_TEST_LOCPRM)
    {
        unique_ptr<ReadonlyDataset> ds(ReadonlyDataset::create(*cfgtest));

        for (unsigned i = 0; i < 3; ++i)
        {
            using namespace arki::types;

            dataset::ByteQuery bq;
            bq.setData(td.test_data[i].matcher);
            std::stringstream os;
            ds->queryBytes(bq, os);

            // Write it out and rescan
            sys::write_file("testdata", os.str());
            metadata::Collection tmp;
            wassert(actual(scan::scan("testdata", tmp, td.test_data[i].md.source().format)).istrue());

            // Ensure that what we rescanned is what was imported
            wassert(actual(tmp.size()) == 1u);
            wassert(actual(tmp[0]).is_similar(td.test_data[i].md));

            //UItem<source::Blob> s1 = input_data[i].source.upcast<source::Blob>();
            //wassert(actual(os.str().size()) == s1->size);
        }
    }

    void test_querybytes_integrity(WIBBLE_TEST_LOCPRM)
    {
        unique_ptr<ReadonlyDataset> ds(ReadonlyDataset::create(*cfgtest));

        // Query everything
        dataset::ByteQuery bq;
        bq.setData(Matcher());
        std::stringstream os;
        ds->queryBytes(bq, os);

        // Check that what we got matches the total size of what we imported
        size_t total_size = 0;
        for (unsigned i = 0; i < 3; ++i)
            total_size += td.test_data[i].md.sourceBlob().size;
        // We use >= and not == because some data sources add extra information
        // to data, like line endings for VM2
        wassert(actual(os.str().size()) >= total_size);

        // Write the results to disk
        sys::write_file("tempdata", os.str());

        // Check that they can be scanned again
        metadata::Collection mdc;
        wassert(actual(scan::scan("tempdata", mdc, td.format)).istrue());

        sys::unlink("tempdata");
    }

    void test_postprocess(WIBBLE_TEST_LOCPRM)
    {
        unique_ptr<ReadonlyDataset> ds(ReadonlyDataset::create(*cfgtest));

        // Do a simple export first, to get the exact metadata that would come
        // out
        metadata::Collection mdc;
        ds->queryData(dataset::DataQuery(td.test_data[0].matcher), mdc);
        wassert(actual(mdc.size()) == 1u);

        // Then do a postprocessed queryBytes

        // Send the script error to stderr. Use dup() because PosixBuf will
        // close its file descriptor at destruction time
        wibble::stream::PosixBuf pb(dup(2));
        ostream os(&pb);
        dataset::ByteQuery bq;
        bq.setPostprocess(td.test_data[0].matcher, "testcountbytes");
        ds->queryBytes(bq, os);

        // Verify that the data that was output was exactly as long as the
        // encoded metadata and its data
        string out = sys::read_file("testcountbytes.out");
        unique_ptr<Metadata> copy(mdc[0].clone());
        copy->makeInline();
        char buf[32];
        snprintf(buf, 32, "%d\n", copy->encodeBinary().size() + copy->data_size());
        wassert(actual(out) == buf);
    }

    void test_locked(WIBBLE_TEST_LOCPRM)
    {
        // Lock a dataset for writing
        unique_ptr<WritableDataset> wds(WritableDataset::create(*cfgtest));
        Pending p = wds->test_writelock();

        // Try to read from it, it should still work with WAL
        unique_ptr<ReadonlyDataset> rds(ReadonlyDataset::create(*cfgtest));
        dataset::ByteQuery bq;
        bq.setData(Matcher());
        std::stringstream os;
        rds->queryBytes(bq, os);
    }

    void test_all(WIBBLE_TEST_LOCPRM)
    {
        wruntest(test_import);
        wruntest(test_querydata);
        wruntest(test_querysummary);
        wruntest(test_querybytes);
        wruntest(test_querybytes_integrity);
        wruntest(test_postprocess);
        wruntest(test_locked);
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
        "index = origin, reftime\n"
        "unique = reftime, origin, product, level, timerange, area\n"
        "name = test\n"
        "path = test\n"
    );
    wruntest(tds.test_all);
}

// ondisk2 GRIB dirsegments
template<> template<>
void to::test<15>()
{
    TestDataset tds(tdata_grib,
        "[test]\n"
        "type = ondisk2\n"
        "step = daily\n"
        "index = origin, reftime\n"
        "unique = reftime, origin, product, level, timerange, area\n"
        "segments = dir\n"
        "name = test\n"
        "path = test\n"
    );
    wruntest(tds.test_all);
}

// ondisk2 BUFR
template<> template<>
void to::test<16>()
{
    TestDataset tds(tdata_bufr,
        "[test]\n"
        "type = ondisk2\n"
        "step = daily\n"
        "index = origin, reftime\n"
        "unique = reftime, origin, product, level, timerange, area\n"
        "name = test\n"
        "path = test\n"
    );
    wruntest(tds.test_all);
}

// ondisk2 BUFR dirsegments
template<> template<>
void to::test<17>()
{
    TestDataset tds(tdata_bufr,
        "[test]\n"
        "type = ondisk2\n"
        "step = daily\n"
        "index = origin, reftime\n"
        "unique = reftime, origin, product, level, timerange, area\n"
        "segments = dir\n"
        "name = test\n"
        "path = test\n"
    );
    wruntest(tds.test_all);
}

// ondisk2 VM2
template<> template<>
void to::test<18>()
{
    TestDataset tds(tdata_vm2,
        "[test]\n"
        "type = ondisk2\n"
        "step = daily\n"
        "index = origin, reftime\n"
        "unique = reftime, origin, product, level, timerange, area\n"
        "name = test\n"
        "path = test\n"
        "smallfiles = true\n"
    );
    wruntest(tds.test_all);
}

// ondisk2 VM2 dirsegments
template<> template<>
void to::test<19>()
{
    TestDataset tds(tdata_vm2,
        "[test]\n"
        "type = ondisk2\n"
        "step = daily\n"
        "index = origin, reftime\n"
        "unique = reftime, origin, product, level, timerange, area\n"
        "segments = dir\n"
        "name = test\n"
        "path = test\n"
        "smallfiles = true\n"
    );
    wruntest(tds.test_all);
}

// ondisk2 ODIM
template<> template<>
void to::test<20>()
{
    TestDataset tds(tdata_odim,
        "[test]\n"
        "type = ondisk2\n"
        "step = daily\n"
        "index = origin, reftime\n"
        "unique = reftime, origin, product, level, timerange, area\n"
        "name = test\n"
        "path = test\n"
    );
    wruntest(tds.test_all);
}

// ondisk2 ODIM dirsegments
template<> template<>
void to::test<21>()
{
    TestDataset tds(tdata_odim,
        "[test]\n"
        "type = ondisk2\n"
        "step = daily\n"
        "index = origin, reftime\n"
        "unique = reftime, origin, product, level, timerange, area\n"
        "segments = dir\n"
        "name = test\n"
        "path = test\n"
    );
    wruntest(tds.test_all);
}

// simple plainmft GRIB
template<> template<>
void to::test<22>()
{
    TestDataset tds(tdata_grib,
        "[test]\n"
        "type = simple\n"
        "step = daily\n"
        "index = origin, reftime\n"
        "unique = reftime, origin, product, level, timerange, area\n"
        "name = test\n"
        "path = test\n"
    );
    wruntest(tds.test_all);
}

// simple plainmft GRIB dirsegments
template<> template<>
void to::test<23>()
{
    TestDataset tds(tdata_grib,
        "[test]\n"
        "type = simple\n"
        "step = daily\n"
        "index = origin, reftime\n"
        "unique = reftime, origin, product, level, timerange, area\n"
        "segments = dir\n"
        "name = test\n"
        "path = test\n"
    );
    wruntest(tds.test_all);
}

// simple sqlitemft GRIB
template<> template<>
void to::test<24>()
{
    TestDataset tds(tdata_grib,
        "[test]\n"
        "type = simple\n"
        "step = daily\n"
        "index = origin, reftime\n"
        "unique = reftime, origin, product, level, timerange, area\n"
        "name = test\n"
        "path = test\n"
        "index_type = sqlite\n"
    );
    wruntest(tds.test_all);
}

// simple sqlitemft GRIB dirsegments
template<> template<>
void to::test<25>()
{
    TestDataset tds(tdata_grib,
        "[test]\n"
        "type = simple\n"
        "step = daily\n"
        "index = origin, reftime\n"
        "unique = reftime, origin, product, level, timerange, area\n"
        "segments = dir\n"
        "name = test\n"
        "path = test\n"
        "index_type = sqlite\n"
    );
    wruntest(tds.test_all);
}

// simple plainmft BUFR
template<> template<>
void to::test<26>()
{
    TestDataset tds(tdata_bufr,
        "[test]\n"
        "type = simple\n"
        "step = daily\n"
        "index = origin, reftime\n"
        "unique = reftime, origin, product, level, timerange, area\n"
        "name = test\n"
        "path = test\n"
    );
    wruntest(tds.test_all);
}

// simple plainmft BUFR dirsegments
template<> template<>
void to::test<27>()
{
    TestDataset tds(tdata_bufr,
        "[test]\n"
        "type = simple\n"
        "step = daily\n"
        "index = origin, reftime\n"
        "unique = reftime, origin, product, level, timerange, area\n"
        "segments = dir\n"
        "name = test\n"
        "path = test\n"
    );
    wruntest(tds.test_all);
}

// simple sqlitemft BUFR
template<> template<>
void to::test<28>()
{
    TestDataset tds(tdata_bufr,
        "[test]\n"
        "type = simple\n"
        "step = daily\n"
        "index = origin, reftime\n"
        "unique = reftime, origin, product, level, timerange, area\n"
        "name = test\n"
        "path = test\n"
        "index_type = sqlite\n"
    );
    wruntest(tds.test_all);
}

// simple sqlitemft BUFR dirsegments
template<> template<>
void to::test<29>()
{
    TestDataset tds(tdata_bufr,
        "[test]\n"
        "type = simple\n"
        "step = daily\n"
        "index = origin, reftime\n"
        "unique = reftime, origin, product, level, timerange, area\n"
        "segments = dir\n"
        "name = test\n"
        "path = test\n"
        "index_type = sqlite\n"
    );
    wruntest(tds.test_all);
}

// simple plainmft VM2
template<> template<>
void to::test<30>()
{
    TestDataset tds(tdata_vm2,
        "[test]\n"
        "type = simple\n"
        "step = daily\n"
        "index = origin, reftime\n"
        "unique = reftime, origin, product, level, timerange, area\n"
        "name = test\n"
        "path = test\n"
    );
    wruntest(tds.test_all);
}

// simple plainmft VM2 dirsegments
template<> template<>
void to::test<31>()
{
    TestDataset tds(tdata_vm2,
        "[test]\n"
        "type = simple\n"
        "step = daily\n"
        "index = origin, reftime\n"
        "unique = reftime, origin, product, level, timerange, area\n"
        "segments = dir\n"
        "name = test\n"
        "path = test\n"
    );
    wruntest(tds.test_all);
}

// simple sqlitemft VM2
template<> template<>
void to::test<32>()
{
    TestDataset tds(tdata_vm2,
        "[test]\n"
        "type = simple\n"
        "step = daily\n"
        "index = origin, reftime\n"
        "unique = reftime, origin, product, level, timerange, area\n"
        "name = test\n"
        "path = test\n"
        "index_type = sqlite\n"
    );
    wruntest(tds.test_all);
}

// simple sqlitemft VM2 dirsegments
template<> template<>
void to::test<33>()
{
    TestDataset tds(tdata_vm2,
        "[test]\n"
        "type = simple\n"
        "step = daily\n"
        "index = origin, reftime\n"
        "unique = reftime, origin, product, level, timerange, area\n"
        "segments = dir\n"
        "name = test\n"
        "path = test\n"
        "index_type = sqlite\n"
    );
    wruntest(tds.test_all);
}

// simple plainmft ODIM
template<> template<>
void to::test<34>()
{
    TestDataset tds(tdata_odim,
        "[test]\n"
        "type = simple\n"
        "step = daily\n"
        "index = origin, reftime\n"
        "unique = reftime, origin, product, level, timerange, area\n"
        "name = test\n"
        "path = test\n"
    );
    wruntest(tds.test_all);
}

// simple plainmft ODIM dirsegments
template<> template<>
void to::test<35>()
{
    TestDataset tds(tdata_odim,
        "[test]\n"
        "type = simple\n"
        "step = daily\n"
        "index = origin, reftime\n"
        "unique = reftime, origin, product, level, timerange, area\n"
        "segments = dir\n"
        "name = test\n"
        "path = test\n"
    );
    wruntest(tds.test_all);
}

// simple sqlitemft ODIM
template<> template<>
void to::test<36>()
{
    TestDataset tds(tdata_odim,
        "[test]\n"
        "type = simple\n"
        "step = daily\n"
        "index = origin, reftime\n"
        "unique = reftime, origin, product, level, timerange, area\n"
        "name = test\n"
        "path = test\n"
        "index_type = sqlite\n"
    );
    wruntest(tds.test_all);
}

// simple sqlitemft ODIM dirsegments
template<> template<>
void to::test<37>()
{
    TestDataset tds(tdata_odim,
        "[test]\n"
        "type = simple\n"
        "step = daily\n"
        "index = origin, reftime\n"
        "unique = reftime, origin, product, level, timerange, area\n"
        "segments = dir\n"
        "name = test\n"
        "path = test\n"
        "index_type = sqlite\n"
    );
    wruntest(tds.test_all);
}

}
