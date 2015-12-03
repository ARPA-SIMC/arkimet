#include "config.h"
#include <arki/dataset/tests.h>
#include <arki/dataset/ondisk2.h>
#include <arki/dataset/test-scenario.h>
#include <arki/configfile.h>
#include <arki/metadata.h>
#include <arki/metadata/collection.h>
#include <arki/matcher.h>
#include <arki/summary.h>
#include <arki/utils.h>
#include <arki/utils/files.h>
#include <arki/utils/sys.h>
#include <arki/iotrace.h>
#include <unistd.h>
#include <sstream>
#include <iostream>

namespace tut {
using namespace std;
using namespace arki;
using namespace arki::dataset;
using namespace arki::utils;
using namespace arki::tests;

struct arki_dataset_ondisk2_reader_shar {
    arki_dataset_ondisk2_reader_shar()
    {
    }
};
TESTGRP(arki_dataset_ondisk2_reader);

// Test querying with postprocessing
template<> template<>
void to::test<1>()
{
    const test::Scenario& s = test::Scenario::get("ondisk2-testgrib1");
    unique_ptr<ondisk2::Reader> reader(new ondisk2::Reader(s.cfg));
    ensure(reader->hasWorkingIndex());

    dataset::ByteQuery bq;
    bq.setPostprocess(Matcher::parse("origin:GRIB1,200"), "testcountbytes");
    reader->query_bytes(bq, 2);

    string out = sys::read_file("testcountbytes.out");
    ensure_equals(out, "7427\n");
}

// Test that summary files are not created for all the extent of the query, but
// only for data present in the dataset
template<> template<>
void to::test<2>()
{
    const test::Scenario& scen = test::Scenario::get("ondisk2-testgrib1");
    ConfigFile cfg = scen.clone("testds");
    // Empty the summary cache
    system("rm testds/.summaries/*");

    unique_ptr<ondisk2::Reader> reader(new ondisk2::Reader(cfg));

	Summary s;
	reader->querySummary(Matcher::parse("reftime:=2007"), s);
	ensure_equals(s.count(), 3u);
	ensure_equals(s.size(), 44412u);

    // Global summary is not built because we only query specific months
    ensure(!sys::access("testds/.summaries/all.summary", F_OK));

    ensure(!sys::access("testds/.summaries/2007-01.summary", F_OK));
    ensure(!sys::access("testds/.summaries/2007-02.summary", F_OK));
    ensure(!sys::access("testds/.summaries/2007-03.summary", F_OK));
    ensure(!sys::access("testds/.summaries/2007-04.summary", F_OK));
    ensure(!sys::access("testds/.summaries/2007-05.summary", F_OK));
    ensure(!sys::access("testds/.summaries/2007-06.summary", F_OK));
    ensure(sys::access("testds/.summaries/2007-07.summary", F_OK));
    // Summary caches corresponding to DB holes are still created and used
    ensure(sys::access("testds/.summaries/2007-08.summary", F_OK));
    ensure(sys::access("testds/.summaries/2007-09.summary", F_OK));
    ensure(sys::access("testds/.summaries/2007-10.summary", F_OK));
    ensure(!sys::access("testds/.summaries/2007-11.summary", F_OK));
    ensure(!sys::access("testds/.summaries/2007-12.summary", F_OK));
    ensure(!sys::access("testds/.summaries/2008-01.summary", F_OK));
}

// Test produce_nth
template<> template<>
void to::test<3>()
{
    const test::Scenario& s = test::Scenario::get("ondisk2-testgrib1");
    unique_ptr<ondisk2::Reader> reader(new ondisk2::Reader(s.cfg));

    {
        metadata::Collection mdc;
        //iotrace::Collector c;
        size_t count = reader->produce_nth(mdc, 0);
        ensure_equals(count, 3u);
        ensure_equals(mdc.size(), 3u);
        //c.dump(cerr);
    }

    {
        metadata::Collection mdc;
        size_t count = reader->produce_nth(mdc, 1);
        ensure_equals(count, 0u);
        ensure_equals(mdc.size(), 0u);
    }
}


}

// vim:set ts=4 sw=4:
