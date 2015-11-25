#include <arki/types/tests.h>
#include "memory.h"
#include <arki/metadata.h>
#include <arki/utils.h>
#include <arki/utils/accounting.h>
#include <arki/types/source.h>
#include <arki/types/source/blob.h>
#include <arki/summary.h>
#include <arki/scan/any.h>
#include <arki/runtime/io.h>
#include <arki/utils/sys.h>
#include <cstring>
#include <sstream>
#include <iostream>

namespace tut {
using namespace std;
using namespace wibble::tests;
using namespace arki;
using namespace arki::types;
using namespace arki::metadata;
using namespace arki::utils;

struct arki_dataset_memory_shar {
    dataset::Memory c;

    arki_dataset_memory_shar()
    {
    }

    void acquireSamples()
    {
        scan::scan("inbound/test.grib1", c);
    }
};
TESTGRP(arki_dataset_memory);

// Test querying
template<> template<>
void to::test<1>()
{
    acquireSamples();

    dataset::Memory mdc;

    c.queryData(dataset::DataQuery(Matcher::parse("origin:GRIB1,200")), mdc);
    ensure_equals(mdc.size(), 1u);
    wassert(actual(mdc[0].source().cloneType()).is_source_blob("grib1", sys::abspath("."), "inbound/test.grib1", 0, 7218));

    mdc.clear();

    c.queryData(dataset::DataQuery(Matcher::parse("origin:GRIB1,80")), mdc);
    ensure_equals(mdc.size(), 1u);
    wassert(actual(mdc[0].source().cloneType()).is_source_blob("grib1", sys::abspath("."), "inbound/test.grib1", 7218, 34960u));

    mdc.clear();
    c.queryData(dataset::DataQuery(Matcher::parse("origin:GRIB1,98")), mdc);
    ensure_equals(mdc.size(), 1u);
    wassert(actual(mdc[0].source().cloneType()).is_source_blob("grib1", sys::abspath("."), "inbound/test.grib1", 42178, 2234));
}

// Test querying the summary
template<> template<>
void to::test<2>()
{
    acquireSamples();
    Summary summary;
    c.querySummary(Matcher::parse("origin:GRIB1,200"), summary);
    ensure_equals(summary.count(), 1u);
}

// Test querying the summary by reftime
template<> template<>
void to::test<3>()
{
    acquireSamples();
    Summary summary;
    //system("bash");
    c.querySummary(Matcher::parse("reftime:>=2007-07"), summary);
    ensure_equals(summary.count(), 3u);
}

}
