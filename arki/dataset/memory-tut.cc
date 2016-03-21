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
using namespace arki::tests;
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
        scan::scan("inbound/test.grib1", c.inserter_func());
    }
};
TESTGRP(arki_dataset_memory);

// Test querying
def_test(1)
{
    acquireSamples();

    metadata::Collection mdc(c, Matcher::parse("origin:GRIB1,200"));
    ensure_equals(mdc.size(), 1u);
    wassert(actual(mdc[0].source().cloneType()).is_source_blob("grib", sys::abspath("."), "inbound/test.grib1", 0, 7218));

    mdc.clear();

    mdc.add(c, Matcher::parse("origin:GRIB1,80"));
    ensure_equals(mdc.size(), 1u);
    wassert(actual(mdc[0].source().cloneType()).is_source_blob("grib", sys::abspath("."), "inbound/test.grib1", 7218, 34960u));

    mdc.clear();
    mdc.add(c, Matcher::parse("origin:GRIB1,98"));
    ensure_equals(mdc.size(), 1u);
    wassert(actual(mdc[0].source().cloneType()).is_source_blob("grib", sys::abspath("."), "inbound/test.grib1", 42178, 2234));
}

// Test querying the summary
def_test(2)
{
    acquireSamples();
    Summary summary;
    c.query_summary(Matcher::parse("origin:GRIB1,200"), summary);
    ensure_equals(summary.count(), 1u);
}

// Test querying the summary by reftime
def_test(3)
{
    acquireSamples();
    Summary summary;
    //system("bash");
    c.query_summary(Matcher::parse("reftime:>=2007-07"), summary);
    ensure_equals(summary.count(), 3u);
}

}
