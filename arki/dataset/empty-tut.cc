#include "config.h"
#include <arki/tests/tests.h>
#include <arki/configfile.h>
#include <arki/metadata.h>
#include <arki/metadata/collection.h>
#include <arki/matcher.h>
#include <arki/dataset/empty.h>
#include <sstream>
#include <iostream>

namespace tut {
using namespace std;
using namespace arki;
using namespace arki::tests;

struct arki_dataset_empty_shar {
};
TESTGRP(arki_dataset_empty);

// Test accessing the data
def_test(1)
{
    ConfigFile cfg;
    dataset::Empty ds(cfg);

    metadata::Collection mdc(ds, Matcher::parse("origin:GRIB1 or BUFR or GRIB2"));
    ensure_equals(mdc.size(), 0u);
}

}
