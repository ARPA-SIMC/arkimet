#include "config.h"
#include "processor.h"
#include "io.h"
#include <arki/dataset.h>
#include <arki/dataset/tests.h>
#include <arki/scan/any.h>
#include <arki/utils/sys.h>

namespace tut {
using namespace std;
using namespace arki::tests;
using namespace arki;
using namespace arki::runtime;
using namespace arki::utils;

struct arki_processor_shar {
    unique_ptr<dataset::Reader> build_dataset()
    {
        DatasetTest dt;
        if (sys::isdir("test")) sys::rmtree("test");
        dt.cfg.setValue("type", "ondisk2");
        dt.cfg.setValue("step", "daily");
        dt.cfg.setValue("index", "origin, reftime");
        dt.cfg.setValue("unique", "reftime, origin, product, level, timerange, area");
        dt.cfg.setValue("path", "test");

        testdata::GRIBData fixture;
        wruntest(dt.import_all, fixture);
        return unique_ptr<dataset::Reader>(dt.makeReader());
    }

    void run_maker(ProcessorMaker& pm, Matcher matcher=Matcher())
    {
        unique_ptr<dataset::Reader> ds(build_dataset());

        if (sys::exists("pm-out"))
            sys::unlink("pm-out");

        runtime::File out("pm-out");
        unique_ptr<DatasetProcessor> dp(pm.make(matcher, out));
        dp->process(*ds, "test");
        dp->end();
    }
};
TESTGRP(arki_processor);

// Export only binary metadata (the default)
def_test(1)
{
    ProcessorMaker pm;
    run_maker(pm);

    metadata::Collection mdc;
    mdc.read_from_file("pm-out");

    wassert(actual(mdc.size()) == 3u);
    wassert(actual_type(mdc[0].source()).is_source_blob("grib1", ".", sys::abspath("test/2007/07-07.grib1"), 0, 34960));
    wassert(actual_type(mdc[1].source()).is_source_blob("grib1", ".", sys::abspath("test/2007/07-08.grib1"), 0, 7218));
    wassert(actual_type(mdc[2].source()).is_source_blob("grib1", ".", sys::abspath("test/2007/10-09.grib1"), 0, 2234));
}

// Export inline data
def_test(2)
{
    ProcessorMaker pm;
    pm.data_inline = true;
    run_maker(pm);

    metadata::Collection mdc;
    mdc.read_from_file("pm-out");

    wassert(actual(mdc.size()) == 3u);
    wassert(actual_type(mdc[0].source()).is_source_inline("grib1", 34960));
    wassert(actual_type(mdc[1].source()).is_source_inline("grib1", 7218));
    wassert(actual_type(mdc[2].source()).is_source_inline("grib1", 2234));
    wassert(actual(mdc[0].getData().size()) == 34960u);
    wassert(actual(mdc[1].getData().size()) == 7218u);
    wassert(actual(mdc[2].getData().size()) == 2234u);
}

// Export data only
def_test(3)
{
    ProcessorMaker pm;
    pm.data_only = true;
    run_maker(pm);

    metadata::Collection mdc;
    scan::scan("pm-out", mdc.inserter_func(), "grib");

    wassert(actual(mdc.size()) == 3u);
    wassert(actual_type(mdc[0].source()).is_source_blob("grib1", sys::abspath("."), "pm-out", 0, 34960));
    wassert(actual_type(mdc[1].source()).is_source_blob("grib1", sys::abspath("."), "pm-out", 34960, 7218));
    wassert(actual_type(mdc[2].source()).is_source_blob("grib1", sys::abspath("."), "pm-out", 34960 + 7218, 2234));
}

}
