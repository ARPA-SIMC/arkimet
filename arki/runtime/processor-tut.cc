#include "config.h"

#include "processor.h"
#include "io.h"
#include <arki/dataset.h>
#include <arki/dataset/tests.h>
#include <arki/scan/any.h>
#include <wibble/sys/fs.h>

namespace tut {
using namespace std;
using namespace wibble::sys;
using namespace wibble::tests;
using namespace arki;
using namespace arki::runtime;

struct arki_processor_shar {
    unique_ptr<ReadonlyDataset> build_dataset()
    {
        tests::DatasetTest dt;
        if (fs::isdir("test")) fs::rmtree("test");
        dt.cfg.setValue("type", "ondisk2");
        dt.cfg.setValue("step", "daily");
        dt.cfg.setValue("index", "origin, reftime");
        dt.cfg.setValue("unique", "reftime, origin, product, level, timerange, area");
        dt.cfg.setValue("path", "test");

        testdata::GRIBData fixture;
        wruntest(dt.import_all, fixture);
        return unique_ptr<ReadonlyDataset>(dt.makeReader());
    }

    void run_maker(ProcessorMaker& pm, Matcher matcher=Matcher())
    {
        unique_ptr<ReadonlyDataset> ds(build_dataset());

        if (fs::exists("pm-out"))
            fs::unlink("pm-out");

        runtime::Output out("pm-out");
        unique_ptr<DatasetProcessor> dp(pm.make(matcher, out));
        dp->process(*ds, "test");
        dp->end();
    }
};
TESTGRP(arki_processor);

// Export only binary metadata (the default)
template<> template<>
void to::test<1>()
{
    ProcessorMaker pm;
    run_maker(pm);

    metadata::Collection mdc;
    Metadata::readFile("pm-out", mdc);

    wassert(actual(mdc.size()) == 3);
    wassert(actual_type(mdc[0].source()).is_source_blob("grib1", "", fs::abspath("test/2007/07-07.grib1"), 0, 34960));
    wassert(actual_type(mdc[1].source()).is_source_blob("grib1", "", fs::abspath("test/2007/07-08.grib1"), 0, 7218));
    wassert(actual_type(mdc[2].source()).is_source_blob("grib1", "", fs::abspath("test/2007/10-09.grib1"), 0, 2234));
}

// Export inline data
template<> template<>
void to::test<2>()
{
    ProcessorMaker pm;
    pm.data_inline = true;
    run_maker(pm);

    metadata::Collection mdc;
    Metadata::readFile("pm-out", mdc);

    wassert(actual(mdc.size()) == 3);
    wassert(actual_type(mdc[0].source()).is_source_inline("grib1", 34960));
    wassert(actual_type(mdc[1].source()).is_source_inline("grib1", 7218));
    wassert(actual_type(mdc[2].source()).is_source_inline("grib1", 2234));
    wassert(actual(mdc[0].getData().size()) == 34960);
    wassert(actual(mdc[1].getData().size()) == 7218);
    wassert(actual(mdc[2].getData().size()) == 2234);
}

// Export data only
template<> template<>
void to::test<3>()
{
    ProcessorMaker pm;
    pm.data_only = true;
    run_maker(pm);

    metadata::Collection mdc;
    scan::scan("pm-out", mdc, "grib");

    wassert(actual(mdc.size()) == 3);
    wassert(actual_type(mdc[0].source()).is_source_blob("grib1", fs::abspath("."), "pm-out", 0, 34960));
    wassert(actual_type(mdc[1].source()).is_source_blob("grib1", fs::abspath("."), "pm-out", 34960, 7218));
    wassert(actual_type(mdc[2].source()).is_source_blob("grib1", fs::abspath("."), "pm-out", 34960 + 7218, 2234));
}

}
