#include "arki/types/tests.h"
#include "arki/core/file.h"
#include "arki/types/source.h"
#include "arki/metadata.h"
#include "arki/dataset/session.h"
#include "arki/summary.h"
#include "arki/segment/data.h"
#include "arki/utils/sys.h"
#include "memory.h"

namespace {
using namespace std;
using namespace arki;
using namespace arki::tests;
using namespace arki::utils;

class Tests : public TestCase
{
    using TestCase::TestCase;

    void register_tests() override;
} test("arki_dataset_memory");

void Tests::register_tests() {

// Test querying
add_method("query", [] {
    auto session = make_shared<dataset::Session>();
    auto segment = session->segment(DataFormat::GRIB, std::filesystem::current_path(), "inbound/test.grib1");
    auto reader = segment->detect_data_reader(std::make_shared<core::lock::Null>());
    auto ds = std::make_shared<dataset::memory::Dataset>(session);
    reader->scan(ds->inserter_func());

    metadata::Collection mdc(*ds, "origin:GRIB1,200");
    wassert(actual(mdc.size()) == 1u);
    wassert(actual(mdc[0].source().cloneType()).is_source_blob(DataFormat::GRIB, std::filesystem::current_path(), "inbound/test.grib1", 0, 7218));

    mdc.clear();

    mdc.add(*ds, "origin:GRIB1,80");
    wassert(actual(mdc.size()) == 1u);
    wassert(actual(mdc[0].source().cloneType()).is_source_blob(DataFormat::GRIB, std::filesystem::current_path(), "inbound/test.grib1", 7218, 34960u));

    mdc.clear();
    mdc.add(*ds, "origin:GRIB1,98");
    wassert(actual(mdc.size()) == 1u);
    wassert(actual(mdc[0].source().cloneType()).is_source_blob(DataFormat::GRIB, std::filesystem::current_path(), "inbound/test.grib1", 42178, 2234));
});

// Test querying the summary
add_method("query_summary", [] {
    auto session = make_shared<dataset::Session>();
    auto segment = session->segment(DataFormat::GRIB, std::filesystem::current_path(), "inbound/test.grib1");
    auto reader = segment->detect_data_reader(std::make_shared<core::lock::Null>());
    auto ds = std::make_shared<dataset::memory::Dataset>(session);
    reader->scan(ds->inserter_func());

    Summary summary;
    ds->create_reader()->query_summary("origin:GRIB1,200", summary);
    wassert(actual(summary.count()) == 1u);
});

// Test querying the summary by reftime
add_method("query_summary_reftime", [] {
    auto session = make_shared<dataset::Session>();
    auto segment = session->segment(DataFormat::GRIB, std::filesystem::current_path(), "inbound/test.grib1");
    auto reader = segment->detect_data_reader(std::make_shared<core::lock::Null>());
    auto ds = std::make_shared<dataset::memory::Dataset>(session);
    reader->scan(ds->inserter_func());

    Summary summary;
    ds->create_reader()->query_summary("reftime:>=2007-07", summary);
    wassert(actual(summary.count()) == 3u);
});

}

}
