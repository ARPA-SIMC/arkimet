#include "arki/dataset/tests.h"
#include "arki/dataset.h"
#include "arki/metadata/collection.h"
#include "arki/types/source.h"
#include "arki/types/source/blob.h"
#include "arki/scan/any.h"
#include "arki/configfile.h"
#include "arki/utils/files.h"
#include "arki/utils/accounting.h"
#include "arki/utils/string.h"
#include "arki/utils/sys.h"
#include <sys/fcntl.h>

using namespace std;
using namespace arki;
using namespace arki::utils;
using namespace arki::tests;

namespace {

template<class Data>
struct FixtureWriter : public DatasetTest
{
    using DatasetTest::DatasetTest;

    Data td;

    void test_setup()
    {
        DatasetTest::test_setup(R"(
            step = daily
            unique = product, area, reftime
        )");
    }

    bool smallfiles() const
    {
        return ConfigFile::boolValue(cfg.value("smallfiles")) ||
            (td.format == "vm2" && cfg.value("type") == "simple");
    }
};


template<class Data>
class TestsWriter : public FixtureTestCase<FixtureWriter<Data>>
{
    using FixtureTestCase<FixtureWriter<Data>>::FixtureTestCase;

    void register_tests() override;
};

TestsWriter<testdata::GRIBData> test_writer_grib_ondisk2("arki_dataset_writer_grib_ondisk2", "type=ondisk2\n");
TestsWriter<testdata::GRIBData> test_writer_grib_simple_plain("arki_dataset_writer_grib_simple_plain", "type=simple\nindex_type=plain\n");
TestsWriter<testdata::GRIBData> test_writer_grib_simple_sqlite("arki_dataset_writer_grib_simple_sqlite", "type=simple\nindex_type=sqlite");
TestsWriter<testdata::BUFRData> test_writer_bufr_ondisk2("arki_dataset_writer_bufr_ondisk2", "type=ondisk2\n");
TestsWriter<testdata::BUFRData> test_writer_bufr_simple_plain("arki_dataset_writer_bufr_simple_plain", "type=simple\nindex_type=plain\n");
TestsWriter<testdata::BUFRData> test_writer_bufr_simple_sqlite("arki_dataset_writer_bufr_simple_sqlite", "type=simple\nindex_type=sqlite");
TestsWriter<testdata::VM2Data> test_writer_vm2_ondisk2("arki_dataset_writer_vm2_ondisk2", "type=ondisk2\n");
TestsWriter<testdata::VM2Data> test_writer_vm2_simple_plain("arki_dataset_writer_vm2_simple_plain", "type=simple\nindex_type=plain\n");
TestsWriter<testdata::VM2Data> test_writer_vm2_simple_sqlite("arki_dataset_writer_vm2_simple_sqlite", "type=simple\nindex_type=sqlite");
TestsWriter<testdata::ODIMData> test_writer_odim_ondisk2("arki_dataset_writer_odim_ondisk2", "type=ondisk2\n");
TestsWriter<testdata::ODIMData> test_writer_odim_simple_plain("arki_dataset_writer_odim_simple_plain", "type=simple\nindex_type=plain\n");
TestsWriter<testdata::ODIMData> test_writer_odim_simple_sqlite("arki_dataset_writer_odim_simple_sqlite", "type=simple\nindex_type=sqlite");

template<class Data>
void TestsWriter<Data>::register_tests() {

typedef FixtureWriter<Data> Fixture;

this->add_method("import", [](Fixture& f) {
    auto ds(f.makeWriter());

    for (unsigned i = 0; i < 3; ++i)
    {
        Metadata md = f.td.test_data[i].md;
        wassert(actual(ds->acquire(md)) == dataset::Writer::ACQ_OK);
        wassert(actual_file(str::joinpath(f.ds_root, f.td.test_data[i].destfile)).exists());
        wassert(actual_type(md.source()).is_source_blob(f.td.format, f.ds_root, f.td.test_data[i].destfile));
    }
});


}
}
