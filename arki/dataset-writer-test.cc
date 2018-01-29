#include "arki/dataset/tests.h"
#include "arki/dataset.h"
#include "arki/dataset/time.h"
#include "arki/metadata/collection.h"
#include "arki/types/source.h"
#include "arki/types/source/blob.h"
#include "arki/types/reftime.h"
#include "arki/scan/any.h"
#include "arki/configfile.h"
#include "arki/utils/files.h"
#include "arki/utils/accounting.h"
#include "arki/utils/string.h"
#include "arki/utils/sys.h"
#include "arki/exceptions.h"
#include <sys/fcntl.h>

using namespace std;
using namespace arki;
using namespace arki::utils;
using namespace arki::tests;
using namespace arki::core;

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

class Tests : public FixtureTestCase<DatasetTest>
{
    using FixtureTestCase::FixtureTestCase;

    void register_tests() override;
};

Tests test_writer_ondisk2("arki_dataset_writer_ondisk2", "type=ondisk2\n");
Tests test_writer_simple_plain("arki_dataset_writer_simple_plain", "type=simple\nindex_type=plain\n");
Tests test_writer_simple_sqlite("arki_dataset_writer_simple_sqlite", "type=simple\nindex_type=sqlite");
Tests test_writer_iseg("arki_dataset_writer_iseg", "type=iseg\nformat=grib");

template<class Data>
class TestsWriter : public FixtureTestCase<FixtureWriter<Data>>
{
    using FixtureTestCase<FixtureWriter<Data>>::FixtureTestCase;

    void register_tests() override;
};

TestsWriter<testdata::GRIBData> test_writer_grib_ondisk2("arki_dataset_writer_grib_ondisk2", "type=ondisk2\n");
TestsWriter<testdata::GRIBData> test_writer_grib_simple_plain("arki_dataset_writer_grib_simple_plain", "type=simple\nindex_type=plain\n");
TestsWriter<testdata::GRIBData> test_writer_grib_simple_sqlite("arki_dataset_writer_grib_simple_sqlite", "type=simple\nindex_type=sqlite");
TestsWriter<testdata::GRIBData> test_writer_grib_iseg("arki_dataset_writer_grib_iseg", "type=iseg\nformat=grib\n");
TestsWriter<testdata::BUFRData> test_writer_bufr_ondisk2("arki_dataset_writer_bufr_ondisk2", "type=ondisk2\n");
TestsWriter<testdata::BUFRData> test_writer_bufr_simple_plain("arki_dataset_writer_bufr_simple_plain", "type=simple\nindex_type=plain\n");
TestsWriter<testdata::BUFRData> test_writer_bufr_simple_sqlite("arki_dataset_writer_bufr_simple_sqlite", "type=simple\nindex_type=sqlite");
TestsWriter<testdata::BUFRData> test_writer_bufr_iseg("arki_dataset_writer_bufr_iseg", "type=iseg\nformat=bufr\n");
TestsWriter<testdata::VM2Data> test_writer_vm2_ondisk2("arki_dataset_writer_vm2_ondisk2", "type=ondisk2\n");
TestsWriter<testdata::VM2Data> test_writer_vm2_simple_plain("arki_dataset_writer_vm2_simple_plain", "type=simple\nindex_type=plain\n");
TestsWriter<testdata::VM2Data> test_writer_vm2_simple_sqlite("arki_dataset_writer_vm2_simple_sqlite", "type=simple\nindex_type=sqlite");
TestsWriter<testdata::VM2Data> test_writer_vm2_iseg("arki_dataset_writer_vm2_iseg", "type=iseg\nformat=vm2\n");
TestsWriter<testdata::ODIMData> test_writer_odim_ondisk2("arki_dataset_writer_odim_ondisk2", "type=ondisk2\n");
TestsWriter<testdata::ODIMData> test_writer_odim_simple_plain("arki_dataset_writer_odim_simple_plain", "type=simple\nindex_type=plain\n");
TestsWriter<testdata::ODIMData> test_writer_odim_simple_sqlite("arki_dataset_writer_odim_simple_sqlite", "type=simple\nindex_type=sqlite");
TestsWriter<testdata::ODIMData> test_writer_odim_iseg("arki_dataset_writer_odim_iseg", "type=iseg\nformat=odimh5\n");

void Tests::register_tests() {

// Test a dataset with very large mock files in it
add_method("import_largefile", [](Fixture& f) {
    // A dataset with hole files
    f.cfg.setValue("step", "daily");
    f.cfg.setValue("segments", "dir");
    f.cfg.setValue("mockdata", "true");

    {
        // Import 24*30*10Mb=7.2Gb of data
        auto writer = f.config().create_writer();
        for (unsigned day = 1; day <= 30; ++day)
        {
            for (unsigned hour = 0; hour < 24; ++hour)
            {
                Metadata md = testdata::make_large_mock("grib", 10*1024*1024, 12, day, hour);
                wassert(actual(*writer).import(md));
            }
        }
        writer->flush();
    }

    auto checker = f.config().create_checker();
    wassert(actual(*checker).check_clean());

    // Query it, without data
    auto reader = f.config().create_reader();
    metadata::Collection mdc(*reader, Matcher::parse(""));
    wassert(actual(mdc.size()) == 720u);

    // Query it, streaming its data to /dev/null
    sys::File out("/dev/null", O_WRONLY);
    dataset::ByteQuery bq;
    bq.setData(Matcher());
    wassert(reader->query_bytes(bq, out));
});

}


template<class Data>
void TestsWriter<Data>::register_tests() {

typedef FixtureWriter<Data> Fixture;

this->add_method("import", [](Fixture& f) {
    auto ds = f.config().create_writer();

    for (unsigned i = 0; i < 3; ++i)
    {
        Metadata md = f.td.test_data[i].md;
        wassert(actual(*ds).import(md));
        wassert(actual_file(str::joinpath(f.ds_root, f.destfile(f.td.test_data[i]))).exists());
        wassert(actual_type(md.source()).is_source_blob(f.td.format, f.ds_root, f.destfile(f.td.test_data[i])));
    }
});

this->add_method("import_batch_replace_never", [](Fixture& f) {
    auto ds = f.config().create_writer();

    std::vector<shared_ptr<dataset::WriterBatchElement>> batch;
    batch.emplace_back(make_shared<dataset::WriterBatchElement>(f.td.test_data[0].md));
    batch.emplace_back(make_shared<dataset::WriterBatchElement>(f.td.test_data[1].md));
    batch.emplace_back(make_shared<dataset::WriterBatchElement>(f.td.test_data[2].md));
    wassert(ds->acquire_batch(batch, dataset::Writer::REPLACE_NEVER));
    for (unsigned i = 0; i < 3; ++i)
    {
        wassert(actual(batch[i]->result) == dataset::ACQ_OK);
        wassert(actual(batch[i]->dataset_name) == "testds");
        wassert(actual_file(str::joinpath(f.ds_root, f.destfile(f.td.test_data[i]))).exists());
        wassert(actual_type(f.td.test_data[i].md.source()).is_source_blob(f.td.format, f.ds_root, f.destfile(f.td.test_data[i])));
    }

    Metadata mds[3] = { f.td.test_data[0].md, f.td.test_data[1].md, f.td.test_data[2].md };
    batch.clear();
    batch.emplace_back(make_shared<dataset::WriterBatchElement>(mds[0]));
    batch.emplace_back(make_shared<dataset::WriterBatchElement>(mds[1]));
    batch.emplace_back(make_shared<dataset::WriterBatchElement>(mds[2]));
    wassert(ds->acquire_batch(batch, dataset::Writer::REPLACE_NEVER));
    for (unsigned i = 0; i < 3; ++i)
    {
        if (ds->type() == "simple")
        {
            wassert(actual(batch[i]->result) == dataset::ACQ_OK);
            wassert(actual(batch[i]->dataset_name) == "testds");
            wassert(actual(mds[i].sourceBlob().absolutePathname()) == f.td.test_data[i].md.sourceBlob().absolutePathname());
            wassert(actual(mds[i].sourceBlob().offset) > f.td.test_data[i].md.sourceBlob().offset);
            wassert(actual(mds[i].sourceBlob().size) == f.td.test_data[i].md.sourceBlob().size);
        } else {
            wassert(actual(batch[i]->result) == dataset::ACQ_ERROR_DUPLICATE);
            wassert(actual(batch[i]->dataset_name) == "");
        }
    }
});

this->add_method("import_batch_replace_always", [](Fixture& f) {
    auto ds = f.config().create_writer();

    std::vector<shared_ptr<dataset::WriterBatchElement>> batch;
    batch.emplace_back(make_shared<dataset::WriterBatchElement>(f.td.test_data[0].md));
    batch.emplace_back(make_shared<dataset::WriterBatchElement>(f.td.test_data[1].md));
    batch.emplace_back(make_shared<dataset::WriterBatchElement>(f.td.test_data[2].md));
    wassert(ds->acquire_batch(batch, dataset::Writer::REPLACE_ALWAYS));

    for (unsigned i = 0; i < 3; ++i)
    {
        wassert(actual(batch[i]->result) == dataset::ACQ_OK);
        wassert(actual(batch[i]->dataset_name) == "testds");
        wassert(actual_file(str::joinpath(f.ds_root, f.destfile(f.td.test_data[i]))).exists());
        wassert(actual_type(f.td.test_data[i].md.source()).is_source_blob(f.td.format, f.ds_root, f.destfile(f.td.test_data[i])));
    }

    Metadata mds[3] = { f.td.test_data[0].md, f.td.test_data[1].md, f.td.test_data[2].md };
    batch.clear();
    batch.emplace_back(make_shared<dataset::WriterBatchElement>(mds[0]));
    batch.emplace_back(make_shared<dataset::WriterBatchElement>(mds[1]));
    batch.emplace_back(make_shared<dataset::WriterBatchElement>(mds[2]));
    wassert(ds->acquire_batch(batch, dataset::Writer::REPLACE_ALWAYS));
    for (unsigned i = 0; i < 3; ++i)
    {
        wassert(actual(batch[i]->result) == dataset::ACQ_OK);
        wassert(actual(batch[i]->dataset_name) == "testds");
        wassert(actual(mds[i].sourceBlob().absolutePathname()) == f.td.test_data[i].md.sourceBlob().absolutePathname());
        wassert(actual(mds[i].sourceBlob().offset) > f.td.test_data[i].md.sourceBlob().offset);
        wassert(actual(mds[i].sourceBlob().size) == f.td.test_data[i].md.sourceBlob().size);
    }
});

this->add_method("import_batch_replace_usn", [](Fixture& f) {
    f.cfg.setValue("format", "bufr");
    metadata::TestCollection mdc("inbound/synop-gts.bufr");
    metadata::TestCollection mdc_upd("inbound/synop-gts-usn2.bufr");

    auto ds = f.config().create_writer();

    std::vector<shared_ptr<dataset::WriterBatchElement>> batch;
    batch.emplace_back(make_shared<dataset::WriterBatchElement>(mdc[0]));
    wassert(ds->acquire_batch(batch, dataset::Writer::REPLACE_HIGHER_USN));
    wassert(actual(batch[0]->result) == dataset::ACQ_OK);
    wassert(actual(batch[0]->dataset_name) == "testds");
    wassert(actual_file(str::joinpath(f.ds_root, "2009/12-04.bufr")).exists());
    wassert(actual_type(mdc[0].source()).is_source_blob("bufr", f.ds_root, "2009/12-04.bufr"));

    // Acquire again: it works, since USNs the same as the existing ones do overwrite
    wassert(ds->acquire_batch(batch, dataset::Writer::REPLACE_HIGHER_USN));
    wassert(actual(batch[0]->result) == dataset::ACQ_OK);
    wassert(actual(batch[0]->dataset_name) == "testds");

    // Acquire with a newer USN: it works
    batch.clear();
    batch.emplace_back(make_shared<dataset::WriterBatchElement>(mdc_upd[0]));
    wassert(ds->acquire_batch(batch, dataset::Writer::REPLACE_HIGHER_USN));
    wassert(actual(batch[0]->result) == dataset::ACQ_OK);
    wassert(actual(batch[0]->dataset_name) == "testds");

    // Acquire with the lower USN: it fails
    batch.clear();
    batch.emplace_back(make_shared<dataset::WriterBatchElement>(mdc[0]));
    wassert(ds->acquire_batch(batch, dataset::Writer::REPLACE_HIGHER_USN));
    if (ds->type() == "simple")
    {
        wassert(actual(batch[0]->result) == dataset::ACQ_OK);
        wassert(actual(batch[0]->dataset_name) == "testds");
    } else {
        wassert(actual(batch[0]->result) == dataset::ACQ_ERROR_DUPLICATE);
        wassert(actual(batch[0]->dataset_name) == "");
    }
});

this->add_method("import_before_archive_age", [](Fixture& f) {
    auto o = dataset::SessionTime::local_override(1483225200); // date +%s --date="2017-01-01"
    f.cfg.setValue("archive age", "1");
    auto ds = f.config().create_writer();

    for (unsigned i = 0; i < 3; ++i)
    {
        Metadata md = f.td.test_data[i].md;
        wassert(actual(ds->acquire(md)) == dataset::ACQ_ERROR);
        wassert(actual(md.notes().back().content).contains("is older than archive age"));
    }

    metadata::Collection mdc(*f.config().create_reader(), Matcher());
    wassert(actual(mdc.size()) == 0u);
});

this->add_method("import_before_delete_age", [](Fixture& f) {
    auto o = dataset::SessionTime::local_override(1483225200); // date +%s --date="2017-01-01"
    f.cfg.setValue("delete age", "1");
    auto ds = f.config().create_writer();

    for (unsigned i = 0; i < 3; ++i)
    {
        Metadata md = f.td.test_data[i].md;
        wassert(actual(*ds).import(md));
        wassert(actual(md.notes().back().content).contains("is older than delete age"));
    }

    metadata::Collection mdc(*f.config().create_reader(), Matcher());
    wassert(actual(mdc.size()) == 0u);
});

this->add_method("second_resolution", [](Fixture& f) {
    Metadata md(f.td.test_data[1].md);
    md.set(types::Reftime::createPosition(Time(2007, 7, 7, 0, 0, 0)));

    // Import a first metadata to create a segment to repack
    {
        auto writer = f.config().create_writer();
        wassert(actual(*writer).import(md));
    }

    md.set(types::Reftime::createPosition(Time(2007, 7, 7, 0, 0, 1)));
    {
        auto writer = f.config().create_writer();
        wassert(actual(*writer).import(md));
    }

    wassert(f.ensure_localds_clean(1, 2));
});

this->add_method("test_acquire", [](Fixture& f) {
});

}
}
