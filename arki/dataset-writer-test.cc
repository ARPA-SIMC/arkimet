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
#include "wibble/sys/childprocess.h"
#include <sys/fcntl.h>
#include <iostream>

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

template<class Fixture>
struct ConcurrentImporter : public wibble::sys::ChildProcess
{
    Fixture& fixture;
    unsigned initial;
    unsigned increment;

    ConcurrentImporter(Fixture& fixture, unsigned initial, unsigned increment)
        : fixture(fixture), initial(initial), increment(increment)
    {
    }

    virtual int main() override
    {
        auto ds(fixture.config().create_writer());

        Metadata md = fixture.td.test_data[0].md;

        for (unsigned i = initial; i < 60; i += increment)
        {
            md.set(types::Reftime::createPosition(core::Time(2016, 6, 1, 0, 0, i)));
            wassert(actual(ds->acquire(md)) == dataset::Writer::ACQ_OK);
        }

        return 0;
    }
};

struct ReadHang : public wibble::sys::ChildProcess
{
    std::shared_ptr<const dataset::Config> config;
    int commfd;

    ReadHang(std::shared_ptr<const dataset::Config> config) : config(config) {}

    bool eat(unique_ptr<Metadata>&& md)
    {
        // Notify start of reading
        cout << "H" << endl;
        // Get stuck while reading
        while (true)
            usleep(100000);
        return true;
    }

    int main() override
    {
        try {
            auto reader = config->create_reader();
            reader->query_data(Matcher(), [&](unique_ptr<Metadata> md) { return eat(move(md)); });
        } catch (std::exception& e) {
            cerr << e.what() << endl;
            cout << "E" << endl;
            return 1;
        }
        return 0;
    }

    void start()
    {
        forkAndRedirect(0, &commfd);
    }

    char waitUntilHung()
    {
        char buf[2];
        if (read(commfd, buf, 1) != 1)
            throw_system_error("reading 1 byte from child process");
        return buf[0];
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

template<class Data>
class TestsWriter : public FixtureTestCase<FixtureWriter<Data>>
{
    using FixtureTestCase<FixtureWriter<Data>>::FixtureTestCase;

    void register_tests() override;
};

TestsWriter<testdata::GRIBData> test_writer_grib_ondisk2("arki_dataset_writer_grib_ondisk2", "type=ondisk2\n");
TestsWriter<testdata::GRIBData> test_writer_grib_ondisk2_sharded("arki_dataset_writer_grib_ondisk2_sharded", "type=ondisk2\nshard=yearly\n");
TestsWriter<testdata::GRIBData> test_writer_grib_simple_plain("arki_dataset_writer_grib_simple_plain", "type=simple\nindex_type=plain\n");
TestsWriter<testdata::GRIBData> test_writer_grib_simple_plain_sharded("arki_dataset_writer_grib_simple_plain_sharded", "type=simple\nindex_type=plain\nshard=yearly\n");
TestsWriter<testdata::GRIBData> test_writer_grib_simple_sqlite("arki_dataset_writer_grib_simple_sqlite", "type=simple\nindex_type=sqlite");
TestsWriter<testdata::BUFRData> test_writer_bufr_ondisk2("arki_dataset_writer_bufr_ondisk2", "type=ondisk2\n");
TestsWriter<testdata::BUFRData> test_writer_bufr_ondisk2_sharded("arki_dataset_writer_bufr_ondisk2_sharded", "type=ondisk2\nshard=yearly\n");
TestsWriter<testdata::BUFRData> test_writer_bufr_simple_plain("arki_dataset_writer_bufr_simple_plain", "type=simple\nindex_type=plain\n");
TestsWriter<testdata::BUFRData> test_writer_bufr_simple_plain_sharded("arki_dataset_writer_bufr_simple_plain_sharded", "type=simple\nindex_type=plain\nshard=yearly\n");
TestsWriter<testdata::BUFRData> test_writer_bufr_simple_sqlite("arki_dataset_writer_bufr_simple_sqlite", "type=simple\nindex_type=sqlite");
TestsWriter<testdata::VM2Data> test_writer_vm2_ondisk2("arki_dataset_writer_vm2_ondisk2", "type=ondisk2\n");
TestsWriter<testdata::VM2Data> test_writer_vm2_ondisk2_sharded("arki_dataset_writer_vm2_ondisk2_sharded", "type=ondisk2\nshard=yearly\n");
TestsWriter<testdata::VM2Data> test_writer_vm2_simple_plain("arki_dataset_writer_vm2_simple_plain", "type=simple\nindex_type=plain\n");
TestsWriter<testdata::VM2Data> test_writer_vm2_simple_plain_sharded("arki_dataset_writer_vm2_simple_plain_sharded", "type=simple\nindex_type=plain\nshard=yearly\n");
TestsWriter<testdata::VM2Data> test_writer_vm2_simple_sqlite("arki_dataset_writer_vm2_simple_sqlite", "type=simple\nindex_type=sqlite");
TestsWriter<testdata::ODIMData> test_writer_odim_ondisk2("arki_dataset_writer_odim_ondisk2", "type=ondisk2\n");
TestsWriter<testdata::ODIMData> test_writer_odim_ondisk2_sharded("arki_dataset_writer_odim_ondisk2_sharded", "type=ondisk2\nshard=yearly\n");
TestsWriter<testdata::ODIMData> test_writer_odim_simple_plain("arki_dataset_writer_odim_simple_plain", "type=simple\nindex_type=plain\n");
TestsWriter<testdata::ODIMData> test_writer_odim_simple_plain_sharded("arki_dataset_writer_odim_simple_plain_sharded", "type=simple\nindex_type=plain\nshard=yearly\n");
TestsWriter<testdata::ODIMData> test_writer_odim_simple_sqlite("arki_dataset_writer_odim_simple_sqlite", "type=simple\nindex_type=sqlite");

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
                dataset::Writer::AcquireResult res = writer->acquire(md);
                wassert(actual(res) == dataset::Writer::ACQ_OK);
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
    reader->query_bytes(bq, out);
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
        wassert(actual(ds->acquire(md)) == dataset::Writer::ACQ_OK);
        wassert(actual_file(str::joinpath(f.ds_root, f.destfile(f.td.test_data[i]))).exists());
        wassert(actual_type(md.source()).is_source_blob(f.td.format, f.ds_root, f.destfile(f.td.test_data[i])));
    }
});

this->add_method("import_before_archive_age", [](Fixture& f) {
    auto o = dataset::SessionTime::local_override(1483225200); // date +%s --date="2017-01-01"
    f.cfg.setValue("archive age", "1");
    auto ds = f.config().create_writer();

    for (unsigned i = 0; i < 3; ++i)
    {
        Metadata md = f.td.test_data[i].md;
        wassert(actual(ds->acquire(md)) == dataset::Writer::ACQ_ERROR);
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
        wassert(actual(ds->acquire(md)) == dataset::Writer::ACQ_OK);
        wassert(actual(md.notes().back().content).contains("is older than delete age"));
    }

    metadata::Collection mdc(*f.config().create_reader(), Matcher());
    wassert(actual(mdc.size()) == 0u);
});

this->add_method("concurrent_import", [](Fixture& f) {
    ConcurrentImporter<Fixture> i0(f, 0, 3);
    ConcurrentImporter<Fixture> i1(f, 1, 3);
    ConcurrentImporter<Fixture> i2(f, 2, 3);

    i0.fork();
    i1.fork();
    i2.fork();

    i0.wait();
    i1.wait();
    i2.wait();

    auto reader = f.config().create_reader();
    metadata::Collection mdc(*reader, Matcher());
    wassert(actual(mdc.size()) == 60u);

    for (int i = 0; i < 60; ++i)
    {
        auto rt = mdc[i].get<types::reftime::Position>();
        wassert(actual(rt->time.se) == i);
    }
});

// Test acquiring with a reader who's stuck on output
this->add_method("import_with_hung_reader", [](Fixture& f) {

    metadata::Collection mdc("inbound/test-sorted.grib1");

    // Import one grib in the dataset
    {
        auto ds = f.config().create_writer();
        wassert(actual(ds->acquire(mdc[0])) == dataset::Writer::ACQ_OK);
        ds->flush();
    }

    // Query the index and hang
    ReadHang readHang(f.dataset_config());
    readHang.start();
    wassert(actual(readHang.waitUntilHung()) == 'H');

    // Import another grib in the dataset
    {
        auto ds = f.config().create_writer();
        wassert(actual(ds->acquire(mdc[1])) == dataset::Writer::ACQ_OK);
        ds->flush();
    }

    readHang.kill(9);
    readHang.wait();

    metadata::Collection mdc1(*f.config().create_reader(), Matcher());
    wassert(actual(mdc1.size()) == 2u);
});

}
}
