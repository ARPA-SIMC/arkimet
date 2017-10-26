#include "arki/dataset/tests.h"
#include "arki/dataset.h"
#include "arki/dataset/time.h"
#include "arki/dataset/reporter.h"
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
    Data td;

    FixtureWriter(const std::string& cfg_instance=std::string())
        : DatasetTest(R"(
            step = daily
            unique = product, area, reftime
        )" + cfg_instance) {}

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

template<class Data>
class Tests : public FixtureTestCase<FixtureWriter<Data>>
{
    using FixtureTestCase<FixtureWriter<Data>>::FixtureTestCase;

    void register_tests() override;
};

Tests<testdata::GRIBData> test_concurrent_grib_ondisk2("arki_dataset_concurrent_grib_ondisk2", "type=ondisk2\n");
Tests<testdata::GRIBData> test_concurrent_grib_simple_plain("arki_dataset_concurrent_grib_simple_plain", "type=simple\nindex_type=plain\n");
Tests<testdata::GRIBData> test_concurrent_grib_simple_sqlite("arki_dataset_concurrent_grib_simple_sqlite", "type=simple\nindex_type=sqlite");
Tests<testdata::GRIBData> test_concurrent_grib_iseg("arki_dataset_concurrent_grib_iseg", "type=iseg\nformat=grib\n");
Tests<testdata::BUFRData> test_concurrent_bufr_ondisk2("arki_dataset_concurrent_bufr_ondisk2", "type=ondisk2\n");
Tests<testdata::BUFRData> test_concurrent_bufr_simple_plain("arki_dataset_concurrent_bufr_simple_plain", "type=simple\nindex_type=plain\n");
Tests<testdata::BUFRData> test_concurrent_bufr_simple_sqlite("arki_dataset_concurrent_bufr_simple_sqlite", "type=simple\nindex_type=sqlite");
Tests<testdata::BUFRData> test_concurrent_bufr_iseg("arki_dataset_concurrent_bufr_iseg", "type=iseg\nformat=bufr\n");
Tests<testdata::VM2Data> test_concurrent_vm2_ondisk2("arki_dataset_concurrent_vm2_ondisk2", "type=ondisk2\n");
Tests<testdata::VM2Data> test_concurrent_vm2_simple_plain("arki_dataset_concurrent_vm2_simple_plain", "type=simple\nindex_type=plain\n");
Tests<testdata::VM2Data> test_concurrent_vm2_simple_sqlite("arki_dataset_concurrent_vm2_simple_sqlite", "type=simple\nindex_type=sqlite");
Tests<testdata::VM2Data> test_concurrent_vm2_iseg("arki_dataset_concurrent_vm2_iseg", "type=iseg\nformat=vm2\n");
Tests<testdata::ODIMData> test_concurrent_odim_ondisk2("arki_dataset_concurrent_odim_ondisk2", "type=ondisk2\n");
Tests<testdata::ODIMData> test_concurrent_odim_simple_plain("arki_dataset_concurrent_odim_simple_plain", "type=simple\nindex_type=plain\n");
Tests<testdata::ODIMData> test_concurrent_odim_simple_sqlite("arki_dataset_concurrent_odim_simple_sqlite", "type=simple\nindex_type=sqlite");
Tests<testdata::ODIMData> test_concurrent_odim_iseg("arki_dataset_concurrent_odim_iseg", "type=iseg\nformat=odimh5\n");

template<class Data>
void Tests<Data>::register_tests() {

typedef FixtureWriter<Data> Fixture;

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
    f.clean();

    // Import one grib in the dataset
    {
        auto ds = f.config().create_writer();
        wassert(actual(ds->acquire(f.td.test_data[0].md)) == dataset::Writer::ACQ_OK);
        ds->flush();
    }

    // Query the index and hang
    ReadHang readHang(f.dataset_config());
    readHang.start();
    wassert(actual(readHang.waitUntilHung()) == 'H');

    // Import another grib in the dataset
    {
        auto ds = f.config().create_writer();
        wassert(actual(ds->acquire(f.td.test_data[1].md)) == dataset::Writer::ACQ_OK);
        ds->flush();
    }

    readHang.kill(9);
    readHang.wait();

    metadata::Collection mdc1(*f.config().create_reader(), Matcher());
    wassert(actual(mdc1.size()) == 2u);
});

this->add_method("repack_during_read", [](Fixture& f) {
    auto orig_data = f.td.earliest_element().md.getData();

    f.reset_test("step=single");
    f.import_all(f.td);

    auto reader = f.dataset_config()->create_reader();
    reader->query_data(Matcher(), [&](unique_ptr<Metadata> md) {
        {
            auto checker = f.dataset_config()->create_checker();
            dataset::NullReporter rep;
            try {
                checker->repack(rep, true, dataset::TEST_MISCHIEF_MOVE_DATA);
            } catch (std::exception& e) {
                wassert(actual(e.what()).contains("a read lock is already held"));
            }
        }

        auto data = md->getData();
        wassert(actual(data == orig_data).istrue());
        return false;
    });
});

this->add_method("import_during_read", [](Fixture& f) {
    f.reset_test("step=single");

    // Import one
    {
        auto writer = f.dataset_config()->create_writer();
        wassert(actual(writer->acquire(f.td.test_data[0].md)) == dataset::Writer::ACQ_OK);
    }

    // Query it and import during query
    auto reader = f.dataset_config()->create_reader();
    unsigned count = 0;
    reader->query_data(Matcher(), [&](unique_ptr<Metadata> md) {
        {
            // Make sure we only get one query result, that is, we don't read
            // the thing we import during the query
            wassert(actual(count) == 0u);

            auto writer = f.dataset_config()->create_writer();
            wassert(actual(writer->acquire(f.td.test_data[1].md)) == dataset::Writer::ACQ_OK);
            wassert(actual(writer->acquire(f.td.test_data[2].md)) == dataset::Writer::ACQ_OK);
        }
        ++count;
        return true;
    });
    wassert(actual(count) == 1u);

    // Querying again returns all imported data
    count = 0;
    reader->query_data(Matcher(), [&](unique_ptr<Metadata> md) { ++count; return true; });
    wassert(actual(count) == 1u);
});

}
}

