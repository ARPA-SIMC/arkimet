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
#include "arki/core/file.h"
#include "arki/utils/accounting.h"
#include "arki/utils/string.h"
#include "arki/utils/sys.h"
#include "arki/exceptions.h"
#include "wibble/sys/childprocess.h"
#include <sys/fcntl.h>
#include <iostream>

using namespace std;
using namespace arki;
using namespace arki::core;
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


class TestSubprocess : public wibble::sys::ChildProcess
{
protected:
    int commfd;

public:
    void start()
    {
        forkAndRedirect(0, &commfd);
    }

    void notify_ready()
    {
        putchar('H');
        fflush(stdout);
    }

    char wait_until_ready()
    {
        char buf[2];
        ssize_t res = read(commfd, buf, 1);
        if (res < 0)
            throw_system_error("reading 1 byte from child process");
        if (res == 0)
            throw runtime_error("child process closed stdout without producing any output");
        return buf[0];
    }

    void during(char expected, std::function<void()> f)
    {
        start();
        wassert(actual(wait_until_ready()) == expected);
        try {
            f();
        } catch(...) {
            kill(9);
            wait();
            throw;
        }
        kill(9);
        wait();
    }

    void during(std::function<void()> f)
    {
        start();
        wait_until_ready();
        try {
            f();
        } catch(...) {
            kill(9);
            wait();
            throw;
        }
        kill(9);
        wait();
    }
};


template<class Fixture>
struct ConcurrentImporter : public wibble::sys::ChildProcess
{
    Fixture& fixture;
    unsigned initial;
    unsigned increment;
    bool increment_year;

    ConcurrentImporter(Fixture& fixture, unsigned initial, unsigned increment, bool increment_year=false)
        : fixture(fixture), initial(initial), increment(increment), increment_year(increment_year)
    {
    }

    int main() override
    {
        core::lock::test_set_nowait_default(false);
        try {
            auto ds(fixture.config().create_writer());

            Metadata md = fixture.td.test_data[0].md;

            for (unsigned i = initial; i < 60; i += increment)
            {
                if (increment_year)
                    md.set(types::Reftime::createPosition(core::Time(2000 + i, 6, 1, 0, 0, 0)));
                else
                    md.set(types::Reftime::createPosition(core::Time(2000, 6, 1, 0, 0, i)));
                //fprintf(stderr, "%d: %d\n", (int)getpid(), i);
                auto res = ds->acquire(md);
                if (res != dataset::Writer::ACQ_OK)
                {
                    fprintf(stderr, "Acquire result: %d\n", (int)res);
                    for (const auto& note: md.notes())
                        fprintf(stderr, "  note: %s\n", note.content.c_str());
                    return 2;
                }
            }

            return 0;
        } catch (std::exception& e) {
            fprintf(stderr, "importer:%u%u: %s\n", initial, increment, e.what());
            return 1;
        }
    }
};

struct ReadHang : public TestSubprocess
{
    std::shared_ptr<const dataset::Config> config;

    ReadHang(std::shared_ptr<const dataset::Config> config) : config(config) {}

    bool eat(unique_ptr<Metadata>&& md)
    {
        notify_ready();
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
};

struct HungReporter : public dataset::NullReporter
{
    TestSubprocess& sp;

    HungReporter(TestSubprocess& sp) : sp(sp) {}

    void segment_info(const std::string& ds, const std::string& relpath, const std::string& message) override
    {
        sp.notify_ready();
        while (true)
            sleep(3600);
    }
};

template<class Fixture>
struct CheckForever : public TestSubprocess
{
    Fixture& fixture;

    CheckForever(Fixture& fixture) : fixture(fixture) {}

    int main() override
    {
        auto ds(fixture.config().create_checker());
        HungReporter reporter(*this);
        ds->check(reporter, false, false);
        return 0;
    }
};

template<class Fixture>
struct RepackForever : public TestSubprocess
{
    Fixture& fixture;

    RepackForever(Fixture& fixture) : fixture(fixture) {}

    int main() override
    {
        string segment = "2007/07-07." + fixture.td.format;
        notify_ready();

        while (true)
        {
            auto ds(fixture.makeSegmentedChecker());
            auto seg = ds->segment(segment);
            seg->repack();
        }
        return 0;
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

this->add_method("read_read", [](Fixture& f) {
    f.import_all(f.td);

    // Query the index and hang
    metadata::Collection mdc;
    ReadHang readHang(f.dataset_config());
    readHang.during('H', [&]{
        // Query in parallel with the other read
        mdc.add(*f.config().create_reader(), Matcher());
    });
    wassert(actual(mdc.size()) == 3u);
});

// Test acquiring with a reader who's stuck on output
this->add_method("read_write", [](Fixture& f) {
    f.clean();

    // Import one grib in the dataset
    {
        auto ds = f.config().create_writer();
        wassert(actual(ds->acquire(f.td.test_data[0].md)) == dataset::Writer::ACQ_OK);
        ds->flush();
    }

    // Query the index and hang
    ReadHang readHang(f.dataset_config());
    readHang.during('H', [&]{
        // Import another grib in the dataset
        {
            auto ds = f.config().create_writer();
            wassert(actual(ds->acquire(f.td.test_data[1].md)) == dataset::Writer::ACQ_OK);
            ds->flush();
        }
    });

    metadata::Collection mdc1(*f.config().create_reader(), Matcher());
    wassert(actual(mdc1.size()) == 2u);
});

this->add_method("read_write1", [](Fixture& f) {
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
    wassert(actual(count) == 3u);
});

this->add_method("write_write_same_segment", [](Fixture& f) {
    ConcurrentImporter<Fixture> i0(f, 0, 3);
    ConcurrentImporter<Fixture> i1(f, 1, 3);
    ConcurrentImporter<Fixture> i2(f, 2, 3);

    i0.fork();
    i1.fork();
    i2.fork();

    wassert(actual(i0.wait()) == 0);
    wassert(actual(i1.wait()) == 0);
    wassert(actual(i2.wait()) == 0);

    auto reader = f.config().create_reader();
    metadata::Collection mdc(*reader, Matcher());
    wassert(actual(mdc.size()) == 60u);

    for (int i = 0; i < 60; ++i)
    {
        auto rt = mdc[i].get<types::reftime::Position>();
        wassert(actual(rt->time.se) == i);
    }
});

this->add_method("write_write_different_segments", [](Fixture& f) {
    ConcurrentImporter<Fixture> i0(f, 0, 3, true);
    ConcurrentImporter<Fixture> i1(f, 1, 3, true);
    ConcurrentImporter<Fixture> i2(f, 2, 3, true);

    i0.fork();
    i1.fork();
    i2.fork();

    wassert(actual(i0.wait()) == 0);
    wassert(actual(i1.wait()) == 0);
    wassert(actual(i2.wait()) == 0);

    auto reader = f.config().create_reader();
    metadata::Collection mdc(*reader, Matcher());
    wassert(actual(mdc.size()) == 60u);

    for (int i = 0; i < 60; ++i)
    {
        auto rt = mdc[i].get<types::reftime::Position>();
        wassert(actual(rt->time.ye) == 2000 + i);
    }
});

this->add_method("read_repack", [](Fixture& f) {
    auto orig_data = f.td.earliest_element().md.getData();

    f.reset_test("step=single");
    f.import_all(f.td);

    auto reader = f.dataset_config()->create_reader();
    reader->query_data(dataset::DataQuery("", true), [&](unique_ptr<Metadata> md) {
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

// Test parallel check and write
this->add_method("write_check", [](Fixture& f) {
    core::lock::TestNowait lock_nowait;

    bool is_iseg;

    {
        auto writer = f.config().create_writer();
        is_iseg = writer->type() == "iseg";
        wassert(actual(writer->acquire(f.td.test_data[0].md)) == dataset::Writer::ACQ_OK);
        writer->flush();

        // Create an error to trigger a call to the reporter that then hangs
        // the checker
        f.makeSegmentedChecker()->test_corrupt_data(f.td.test_data[0].md.sourceBlob().filename, 0);
    }

    CheckForever<Fixture> cf(f);
    cf.during([&]{
        if (!is_iseg)
        {
            try {
                f.config().create_writer();
                wassert(actual(0) == 1);
            } catch (std::runtime_error& e) {
                wassert(actual(e.what()).contains("a read lock is already held"));
            }
        } else {
            auto writer = wcallchecked(f.makeSegmentedWriter());
            wassert(actual(writer->acquire(f.td.test_data[2].md)) == dataset::Writer::ACQ_OK);

            // Importing on the segment being checked should hang except on dir segments
            if (f.td.format == "odimh5")
                writer->acquire(f.td.test_data[0].md);
            else
                try {
                    writer->acquire(f.td.test_data[0].md);
                    wassert(actual(0) == 1);
                } catch (std::runtime_error& e) {
                    wassert(actual(e.what()).contains("a read lock is already held"));
                }
        }
    });
});

this->add_method("read_repack2", [](Fixture& f) {
    f.import_all(f.td);

    core::lock::TestWait lock_wait;

    metadata::Collection mdc;
    RepackForever<Fixture> rf(f);
    rf.during([&]{
        for (unsigned i = 0; i < 120; ++i)
            mdc.add(*f.config().create_reader(), Matcher());
    });

    wassert(actual(mdc.size()) == 120u * 3);
});

// Test parallel repack and write
this->add_method("write_repack", [](Fixture& f) {
    core::lock::TestWait lock_wait;

    Metadata md(f.td.test_data[1].md);
    md.set(types::Reftime::createPosition(Time(2007, 7, 7, 0, 0, 0)));

    // Import a first metadata to create a segment to repack
    {
        auto writer = f.config().create_writer();
        wassert(actual(*writer).import(md));
    }

    RepackForever<Fixture> rf(f);
    rf.during([&]{
        for (unsigned minute = 0; minute < 60; ++minute)
        {
            for (unsigned second = 0; second < 2; ++second)
            {
                md.set(types::Reftime::createPosition(Time(2007, 7, 7, 1, minute, second)));
                auto writer = f.config().create_writer();
                wassert(actual(*writer).import(md));
            }
        }
    });

    auto reader = f.config().create_reader();
    unsigned count = 0;
    reader->query_data(Matcher(), [&](unique_ptr<Metadata> md) { ++count; return true; });
    wassert(actual(count) == 121u);
});

this->add_method("nolock_read", [](Fixture& f) {
    f.reset_test("locking=no");
    f.import_all(f.td);
    core::lock::TestCount count;
    wassert(f.query_results(dataset::DataQuery("", true), {1, 0, 2}));
    count.measure();
    wassert(actual(count.ofd_setlk) == 0u);
    wassert(actual(count.ofd_setlkw) == 0u);
    wassert(actual(count.ofd_getlk) == 0u);
});

this->add_method("nolock_write", [](Fixture& f) {
    f.reset_test("locking=no");
    core::lock::TestCount count;
    f.import_all(f.td);
    count.measure();
    wassert(actual(count.ofd_setlk) == 0u);
    wassert(actual(count.ofd_setlkw) == 0u);
    wassert(actual(count.ofd_getlk) == 0u);
});

this->add_method("nolock_rescan", [](Fixture& f) {
    f.reset_test("locking=no");
    f.import_all(f.td);
    string test_segment = "2007/07-08." + f.td.format;
    f.make_unaligned(test_segment);

    core::lock::TestCount count;
    {
        auto writer(f.makeSegmentedChecker());
        ReporterExpected e;
        e.report.emplace_back("testds", "check", "2 files ok");
        e.rescanned.emplace_back("testds", test_segment);
        wassert(actual(writer.get()).check(e, true));
    }
    count.measure();
    wassert(actual(count.ofd_setlk) == 0u);
    wassert(actual(count.ofd_setlkw) == 0u);
    wassert(actual(count.ofd_getlk) == 0u);
});

this->add_method("nolock_repack", [](Fixture& f) {
    f.reset_test("locking=no");
    f.import_all(f.td);
    string test_segment = "2007/07-08." + f.td.format;
    f.makeSegmentedChecker()->test_make_hole(test_segment, 10, 1);

    core::lock::TestCount count;
    {
        auto checker(f.makeSegmentedChecker());
        ReporterExpected e;
        e.report.emplace_back("testds", "repack", "2 files ok");
        e.repacked.emplace_back("testds", test_segment);
        wassert(actual(checker.get()).repack(e, true));
    }
    count.measure();
    wassert(actual(count.ofd_setlk) == 0u);
    wassert(actual(count.ofd_setlkw) == 0u);
    wassert(actual(count.ofd_getlk) == 0u);
});

}
}
