#include <arki/dataset/test-scenario.h>
#include <arki/exceptions.h>
#include <arki/dataset/ondisk2.h>
#include <arki/dataset/ondisk2/writer.h>
#include <arki/dataset/archive.h>
#include <arki/dataset/maintenance.h>
#include <arki/dataset/indexed.h>
#include <arki/dataset/reporter.h>
#include <arki/metadata/test-generator.h>
#include <arki/metadata/consumer.h>
#include <arki/scan/any.h>
#include <arki/utils.h>
#include <arki/utils/sys.h>
#include <arki/utils/string.h>
#include <sstream>

using namespace std;
using namespace arki;
using namespace arki::utils;

namespace arki {
namespace dataset {
namespace test {

Scenario::Scenario(const std::string& name)
    : name(name), built(false)
{
    path = sys::abspath(str::joinpath("scenarios", name));
    cfg.setValue("path", path);
    cfg.setValue("name", name);
}
Scenario::~Scenario() {}

void Scenario::build()
{
    built = true;
    sys::mkdir_ifmissing("scenarios", 0777);
    if (sys::exists(path))
        sys::rmtree(path);
    // TODO: create config file
}

ConfigFile Scenario::clone(const std::string& newpath) const
{
    if (path.find("'") != string::npos)
        throw std::runtime_error("cannot clone scenario " + name + " from " + path + ": the old path contains \"'\"");
    if (newpath.find("'") != string::npos)
        throw std::runtime_error("cannot clone scenario " + name + " from " + path + ": the new path contains \"'\"");

    if (sys::exists(newpath))
        sys::rmtree(newpath);

    // TODO: replace "cp -a" with something that doesn't require system
    string cmd = "cp -a '" + path + "' '" + newpath + "'";
    int sres = system(cmd.c_str());
    if (sres == -1)
        throw_system_error("Running command " + cmd);
    if (sres != 0)
    {
        stringstream ss;
        ss << "command " << cmd << " returned error code " << sres;
        throw std::runtime_error(ss.str());
    }

    ConfigFile res;
    res.merge(cfg);
    res.setValue("path", newpath);
    return res;
}

namespace {

metadata_dest_func make_importer(dataset::SegmentedWriter& ds)
{
    return [&](unique_ptr<Metadata> md) {
        Writer::AcquireResult r = ds.acquire(*md);
        if (r != Writer::ACQ_OK)
            throw std::runtime_error("cannot build test scenario: metadata was not imported successfully");
        return true;
    };
}

/// Unlity base class for ondisk2 scenario builders
struct Ondisk2Scenario : public Scenario
{
    static const time_t t_start = 1283299200;

    Ondisk2Scenario(const std::string& name)
        : Scenario(name)
    {
        cfg.setValue("type", "ondisk2");
    }

    /// Run a repack run as if the current day in the month is \a curday
    void run_repack(int curday, int expected_archived)
    {
        using namespace dataset;

        // Override current date for maintenance to 2010-09-01 + curday
        dataset::TestOverrideCurrentDateForMaintenance od(t_start + 3600*24*(curday-1));

        // Pack and archive
        auto config = ondisk2::Config::create(cfg);
        ondisk2::Checker ds(config);
        stringstream packlog;
        OstreamReporter reporter(packlog);
        ds.repack(reporter, true);
        char expected[32];
        snprintf(expected, 32, "%d files archived", expected_archived);
        if (packlog.str().find(expected) == string::npos)
        {
            stringstream ss;
            ss << "cannot check that only " << expected_archived << " files have been archived: archive output is: " << packlog.str();
            throw std::runtime_error(ss.str());
        }
    }

    /// Rename the 'last' archive to the given name
    void mvlast(const std::string& newname)
    {
        string path = cfg.value("path");
        string last = str::joinpath(path, ".archive/last");
        string older = str::joinpath(path, ".archive/" + newname);
        if (rename(last.c_str(), older.c_str()) < 0)
            throw_system_error("cannot rename " + last + " to " + older);
    }

    /* Boilerplate left here in case there is something to add at a later stage
    void build()
    {
        using namespace arki::dataset;

        Scenario::build();
    }
    */
};

struct Ondisk2TestGrib1 : public Ondisk2Scenario
{
    Ondisk2TestGrib1()
        : Ondisk2Scenario("ondisk2-testgrib1")
    {
        desc = "Ondisk2 dataset which contains all the contents of test.grib1 from the "
               " test data (scanned from ./inbound/test.grib1)."
               " Allowed postprocessor: testcountbytes.";
        cfg.setValue("step", "daily");
        cfg.setValue("unique", "reftime");
        cfg.setValue("postprocess", "testcountbytes");
    }
    void build()
    {
        using namespace arki::dataset;

        Ondisk2Scenario::build();
        auto config = ondisk2::Config::create(cfg);

        {
            ondisk2::Writer ds(config);
            scan::scan("inbound/test.grib1", make_importer(ds));
            ds.flush();
        }

        {
            // Generate a dataset with archived data
            ondisk2::Checker ds(config);

            // Run a check to remove new dataset marker
            stringstream checklog;
            OstreamReporter reporter(checklog);
            ds.check(reporter, true, true);
            if (checklog.str() != "ondisk2-testgrib1: check 3 files ok\n")
                throw std::runtime_error("check on new dataset failed: " + checklog.str());
        }
    }
};

struct Ondisk2Archived : public Ondisk2Scenario
{
    Ondisk2Archived()
        : Ondisk2Scenario("ondisk2-archived")
    {
        desc = "Ondisk2 dataset containing one grib1 per day for all the month 2010-09."
               " Data from day 1 to 7 is archived in .archive/older; data from day 8 to 15"
               " is archived in .archive/last. Data from 16 to 30 is live."
               " The only changing metadata item is the reference time";
        cfg.setValue("step", "daily");
        cfg.setValue("unique", "reftime");
        cfg.setValue("archive age", "7");
    }
    void build()
    {
        using namespace arki::dataset;

        Ondisk2Scenario::build();
        auto config = ondisk2::Config::create(cfg);

        {
            // Generate a dataset with archived data
            ondisk2::Writer ds(config);

            // Import several metadata items
            metadata::test::Generator gen("grib1");
            for (int i = 1; i <= 30; ++i)
            {
                char buf[32];
                snprintf(buf, 32, "2010-09-%02dT00:00:00Z", i);
                gen.add(TYPE_REFTIME, buf);
            }
            gen.generate(make_importer(ds));
            ds.flush();
        }

        // Run a check to remove new dataset marker
        ondisk2::Checker ds(config);
        stringstream checklog;
        OstreamReporter reporter(checklog);
        ds.check(reporter, true, true);
        if (checklog.str() != "ondisk2-archived: check 0 files ok\n")
            throw std::runtime_error("cannot run check on correct dataset: log is not empty: " + checklog.str());

        // Pack to build 'older' archive
        run_repack(16, 8);
        mvlast("older");

        // Pack to build 'last' archive
        run_repack(31, 15);
    }
};

struct Ondisk2ManyArchiveStates : public Ondisk2Scenario
{
    Ondisk2ManyArchiveStates()
        : Ondisk2Scenario("ondisk2-manyarchivestates")
    {
        desc = "Ondisk2 dataset containing many archives in many different states:\n"
               " - last: as usual\n"
               " - old: normal archive dir\n"
               " - ro: normal archive dir archived to mounted readonly media\n"
               "   with dir.summary at the top\n"
               " - wrongro: same as ro, but toplevel summary does not match the data\n"
               " - offline: same as ro, but only the toplevel summary is present\n"
               "\n"
               "The dataset contains 18 days of daily data. There are 3 days"
               " live and each archive spans 3 days."
               " The only changing metadata item is the reference time";
        cfg.setValue("step", "daily");
        cfg.setValue("unique", "reftime");
        cfg.setValue("archive age", "3");
    }

    void build()
    {
        using namespace arki::dataset;

        Ondisk2Scenario::build();
        auto config = ondisk2::Config::create(cfg);

        // Generate a dataset with archived data
        {
            ondisk2::Writer ds(config);

            // Import several metadata items
            metadata::test::Generator gen("grib1");
            for (int i = 1; i <= 18; ++i)
            {
                char buf[32];
                snprintf(buf, 32, "2010-09-%02dT00:00:00Z", i);
                gen.add(TYPE_REFTIME, buf);
            }
            gen.generate(make_importer(ds));
            ds.flush();
        }

        // Run a check to remove new dataset marker
        {
            ondisk2::Checker ds(config);
            stringstream checklog;
            OstreamReporter reporter(checklog);
            ds.check(reporter, true, true);
            if (checklog.str() != "ondisk2-manyarchivestates: check 0 files ok\n")
                throw std::runtime_error("cannot run check on correct dataset: log is not empty: " + checklog.str());
        }

        // Pack and build 'offline' archive
        run_repack(7, 3);
        mvlast("offline");
        // same as ro, but only the toplevel summary is present
        string ofsum = sys::read_file(str::joinpath(path, ".archive/offline/summary"));
        sys::write_file(
                str::joinpath(path, ".archive/offline.summary"),
                ofsum);
        sys::rmtree(str::joinpath(path, ".archive/offline"));

        // Pack and build 'wrongro' archive
        run_repack(10, 3);
        mvlast("wrongro");
        // same as ro, but toplevel summary does not match the data
        sys::write_file(
                str::joinpath(path, ".archive/wrongro.summary"),
                ofsum);

        // Pack and build 'ro' archive
        run_repack(13, 3);
        mvlast("ro");
        // normal archive dir archived to mounted readonly media with dir.summary at the top
        sys::write_file(
                str::joinpath(path, ".archive/ro.summary"),
                sys::read_file(str::joinpath(path, ".archive/ro/summary")));

        // Pack and build 'old' archive
        run_repack(16, 3);
        mvlast("old");

        // Pack and build 'last' archive
        run_repack(19, 3);
    }
};

struct ScenarioDB : public map<string, Scenario*>
{
    ScenarioDB()
    {
        // Build scenario archive
        add(new Ondisk2TestGrib1);
        add(new Ondisk2Archived);
        add(new Ondisk2ManyArchiveStates);
    }
    void add(Scenario* s)
    {
        insert(make_pair(s->name, s));
    }
};
}

static ScenarioDB *scenarios = 0;

const Scenario& Scenario::get(const std::string& name)
{
    if (!scenarios) scenarios = new ScenarioDB;
    // Get the scenario
    ScenarioDB::iterator i = scenarios->find(name);
    if (i == scenarios->end())
        throw std::runtime_error("cannot create test scenario \"" + name + "\": scenario does not exist");
    // Build it if needed
    if (!i->second->built)
        i->second->build();
    return *i->second;
}

std::vector<std::string> Scenario::list()
{
    if (!scenarios) scenarios = new ScenarioDB;
    vector<string> res;
    for (ScenarioDB::const_iterator i = scenarios->begin();
            i != scenarios->end(); ++i)
        res.push_back(i->first);
    return res;
}

}
}
}
