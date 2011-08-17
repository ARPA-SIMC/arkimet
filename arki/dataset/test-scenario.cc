/**
 * dataset/test-scenario - Build dataset scenarios for testing arkimet
 *
 * Copyright (C) 2010--2011  ARPA-SIM <urpsim@smr.arpa.emr.it>
 *
 * This program is free software; you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation; either version 2 of the License, or
 * (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License along
 * with this program; if not, write to the Free Software Foundation, Inc.,
 * 51 Franklin Street, Fifth Floor, Boston, MA 02110-1301 USA.
 *
 * Author: Enrico Zini <enrico@enricozini.com>
 */

#include "config.h"

#include <arki/dataset/test-scenario.h>
#include <arki/dataset/ondisk2.h>
#include <arki/dataset/archive.h>
#include <arki/metadata/test-generator.h>
#include <arki/metadata/consumer.h>
#include <arki/scan/any.h>
#include <arki/utils.h>
#include <wibble/exception.h>
#include <wibble/sys/fs.h>
#include <sstream>

using namespace std;
using namespace arki;
using namespace wibble;

namespace arki {
namespace dataset {
namespace test {

Scenario::Scenario(const std::string& name)
    : name(name), built(false)
{
    path = sys::fs::abspath(str::joinpath("scenarios", name));
    cfg.setValue("path", path);
    cfg.setValue("name", name);
}
Scenario::~Scenario() {}

void Scenario::build()
{
    built = true;
    sys::fs::mkdirIfMissing("scenarios", 0777);
    if (sys::fs::exists(path))
        sys::fs::rmtree(path);
    // TODO: create config file
}

ConfigFile Scenario::clone(const std::string& newpath) const
{
    if (path.find("'") != string::npos)
        throw wibble::exception::Consistency(
                "cloning scenario " + name + " from " + path,
                "cannot currently clone scenario if the old path contains \"'\"");
    if (newpath.find("'") != string::npos)
        throw wibble::exception::Consistency(
                "cloning scenario " + name + " to " + newpath,
                "cannot currently clone scenario if the new path contains \"'\"");

    if (sys::fs::exists(newpath))
        sys::fs::rmtree(newpath);

    // TODO: replace "cp -a" with something that doesn't require system
    string cmd = str::fmtf("cp -a '%s' '%s'", path.c_str(), newpath.c_str());
    int sres = system(cmd.c_str());
    if (sres == -1)
        throw wibble::exception::System("Running command " + cmd);
    if (sres != 0)
        throw wibble::exception::Consistency("Running command " + cmd, str::fmtf("Command returned %d", sres));

    ConfigFile res;
    res.merge(cfg);
    res.setValue("path", newpath);
    return res;
}

namespace {

struct Importer : public metadata::Consumer
{
    dataset::WritableLocal& ds;

    Importer(dataset::WritableLocal& ds) : ds(ds) {}
    bool operator()(Metadata& md)
    {
        WritableDataset::AcquireResult r = ds.acquire(md);
        if (r != WritableDataset::ACQ_OK)
            throw wibble::exception::Consistency("building test scenario", "metadata was not imported successfully");
        return true;
    }
};

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
        dataset::ondisk2::TestOverrideCurrentDateForMaintenance od(t_start + 3600*24*(curday-1));

        // Pack and archive
        auto_ptr<WritableLocal> ds(WritableLocal::create(cfg));
        stringstream packlog;
        ds->repack(packlog, true);
        if (packlog.str().find(str::fmtf("%d files archived", expected_archived)) == string::npos)
            throw wibble::exception::Consistency(
                    str::fmtf("checking that only %d files have been archived", expected_archived),
                    "archive output is: " + packlog.str());
        ds->flush();
    }

    /// Rename the 'last' archive to the given name
    void mvlast(const std::string& newname)
    {
        string path = cfg.value("path");
        string last = str::joinpath(path, ".archive/last");
        string older = str::joinpath(path, ".archive/" + newname);
        if (rename(last.c_str(), older.c_str()) < 0)
            throw wibble::exception::System("cannot rename " + last + " to " + older);
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

        // Generate a dataset with archived data
        auto_ptr<WritableLocal> ds(WritableLocal::create(cfg));

        Importer importer(*ds);
        scan::scan("inbound/test.grib1", importer);
        ds->flush();

        // Run a check to remove new dataset marker
        stringstream checklog;
        ds->check(checklog, true, true);
        if (checklog.str() != "")
            throw wibble::exception::Consistency("running check on correct dataset", "log is not empty: " + checklog.str());
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

        // Generate a dataset with archived data
        auto_ptr<WritableLocal> ds(WritableLocal::create(cfg));

        // Import several metadata items
        metadata::test::Generator gen("grib1");
        for (int i = 1; i <= 30; ++i)
            gen.add(types::TYPE_REFTIME, str::fmtf("2010-09-%02dT00:00:00Z", i));
        Importer importer(*ds);
        gen.generate(importer);
        ds->flush();

        // Run a check to remove new dataset marker
        stringstream checklog;
        ds->check(checklog, true, true);
        if (checklog.str() != "")
            throw wibble::exception::Consistency("running check on correct dataset", "log is not empty: " + checklog.str());

        // Pack to build 'older' archive
        run_repack(15, 8);
        mvlast("older");

        // Pack to build 'last' archive
        run_repack(30, 15);
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

        // Generate a dataset with archived data
        auto_ptr<WritableLocal> ds(WritableLocal::create(cfg));

        // Import several metadata items
        metadata::test::Generator gen("grib1");
        for (int i = 1; i <= 18; ++i)
            gen.add(types::TYPE_REFTIME, str::fmtf("2010-09-%02dT00:00:00Z", i));
        Importer importer(*ds);
        gen.generate(importer);
        ds->flush();

        // Run a check to remove new dataset marker
        stringstream checklog;
        ds->check(checklog, true, true);
        if (checklog.str() != "")
            throw wibble::exception::Consistency("running check on correct dataset", "log is not empty: " + checklog.str());

        // Pack and build 'offline' archive
        run_repack(6, 3);
        ds->archive().rescan_archives();
        mvlast("offline");
        // same as ro, but only the toplevel summary is present
        string ofsum = sys::fs::readFile(str::joinpath(path, ".archive/offline/summary"));
        sys::fs::writeFile(
                str::joinpath(path, ".archive/offline.summary"),
                ofsum);
        sys::fs::rmtree(str::joinpath(path, ".archive/offline"));

        // Pack and build 'wrongro' archive
        run_repack(9, 3);
        ds->archive().rescan_archives();
        mvlast("wrongro");
        // same as ro, but toplevel summary does not match the data
        sys::fs::writeFile(
                str::joinpath(path, ".archive/wrongro.summary"),
                ofsum);

        // Pack and build 'ro' archive
        run_repack(12, 3);
        ds->archive().rescan_archives();
        mvlast("ro");
        // normal archive dir archived to mounted readonly media with dir.summary at the top
        sys::fs::writeFile(
                str::joinpath(path, ".archive/ro.summary"),
                sys::fs::readFile(str::joinpath(path, ".archive/ro/summary")));

        // Pack and build 'old' archive
        run_repack(15, 3);
        ds->archive().rescan_archives();
        mvlast("old");

        // Pack and build 'last' archive
        run_repack(18, 3);
        ds->archive().rescan_archives();
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
        throw wibble::exception::Consistency(
                "creating test scenario \"" + name + "\"",
                "scenario does not exist");
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
// vim:set ts=4 sw=4:
