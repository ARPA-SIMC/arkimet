/**
 * Copyright (C) 2007--2010  ARPA-SIM <urpsim@smr.arpa.emr.it>
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
#include <arki/dataset/test-utils.h>
#include <arki/metadata.h>
#include <arki/metadata/collection.h>
#include <arki/metadata/test-generator.h>
#include <arki/dataset/local.h>
#include <arki/dataset/ondisk2.h>
#include <arki/dataset/simple/reader.h>
#include <arki/dataset/simple/writer.h>
#include <arki/dataset/simple/index.h>
#include <arki/dispatcher.h>
#include <arki/scan/grib.h>
#include <arki/utils.h>
#include <arki/utils/files.h>
#include <wibble/string.h>
#include <wibble/regexp.h>
#include <wibble/grcal/grcal.h>
#include <wibble/sys/fs.h>
#include <fstream>
#include <strings.h>

using namespace std;
using namespace arki;
using namespace wibble;
using namespace arki::types;
using namespace arki::utils;

namespace arki {
namespace tests {

void impl_ensure_dispatches(const wibble::tests::Location& loc, Dispatcher& dispatcher, Metadata& md, metadata::Consumer& mdc)
{
	metadata::Collection c;
	Dispatcher::Outcome res = dispatcher.dispatch(md, c);
	// If dispatch fails, print the notes
	if (res != Dispatcher::DISP_OK)
	{
		for (vector<Metadata>::iterator i = c.begin(); i != c.end(); ++i)
		{
			cerr << "Failed dispatch notes:" << endl;
			std::vector< Item<types::Note> > notes = i->notes();
			for (std::vector< Item<types::Note> >::const_iterator j = notes.begin();
					j != notes.end(); ++j)
				cerr << "   " << *j << endl;
		}
	}
	inner_ensure_equals(res, Dispatcher::DISP_OK);
	for (vector<Metadata>::iterator i = c.begin(); i != c.end(); ++i)
		mdc(*i);
}

OutputChecker::OutputChecker() : split(false) {}

void OutputChecker::splitIfNeeded()
{
	if (split) return;
	Splitter splitter("[\n\r]+", REG_EXTENDED);
	for (Splitter::const_iterator i = splitter.begin(str()); i != splitter.end(); ++i)
		lines.push_back(" " + *i);
	split = true;
}

std::string OutputChecker::join() const
{
	return str::join(lines.begin(), lines.end(), "\n");
}

void OutputChecker::ignore_line_containing(const std::string& needle)
{
	splitIfNeeded();

	for (vector<string>::iterator i = lines.begin();
			i != lines.end(); ++i)
	{
		if ((*i)[0] == '!') continue;

		if (i->find(needle) != std::string::npos )
		{
			(*i)[0] = '!';
			break;
		}
	}
}

void OutputChecker::impl_ensure_line_contains(const wibble::tests::Location& loc, const std::string& needle)
{
	splitIfNeeded();

	bool found = false;
	for (vector<string>::iterator i = lines.begin();
			!found && i != lines.end(); ++i)
	{
		if ((*i)[0] == '!') continue;

		if (i->find(needle) != std::string::npos )
		{
			(*i)[0] = '!';
			found = true;
		}
	}
	
	if (!found)
	{
		std::stringstream ss;
		ss << "'" << join() << "' does not contain '" << needle << "'";
		throw tut::failure(loc.msg(ss.str()));
	}
}

void OutputChecker::impl_ensure_all_lines_seen(const wibble::tests::Location& loc)
{
	splitIfNeeded();

	for (vector<string>::const_iterator i = lines.begin();
			i != lines.end(); ++i)
	{
		if ((*i)[0] != '!')
		{
			std::stringstream ss;
			ss << "'" << join() << "' still contains unchecked lines";
			throw tut::failure(loc.msg(ss.str()));
		}
	}
}

ForceSqlite::ForceSqlite(bool val) : old(dataset::simple::Manifest::get_force_sqlite())
{
	dataset::simple::Manifest::set_force_sqlite(val);
}
ForceSqlite::~ForceSqlite()
{
	dataset::simple::Manifest::set_force_sqlite(old);
}

std::string DatasetTest::idxfname(const ConfigFile* wcfg) const
{
	if (!wcfg) wcfg = &cfg;
	if (wcfg->value("type") == "ondisk2")
		return "index.sqlite";
	else
		return dataset::simple::Manifest::get_force_sqlite() ? "index.sqlite" : "MANIFEST";
}

std::string DatasetTest::arcidxfname() const
{
	return dataset::simple::Manifest::get_force_sqlite() ? "index.sqlite" : "MANIFEST";
}

int DatasetTest::days_since(int year, int month, int day)
{
	// Data are from 07, 08, 10 2007
	int threshold[6] = { year, month, day, 0, 0, 0 };
	int now[6];
	grcal::date::now(now);
	long long int duration = grcal::date::duration(threshold, now);

	//cerr << str::fmt(duration/(3600*24)) + " days";
	return duration/(3600*24);
}

ReadonlyDataset* DatasetTest::makeReader(const ConfigFile* wcfg)
{
	if (!wcfg) wcfg = &cfg;
	ReadonlyDataset* ds = ReadonlyDataset::create(*wcfg);
	ensure(ds);
	return ds;
}

WritableDataset* DatasetTest::makeWriter(const ConfigFile* wcfg)
{
	if (!wcfg) wcfg = &cfg;
	WritableDataset* ds = WritableDataset::create(*wcfg);
	ensure(ds);
	return ds;
}

dataset::Local* DatasetTest::makeLocalReader(const ConfigFile* wcfg)
{
	using namespace arki::dataset;
	ReadonlyDataset* ds = makeReader(wcfg);
	Local* wl = dynamic_cast<Local*>(ds);
	ensure(wl);
	return wl;
}

dataset::WritableLocal* DatasetTest::makeLocalWriter(const ConfigFile* wcfg)
{
	using namespace arki::dataset;
	WritableDataset* ds = makeWriter(wcfg);
	WritableLocal* wl = dynamic_cast<WritableLocal*>(ds);
	ensure(wl);
	return wl;
}

dataset::ondisk2::Reader* DatasetTest::makeOndisk2Reader(const ConfigFile* wcfg)
{
	ReadonlyDataset* ds = makeReader(wcfg);
	dataset::ondisk2::Reader* wl = dynamic_cast<dataset::ondisk2::Reader*>(ds);
	ensure(wl);
	return wl;
}

dataset::ondisk2::Writer* DatasetTest::makeOndisk2Writer(const ConfigFile* wcfg)
{
	WritableDataset* ds = makeWriter(wcfg);
	dataset::ondisk2::Writer* wl = dynamic_cast<dataset::ondisk2::Writer*>(ds);
	ensure(wl);
	return wl;
}

dataset::simple::Reader* DatasetTest::makeSimpleReader(const ConfigFile* wcfg)
{
	ReadonlyDataset* ds = makeReader(wcfg);
	dataset::simple::Reader* wl = dynamic_cast<dataset::simple::Reader*>(ds);
	ensure(wl);
	return wl;
}

dataset::simple::Writer* DatasetTest::makeSimpleWriter(const ConfigFile* wcfg)
{
	WritableDataset* ds = makeWriter(wcfg);
	dataset::simple::Writer* wl = dynamic_cast<dataset::simple::Writer*>(ds);
	ensure(wl);
	return wl;
}

void DatasetTest::clean(const ConfigFile* wcfg)
{
	if (!wcfg) wcfg = &cfg;

	system(("rm -rf " + wcfg->value("path")).c_str());
	system(("mkdir " + wcfg->value("path")).c_str());
}

void DatasetTest::import(const ConfigFile* wcfg, const std::string& testfile)
{
	if (!wcfg) wcfg = &cfg;

	{
		std::auto_ptr<WritableDataset> writer(makeWriter(wcfg));

		scan::Grib scanner;
		scanner.open(testfile);

		Metadata md;
		while (scanner.next(md))
		{
			WritableDataset::AcquireResult res = writer->acquire(md);
			ensure_equals(res, WritableDataset::ACQ_OK);
		}
	}

	utils::files::removeDontpackFlagfile(wcfg->value("path"));
}

void DatasetTest::clean_and_import(const ConfigFile* wcfg, const std::string& testfile)
{
	if (!wcfg) wcfg = &cfg;

	clean(wcfg);
	import(wcfg, testfile);
}

void DatasetTest::impl_ensure_maint_clean(const wibble::tests::Location& loc, size_t filecount, const ConfigFile* wcfg)
{
	auto_ptr<dataset::WritableLocal> writer(makeLocalWriter(wcfg));
	MaintenanceCollector c;
	writer->maintenance(c);
	inner_ensure_equals(c.fileStates.size(), filecount);
	inner_ensure_equals(c.count(OK), filecount);
	inner_ensure_equals(c.remaining(), string());
	inner_ensure(c.isClean());
}


void DatasetTest::impl_ensure_localds_clean(const wibble::tests::Location& loc, size_t filecount, size_t resultcount, const ConfigFile* wcfg)
{
	impl_ensure_maint_clean(loc, filecount, wcfg);

	auto_ptr<dataset::Local> reader(makeLocalReader(wcfg));
	metadata::Collection mdc;
	reader->queryData(dataset::DataQuery(Matcher(), false), mdc);
	inner_ensure_equals(mdc.size(), resultcount);

	if (filecount > 0)
		inner_ensure(files::exists(str::joinpath(reader->path(), idxfname())));
}

Scenario::Scenario() : built(false) {}
Scenario::~Scenario() {}

void Scenario::build()
{
    built = true;
    sys::fs::mkdirIfMissing("scenarios", 0777);
    string path = sys::fs::abspath(str::joinpath("scenarios", name()));
    if (utils::files::exists(path))
        utils::rmtree(path);
    cfg.setValue("path", path);
    cfg.setValue("name", name());
}

struct Ondisk2Archived : public Scenario
{
    std::string name() const { return "ondisk2-archived"; }
    std::string description() const
    {
        return "Ondisk2 dataset containing one grib1 per day for all the month 2010-09."
               " Data from day 1 to 7 is archived in .archive/older; data from day 8 to 15"
               " is archived in .archive/last. Data from 16 to 30 is live."
               " The only changing metadata item is the reference time";
    }
    void build()
    {
        using namespace arki::dataset;

        Scenario::build();
        cfg.setValue("type", "ondisk2");
        cfg.setValue("step", "daily");
        cfg.setValue("unique", "reftime");
        cfg.setValue("archive age", "7");

        // Generate a dataset with archived data
        auto_ptr<WritableLocal> ds(WritableLocal::create(cfg));

        // Import several metadata items
        metadata::test::Generator gen("grib1");
        for (int i = 0; i < 30; ++i)
            gen.add(types::TYPE_REFTIME, str::fmtf("2010-09-%02dT00:00:00Z", i + 1));
        struct Importer : public metadata::Consumer
        {
            WritableLocal& ds;

            Importer(WritableLocal& ds) : ds(ds) {}
            bool operator()(Metadata& md)
            {
                WritableDataset::AcquireResult r = ds.acquire(md);
                ensure(r == WritableDataset::ACQ_OK);
                return true;
            }
        } importer(*ds);
        gen.generate(importer);

        // Check to remove new dataset marker
        stringstream checklog;
        ds->check(checklog, true, true);
        ensure_equals(checklog.str(), "");

        // Pack
        {
            // Override current date for maintenance to 2010-09-15
            dataset::ondisk2::TestOverrideCurrentDateForMaintenance od(1284505200);

            // Archive old files
            stringstream packlog;
            ds->repack(packlog, true);
            ensure_contains(packlog.str(), "6 files archived");
        }

        // Move some archived files to a separate archive dir
        string path = cfg.value("path");
        string last = str::joinpath(path, ".archive/last");
        string older = str::joinpath(path, ".archive/older");
        if (rename(last.c_str(), older.c_str()) < 0)
            throw wibble::exception::System("cannot rename " + last + " to " + older);

        // Pack again
        {
            // Override current date for maintenance to 2010-09-30
            dataset::ondisk2::TestOverrideCurrentDateForMaintenance od(1285801200);

            // Archive old files
            stringstream packlog;
            ds->repack(packlog, true);
            ensure_contains(packlog.str(), "15 files archived");
        }
    }
};

namespace {
struct ScenarioDB : public map<string, Scenario*>
{
    ScenarioDB()
    {
        // Build scenario archive
        add(new Ondisk2Archived);
    }
    void add(Scenario* s)
    {
        insert(make_pair(s->name(), s));
    }
};
}

const Scenario& Scenario::get(const std::string& name)
{
    static ScenarioDB *scenarios = 0;
    if (!scenarios)
        scenarios = new ScenarioDB;
    // Get the scenario
    ScenarioDB::iterator i = scenarios->find(name);
    if (i == scenarios->end())
        throw tut::failure("asked for dataset test scenario \"" + name + "\" which does not exist");
    // Build it if needed
    if (!i->second->built)
        i->second->build();
    return *i->second;
}

}

MaintenanceCollector::MaintenanceCollector()
{
	bzero(counts, sizeof(counts));
}

void MaintenanceCollector::clear()
{
	bzero(counts, sizeof(counts));
	fileStates.clear();
	checked.clear();
}

bool MaintenanceCollector::isClean() const
{
	for (size_t i = 0; i < STATE_MAX; ++i)
		if (i != OK && i != ARC_OK && counts[i])
			return false;
	return true;
}

void MaintenanceCollector::operator()(const std::string& file, State state)
{
	fileStates[file] = state;
	++counts[state];
}

size_t MaintenanceCollector::count(State s)
{
	checked.insert(s);
	return counts[s];
}

std::string MaintenanceCollector::remaining() const
{
	std::vector<std::string> res;
	for (size_t i = 0; i < MaintFileVisitor::STATE_MAX; ++i)
	{
		if (checked.find((State)i) != checked.end())
			continue;
		if (counts[i] == 0)
			continue;
		res.push_back(str::fmtf("%s: %d", names[i], counts[i]));
	}
	return str::join(res.begin(), res.end());
}

void MaintenanceCollector::dump(std::ostream& out) const
{
	using namespace std;
	out << "Results:" << endl;
	for (size_t i = 0; i < STATE_MAX; ++i)
		out << " " << names[i] << ": " << counts[i] << endl;
	for (std::map<std::string, State>::const_iterator i = fileStates.begin();
			i != fileStates.end(); ++i)
		out << "   " << i->first << ": " << names[i->second] << endl;
}

const char* MaintenanceCollector::names[] = {
	"ok",
	"to archive",
	"to delete",
	"to pack",
	"to index",
	"to rescan",
	"deleted",
	"arc ok",
	"arc to index",
	"arc to rescan",
	"arc deleted",
	"state max",
};

}
// vim:set ts=4 sw=4:
