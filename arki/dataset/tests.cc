/**
 * Copyright (C) 2007--2015  ARPA-SIM <urpsim@smr.arpa.emr.it>
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

#include "tests.h"
#include <arki/metadata.h>
#include <arki/metadata/collection.h>
#include <arki/dataset/local.h>
#include <arki/dataset/ondisk2.h>
#include <arki/dataset/simple/reader.h>
#include <arki/dataset/simple/writer.h>
#include <arki/dataset/index/manifest.h>
#include <arki/dispatcher.h>
#include <arki/scan/grib.h>
#include <arki/scan/vm2.h>
#include <arki/utils.h>
#include <arki/utils/files.h>
#include <arki/utils/fd.h>
#include <arki/types/timerange.h>
#include <arki/types/area.h>
#include <arki/types/proddef.h>
#include <wibble/string.h>
#include <wibble/sys/fs.h>
#include <wibble/regexp.h>
#include <wibble/grcal/grcal.h>
#include <algorithm>
#include <fstream>
#include <cstring>
#include <limits.h>
#include <sys/time.h>
#include <sys/types.h>
#include <sys/stat.h>
#include <fcntl.h>

using namespace std;
using namespace arki;
using namespace wibble;
using namespace wibble::tests;
using namespace arki::types;
using namespace arki::utils;
using namespace arki::dataset;

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
            std::vector<Note> notes = i->notes();
            for (std::vector<Note>::const_iterator j = notes.begin();
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

void LineChecker::ignore_regexp(const std::string& regexp)
{
    ignore_regexps.push_back(regexp);
}

void LineChecker::require_line_contains(const std::string& needle)
{
    require_contains.push_back(needle);
}

void LineChecker::require_line_contains_re(const std::string& needle)
{
    require_contains_re.push_back(needle);
}

namespace {
struct Line : public std::string
{
    bool seen;
    Line() : seen(false) {}
    Line(const std::string& s) : std::string(s), seen(false) {}
};
}

void LineChecker::check(WIBBLE_TEST_LOCPRM, const std::string& s) const
{
    vector<Line> lines;
    Splitter splitter("[\n\r]+", REG_EXTENDED);
    for (Splitter::const_iterator i = splitter.begin(s); i != splitter.end(); ++i)
        lines.push_back(" " + *i);

    // Mark ignored lines as seen
    for (vector<string>::const_iterator i = ignore_regexps.begin(); i != ignore_regexps.end(); ++i)
    {
        ERegexp re(*i);
        for (vector<Line>::iterator j = lines.begin(); j != lines.end(); ++j)
            if (re.match(*j))
                j->seen = true;
    }

    stringstream complaints;

    // Check required lines
    for (vector<string>::const_iterator i = require_contains.begin(); i != require_contains.end(); ++i)
    {
        bool found = false;
        for (vector<Line>::iterator j = lines.begin(); j != lines.end(); ++j)
        {
            if (j->find(*i) == string::npos) continue;
            if (j->seen) continue;
            j->seen = true;
            found = true;
            break;
        }
        // Complain about required line i not found
        if (!found)
            complaints << "Required match not found: \"" << *i << "\"" << endl;
    }
    for (vector<string>::const_iterator i = require_contains_re.begin(); i != require_contains_re.end(); ++i)
    {
        ERegexp re(*i);
        bool found = false;
        for (vector<Line>::iterator j = lines.begin(); j != lines.end(); ++j)
        {
            if (!re.match(*j)) continue;
            if (j->seen) continue;
            found = true;
            j->seen = true;
            break;
        }
        // Complain about required line i not found
        if (!found)
            complaints << "Required regexp not matched: \"" << *i << "\"" << endl;
    }

    for (vector<Line>::const_iterator i = lines.begin(); i != lines.end(); ++i)
        if (!i->seen)
            complaints << "Output line not checked: \"" << *i << "\"" << endl;

    if (!complaints.str().empty())
        wibble_test_location.fail_test(complaints.str());
}

ForceSqlite::ForceSqlite(bool val) : old(dataset::index::Manifest::get_force_sqlite())
{
	dataset::index::Manifest::set_force_sqlite(val);
}
ForceSqlite::~ForceSqlite()
{
	dataset::index::Manifest::set_force_sqlite(old);
}

int days_since(int year, int month, int day)
{
    // Data are from 07, 08, 10 2007
    int threshold[6] = { year, month, day, 0, 0, 0 };
    int now[6];
    grcal::date::now(now);
    long long int duration = grcal::date::duration(threshold, now);

    //cerr << str::fmt(duration/(3600*24)) + " days";
    return duration/(3600*24);
}

namespace {
const ConfigFile* default_datasettest_config = 0;
}

DatasetTestDefaultConfig::DatasetTestDefaultConfig(const ConfigFile& cfg)
{
    default_datasettest_config = &cfg;
}
DatasetTestDefaultConfig::~DatasetTestDefaultConfig()
{
    default_datasettest_config = 0;
}

DatasetTest::DatasetTest()
    : segment_manager(0)
{
    if (default_datasettest_config)
        cfg = *default_datasettest_config;
}
DatasetTest::~DatasetTest()
{
    if (segment_manager) delete segment_manager;
}

dataset::data::SegmentManager& DatasetTest::segments()
{
    if (!segment_manager)
        segment_manager = dataset::data::SegmentManager::get(cfg).release();
    return *segment_manager;
}

std::string DatasetTest::idxfname(const ConfigFile* wcfg) const
{
	if (!wcfg) wcfg = &cfg;
	if (wcfg->value("type") == "ondisk2")
		return "index.sqlite";
	else
		return dataset::index::Manifest::get_force_sqlite() ? "index.sqlite" : "MANIFEST";
}

std::string DatasetTest::arcidxfname() const
{
	return dataset::index::Manifest::get_force_sqlite() ? "index.sqlite" : "MANIFEST";
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

		if (wibble::str::endsWith(testfile, ".vm2")) {
			scan::Vm2 scanner;
			scanner.open(testfile);

			Metadata md;
			while (scanner.next(md))
			{
				WritableDataset::AcquireResult res = writer->acquire(md);
				ensure_equals(res, WritableDataset::ACQ_OK);
			}
		} else {

			scan::Grib scanner;
			scanner.open(testfile);

			Metadata md;
			while (scanner.next(md))
			{
				WritableDataset::AcquireResult res = writer->acquire(md);
				ensure_equals(res, WritableDataset::ACQ_OK);
			}
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

void DatasetTest::impl_ensure_maint_clean(WIBBLE_TEST_LOCPRM, size_t filecount, const ConfigFile* wcfg)
{
    auto_ptr<dataset::WritableLocal> writer(makeLocalWriter(wcfg));
    arki::tests::MaintenanceResults expected(true, filecount);
    expected.by_type[COUNTED_OK] = filecount;
    wassert(actual(writer.get()).maintenance(expected));
}


void DatasetTest::impl_ensure_localds_clean(const wibble::tests::Location& loc, size_t filecount, size_t resultcount, const ConfigFile* wcfg)
{
	impl_ensure_maint_clean(loc, filecount, wcfg);

	auto_ptr<dataset::Local> reader(makeLocalReader(wcfg));
	metadata::Collection mdc;
	reader->queryData(dataset::DataQuery(Matcher(), false), mdc);
	inner_ensure_equals(mdc.size(), resultcount);

	if (filecount > 0)
		inner_ensure(sys::fs::exists(str::joinpath(reader->path(), idxfname())));
}

void DatasetTest::import_all(WIBBLE_TEST_LOCPRM, const testdata::Fixture& fixture)
{
    clean();

    std::auto_ptr<WritableLocal> writer(makeLocalWriter());
    for (int i = 0; i < 3; ++i)
    {
        import_results[i] = fixture.test_data[i].md;
        WritableDataset::AcquireResult res = writer->acquire(import_results[i]);
        wassert(actual(res) == WritableDataset::ACQ_OK);
    }

    utils::files::removeDontpackFlagfile(cfg.value("path"));
}

void DatasetTest::import_all_packed(WIBBLE_TEST_LOCPRM, const testdata::Fixture& fixture)
{
    wruntest(import_all, fixture);

    // Pack the dataset in case something imported data out of order
    {
        auto_ptr<WritableLocal> writer(makeLocalWriter());
        LineChecker checker;
        checker.ignore_regexp(": packed ");
        checker.ignore_regexp(": [0-9]+ files? packed");
        wassert(actual(writer.get()).repack(checker, true));
    }
}


}

MaintenanceCollector::MaintenanceCollector()
{
    memset(counts, 0, sizeof(counts));
}

void MaintenanceCollector::clear()
{
    memset(counts, 0, sizeof(counts));
    fileStates.clear();
    checked.clear();
}

bool MaintenanceCollector::isClean() const
{
	for (size_t i = 0; i < tests::DatasetTest::COUNTED_MAX; ++i)
		if (i != tests::DatasetTest::COUNTED_OK && i != tests::DatasetTest::COUNTED_ARC_OK && counts[i])
			return false;
	return true;
}

void MaintenanceCollector::operator()(const std::string& file, dataset::data::FileState state)
{
    using namespace arki::dataset;

    fileStates[file] = state;
    if (state.is_ok())           ++counts[tests::DatasetTest::COUNTED_OK];
    if (state.is_archived_ok())  ++counts[tests::DatasetTest::COUNTED_ARC_OK];
    if (state.has(FILE_ARCHIVED))
    {
        if (state.has(FILE_TO_INDEX))   ++counts[tests::DatasetTest::COUNTED_ARC_TO_INDEX];
        if (state.has(FILE_TO_RESCAN))  ++counts[tests::DatasetTest::COUNTED_ARC_TO_RESCAN];
        if (state.has(FILE_TO_DEINDEX)) ++counts[tests::DatasetTest::COUNTED_ARC_TO_DEINDEX];
    } else {
        if (state.has(FILE_TO_ARCHIVE)) ++counts[tests::DatasetTest::COUNTED_TO_ARCHIVE];
        if (state.has(FILE_TO_DELETE))  ++counts[tests::DatasetTest::COUNTED_TO_DELETE];
        if (state.has(FILE_TO_PACK))    ++counts[tests::DatasetTest::COUNTED_TO_PACK];
        if (state.has(FILE_TO_INDEX))   ++counts[tests::DatasetTest::COUNTED_TO_INDEX];
        if (state.has(FILE_TO_RESCAN))  ++counts[tests::DatasetTest::COUNTED_TO_RESCAN];
        if (state.has(FILE_TO_DEINDEX)) ++counts[tests::DatasetTest::COUNTED_TO_DEINDEX];
    }
}

size_t MaintenanceCollector::count(tests::DatasetTest::Counted s)
{
    checked.insert(s);
    return counts[s];
}

std::string MaintenanceCollector::remaining() const
{
    std::vector<std::string> res;
    for (size_t i = 0; i < tests::DatasetTest::COUNTED_MAX; ++i)
    {
        if (checked.find((Counted)i) != checked.end())
            continue;
        if (counts[i] == 0)
            continue;
        res.push_back(str::fmtf("%s: %zd", names[i], counts[i]));
    }
    return str::join(res.begin(), res.end());
}

void MaintenanceCollector::dump(std::ostream& out) const
{
    using namespace std;
    out << "Results:" << endl;
    for (size_t i = 0; i < tests::DatasetTest::COUNTED_MAX; ++i)
        out << " " << names[i] << ": " << counts[i] << endl;
    /*
       for (std::map<std::string, unsigned>::const_iterator i = fileStates.begin();
       i != fileStates.end(); ++i)
       out << "   " << i->first << ": " << names[i->second] << endl;
    */
}

const char* MaintenanceCollector::names[] = {
    "ok",
    "arc ok",
    "to archive",
    "to delete",
    "to pack",
    "to index",
    "to rescan",
    "to deindex",
    "arc to index",
    "arc to rescan",
    "arc deindex",
    "counted_max",
};

OrderCheck::OrderCheck(const std::string& order)
    : order(sort::Compare::parse(order)), first(true)
{
}
OrderCheck::~OrderCheck()
{
}
bool OrderCheck::operator()(Metadata& md)
{
    if (!first)
    {
        if (order->compare(old, md) > 0)
        {
            stringstream msg;
            old.writeYaml(msg);
            msg << " should come after ";
            md.writeYaml(msg);
            throw wibble::exception::Consistency(
                    "checking order of a metadata stream",
                    msg.str());
        }
    }
    old = md;
    first = false;
    return true;
}

namespace tests {

static ostream& operator<<(ostream& out, const MaintenanceResults& c)
{
    out << "clean: ";
    switch (c.is_clean)
    {
        case -1: out << "-"; break;
        case 0: out << "false"; break;
        default: out << "true"; break;
    }

    out << ", files: ";
    if (c.files_seen == -1)
        out << "-";
    else
        out << c.files_seen;

    for (unsigned i = 0; i < tests::DatasetTest::COUNTED_MAX; ++i)
        if (c.by_type[i] != -1)
        {
            out << ", " << MaintenanceCollector::names[i] << ": " << c.by_type[i];
        }

    return out;
}

static ostream& operator<<(ostream& out, const MaintenanceCollector& c)
{
    out << "clean: " << (c.isClean() ? "true" : "false");
    out << ", files: " << c.fileStates.size();
    for (size_t i = 0; i < tests::DatasetTest::COUNTED_MAX; ++i)
        if (c.counts[i] > 0)
            out << ", " << MaintenanceCollector::names[i] << ": " << c.counts[i];
    return out;
}

void TestMaintenance::check(WIBBLE_TEST_LOCPRM) const
{
    using namespace wibble::tests;

    MaintenanceCollector c;
    wrunchecked(dataset.maintenance(c, quick));

    bool ok = true;
    if (expected.files_seen != -1 &&
        c.fileStates.size() != (unsigned)expected.files_seen)
        ok = false;

    for (unsigned i = 0; i < tests::DatasetTest::COUNTED_MAX; ++i)
        if (expected.by_type[i] != -1 &&
            c.count((tests::DatasetTest::Counted)i) != (unsigned)expected.by_type[i])
            ok = false;

    if (c.remaining() != "")
        ok = false;

    if (expected.is_clean != -1)
    {
        if (expected.is_clean)
            ok = ok && c.isClean();
        else
            ok = ok && !c.isClean();
    }

    if (ok) return;

    std::stringstream ss;
    ss << "maintenance gave '" << c << "' instead of the expected '" << expected << "'";
    wibble_test_location.fail_test(ss.str());
}

void TestRepack::check(WIBBLE_TEST_LOCPRM) const
{
    using namespace wibble::tests;

    stringstream s;
    wrunchecked(dataset.repack(s, write));
    wruntest(expected.check, s.str());
}

void TestCheck::check(WIBBLE_TEST_LOCPRM) const
{
    using namespace wibble::tests;

    stringstream s;
    wrunchecked(dataset.check(s, write, quick));
    wruntest(expected.check, s.str());
}

void corrupt_datafile(const std::string& absname)
{
    files::PreserveFileTimes pft(absname);

    string to_corrupt = absname;
    if (sys::fs::isdir(absname))
    {
        // Truncate dir segment
        string format = utils::require_format(absname);
        sys::fs::Directory dir(absname);
        unsigned long first = ULONG_MAX;
        string selected;
        for (sys::fs::Directory::const_iterator i = dir.begin(); i != dir.end(); ++i)
        {
            if (!i.isreg()) continue;
            if (*i != ".sequence" && !str::endsWith(*i, format)) continue;
            unsigned long cur = strtoul((*i).c_str(), 0, 10);
            if (cur < first)
            {
                first = cur;
                selected = *i;
            }
        }
        if (selected.empty())
            throw wibble::exception::Consistency("corrupting " + absname, "no files found to corrupt");
        to_corrupt = str::joinpath(absname, selected);
    }

    // Corrupt the beginning of the file
    int fd = open(to_corrupt.c_str(), O_RDWR);
    if (fd == -1)
        throw wibble::exception::File(to_corrupt, "cannot open file");
    fd::HandleWatch hw(to_corrupt, fd);
    ssize_t written = pwrite(fd, "\0\0\0\0", 4, 0);
    if (written < 0)
        throw wibble::exception::File(to_corrupt, "cannot write to file");
    if (written != 4)
        throw wibble::exception::Consistency("corrupting " + to_corrupt, "wrote less than 4 bytes");
}

std::auto_ptr<dataset::WritableLocal> make_dataset_writer(const std::string& cfgstr, bool empty)
{
    // Parse configuration
    stringstream incfg(cfgstr);
    ConfigFile cfg;
    cfg.parse(incfg, "(memory)");
    wassert(actual(cfg.value("path").empty()).isfalse());

    // Remove the dataset directory if it exists
    if (empty && sys::fs::isdir(cfg.value("path"))) sys::fs::rmtree(cfg.value("path"));

    auto_ptr<dataset::WritableLocal> ds(dataset::WritableLocal::create(cfg));
    wassert(actual(ds.get()).istrue());
    return ds;
}

std::auto_ptr<ReadonlyDataset> make_dataset_reader(const std::string& cfgstr)
{
    // Parse configuration
    stringstream incfg(cfgstr);
    ConfigFile cfg;
    cfg.parse(incfg, "(memory)");
    wassert(actual(cfg.value("path").empty()).isfalse());

    auto_ptr<ReadonlyDataset> ds(ReadonlyDataset::create(cfg));
    wassert(actual(ds.get()).istrue());
    return ds;
}

void test_append_transaction_ok(WIBBLE_TEST_LOCPRM, dataset::data::Writer* dw, Metadata& md)
{
    typedef types::source::Blob Blob;

    // Make a snapshot of everything before appending
    auto_ptr<Source> orig_source(md.source().clone());
    size_t data_size = md.dataSize();
    size_t orig_fsize = sys::fs::size(dw->absname, 0);

    // Start the append transaction, nothing happens until commit
    off_t ofs;
    Pending p = dw->append(md, &ofs);
    wassert(actual((size_t)ofs) == orig_fsize);
    wassert(actual(sys::fs::size(dw->absname)) == orig_fsize);
    wassert(actual_type(md.source()) == *orig_source);

    // Commit
    p.commit();

    // After commit, data is appended
    wassert(actual(sys::fs::size(dw->absname)) == orig_fsize + data_size);

    // And metadata is updated
    wassert(actual_type(md.source()).is_source_blob("grib", "", dw->absname, orig_fsize, data_size));
}

void test_append_transaction_rollback(WIBBLE_TEST_LOCPRM, dataset::data::Writer* dw, Metadata& md)
{
    // Make a snapshot of everything before appending
    auto_ptr<Source> orig_source(md.source().clone());
    size_t orig_fsize = sys::fs::size(dw->absname, 0);

    // Start the append transaction, nothing happens until commit
    off_t ofs;
    Pending p = dw->append(md, &ofs);
    wassert(actual((size_t)ofs) == orig_fsize);
    wassert(actual(sys::fs::size(dw->absname, 0)) == orig_fsize);
    wassert(actual_type(md.source()) == *orig_source);

    // Rollback
    p.rollback();

    // After rollback, nothing has changed
    wassert(actual(sys::fs::size(dw->absname, 0)) == orig_fsize);
    wassert(actual_type(md.source()) == *orig_source);
}

}

namespace testdata {

void Fixture::finalise_init()
{
    // Compute selective_cutoff
    Time tmin = test_data[0].time;
    //Item<types::Time> tmax = test_data[0].time;
    for (int i = 2; i < 3; ++i)
    {
        tmin = min(tmin, test_data[i].time);
        //tmax = max(tmax, test_data[i].time);
    }
    for (int i = 0; i < 6; ++i)
        selective_cutoff[i] = tmin.vals[i];
    ++selective_cutoff[1];
    wibble::grcal::date::normalise(selective_cutoff);

    Time cutoff(selective_cutoff);
    for (int i = 0; i < 3; ++i)
    {
        fnames.insert(test_data[i].destfile);
        if (test_data[i].time < cutoff)
            fnames_before_cutoff.insert(test_data[i].destfile);
        else
            fnames_after_cutoff.insert(test_data[i].destfile);
    }
}

unsigned Fixture::count_dataset_files() const
{
    return fnames.size();
}

unsigned Fixture::selective_days_since() const
{
    return tests::days_since(selective_cutoff[0], selective_cutoff[1], selective_cutoff[2]);
}

Metadata make_large_mock(const std::string& format, size_t size, unsigned month, unsigned day, unsigned hour)
{
    Metadata md;
    md.set_source(Source::createInline("grib", size));
    md.set("origin", "GRIB1(200, 10, 100)");
    md.set("product", "GRIB1(3, 4, 5)");
    md.set("level", "GRIB1(1, 2)");
    md.set("timerange", "GRIB1(4, 5, 6, 7)");
    md.set(Reftime::createPosition(Time(2014, month, day, hour, 0, 0)));
    md.set("area", "GRIB(foo=5,bar=5000)");
    md.set("proddef", "GRIB(foo=5,bar=5000)");
    md.add_note("this is a test");
    return md;
}

}

}
// vim:set ts=4 sw=4:
