#include "tests.h"
#include "arki/metadata.h"
#include "arki/metadata/collection.h"
#include "arki/dataset/local.h"
#include "arki/dataset/ondisk2.h"
#include "arki/dataset/simple/reader.h"
#include "arki/dataset/simple/writer.h"
#include "arki/dataset/index/manifest.h"
#include "arki/dispatcher.h"
#include "arki/scan/grib.h"
#include "arki/scan/vm2.h"
#include "arki/utils.h"
#include "arki/utils/files.h"
#include "arki/utils/fd.h"
#include "arki/types/timerange.h"
#include "arki/types/area.h"
#include "arki/types/proddef.h"
#include "arki/utils/string.h"
#include "arki/utils/sys.h"
#include <arki/wibble/regexp.h>
#include <arki/wibble/grcal/grcal.h>
#include <algorithm>
#include <fstream>
#include <cstring>
#include <limits.h>
#include <sys/time.h>
#include <sys/types.h>
#include <sys/stat.h>
#include <fcntl.h>
#include <iostream>

using namespace std;
using namespace arki;
using namespace arki::tests;
using namespace arki::types;
using namespace arki::utils;
using namespace arki::dataset;

namespace arki {
namespace tests {

unsigned count_results(Reader& ds, const dataset::DataQuery& dq)
{
    unsigned count = 0;
    ds.query_data(dq, [&](unique_ptr<Metadata>) { ++count; return true; });
    return count;
}

void impl_ensure_dispatches(Dispatcher& dispatcher, unique_ptr<Metadata> md, metadata_dest_func mdc)
{
    metadata::Collection c;
    Dispatcher::Outcome res = dispatcher.dispatch(move(md), c.inserter_func());
    // If dispatch fails, print the notes
    if (res != Dispatcher::DISP_OK)
    {
        for (vector<Metadata*>::const_iterator i = c.begin(); i != c.end(); ++i)
        {
            cerr << "Failed dispatch notes:" << endl;
            std::vector<Note> notes = (*i)->notes();
            for (std::vector<Note>::const_iterator j = notes.begin();
                    j != notes.end(); ++j)
                cerr << "   " << *j << endl;
        }
    }
    wassert(actual(res) == Dispatcher::DISP_OK);
    c.move_to(mdc);
}

OutputChecker::OutputChecker() : split(false) {}

void OutputChecker::splitIfNeeded()
{
    if (split) return;
    wibble::Splitter splitter("[\n\r]+", REG_EXTENDED);
    for (wibble::Splitter::const_iterator i = splitter.begin(str()); i != splitter.end(); ++i)
        lines.push_back(" " + *i);
    split = true;
}

std::string OutputChecker::join() const
{
    return str::join("\n", lines.begin(), lines.end());
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

void OutputChecker::ensure_line_contains(const std::string& needle)
{
    splitIfNeeded();

    bool found = false;
    for (auto& i : lines)
    {
        if (i[0] == '!') continue;

        if (i.find(needle) != std::string::npos)
        {
            i[0] = '!';
            found = true;
        }
    }

    if (!found)
    {
        std::stringstream ss;
        ss << "'" << join() << "' does not contain '" << needle << "'";
        throw TestFailed(ss.str());
    }
}

void OutputChecker::ensure_all_lines_seen()
{
    splitIfNeeded();

    for (vector<string>::const_iterator i = lines.begin();
            i != lines.end(); ++i)
    {
        if ((*i)[0] != '!')
        {
            std::stringstream ss;
            ss << "'" << join() << "' still contains unchecked lines";
            throw TestFailed(ss.str());
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

void LineChecker::check(const std::string& s) const
{
    vector<Line> lines;
    wibble::Splitter splitter("[\n\r]+", REG_EXTENDED);
    for (wibble::Splitter::const_iterator i = splitter.begin(s); i != splitter.end(); ++i)
        lines.push_back(" " + *i);

    // Mark ignored lines as seen
    for (vector<string>::const_iterator i = ignore_regexps.begin(); i != ignore_regexps.end(); ++i)
    {
        wibble::ERegexp re(*i);
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
        wibble::ERegexp re(*i);
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
        throw TestFailed(complaints.str());
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
    wibble::grcal::date::now(now);
    long long int duration = wibble::grcal::date::duration(threshold, now);

    //cerr << str::fmt(duration/(3600*24)) + " days";
    return duration/(3600*24);
}

#if 0
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
#endif

DatasetTest::DatasetTest()
{
    //if (default_datasettest_config)
        //cfg = *default_datasettest_config;
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

Reader* DatasetTest::makeReader(const ConfigFile* wcfg)
{
	if (!wcfg) wcfg = &cfg;
	Reader* ds = Reader::create(*wcfg);
	ensure(ds);
	return ds;
}

Writer* DatasetTest::makeWriter(const ConfigFile* wcfg)
{
    if (!wcfg) wcfg = &cfg;
    Writer* ds = Writer::create(*wcfg);
    ensure(ds);
    return ds;
}

dataset::LocalReader* DatasetTest::makeLocalReader(const ConfigFile* wcfg)
{
	using namespace arki::dataset;
	Reader* ds = makeReader(wcfg);
	LocalReader* wl = dynamic_cast<LocalReader*>(ds);
	ensure(wl);
	return wl;
}

dataset::SegmentedWriter* DatasetTest::makeLocalWriter(const ConfigFile* wcfg)
{
    using namespace arki::dataset;
    Writer* ds = makeWriter(wcfg);
    SegmentedWriter* wl = dynamic_cast<SegmentedWriter*>(ds);
    ensure(wl);
    return wl;
}

dataset::ondisk2::Reader* DatasetTest::makeOndisk2Reader(const ConfigFile* wcfg)
{
	Reader* ds = makeReader(wcfg);
	dataset::ondisk2::Reader* wl = dynamic_cast<dataset::ondisk2::Reader*>(ds);
	ensure(wl);
	return wl;
}

dataset::ondisk2::Writer* DatasetTest::makeOndisk2Writer(const ConfigFile* wcfg)
{
    Writer* ds = makeWriter(wcfg);
    dataset::ondisk2::Writer* wl = dynamic_cast<dataset::ondisk2::Writer*>(ds);
    ensure(wl);
    return wl;
}

dataset::simple::Reader* DatasetTest::makeSimpleReader(const ConfigFile* wcfg)
{
	Reader* ds = makeReader(wcfg);
	dataset::simple::Reader* wl = dynamic_cast<dataset::simple::Reader*>(ds);
	ensure(wl);
	return wl;
}

dataset::simple::Writer* DatasetTest::makeSimpleWriter(const ConfigFile* wcfg)
{
    Writer* ds = makeWriter(wcfg);
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
        std::unique_ptr<Writer> writer(makeWriter(wcfg));

        if (str::endswith(testfile, ".vm2")) {
            scan::Vm2 scanner;
            scanner.open(testfile);

            Metadata md;
            while (scanner.next(md))
            {
                Writer::AcquireResult res = writer->acquire(md);
                ensure_equals(res, Writer::ACQ_OK);
            }
        } else {

            scan::Grib scanner;
            scanner.open(testfile);

            Metadata md;
            while (scanner.next(md))
            {
                Writer::AcquireResult res = writer->acquire(md);
                ensure_equals(res, Writer::ACQ_OK);
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

void DatasetTest::ensure_maint_clean(size_t filecount, const ConfigFile* wcfg)
{
    unique_ptr<dataset::SegmentedWriter> writer(makeLocalWriter(wcfg));
    arki::tests::MaintenanceResults expected(true, filecount);
    expected.by_type[COUNTED_OK] = filecount;
    wassert(actual(writer.get()).maintenance(expected));
}


void DatasetTest::ensure_localds_clean(size_t filecount, size_t resultcount, const ConfigFile* wcfg)
{
    wassert(ensure_maint_clean(filecount, wcfg));

    unique_ptr<dataset::LocalReader> reader(makeLocalReader(wcfg));
    metadata::Collection mdc(*reader, Matcher());
    wassert(actual(mdc.size()) == resultcount);

    if (filecount > 0)
        wassert(actual_file(str::joinpath(reader->path(), idxfname())).exists());
}

void DatasetTest::import_all(const testdata::Fixture& fixture)
{
    clean();

    std::unique_ptr<LocalWriter> writer(makeLocalWriter());
    for (int i = 0; i < 3; ++i)
    {
        import_results[i] = fixture.test_data[i].md;
        Writer::AcquireResult res = writer->acquire(import_results[i]);
        wassert(actual(res) == Writer::ACQ_OK);
    }

    utils::files::removeDontpackFlagfile(cfg.value("path"));
}

void DatasetTest::import_all_packed(const testdata::Fixture& fixture)
{
    wruntest(import_all, fixture);

    // Pack the dataset in case something imported data out of order
    {
        unique_ptr<LocalWriter> writer(makeLocalWriter());
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
        char buf[32];
        snprintf(buf, 32, "%s: %zd", names[i], counts[i]);
        res.push_back(buf);
    }
    return str::join(", ", res.begin(), res.end());
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
bool OrderCheck::eat(unique_ptr<Metadata>&& md)
{
    if (!first)
    {
        if (order->compare(old, *md) > 0)
        {
            stringstream msg;
            old.writeYaml(msg);
            msg << " should come after ";
            md->writeYaml(msg);
            throw wibble::exception::Consistency(
                    "checking order of a metadata stream",
                    msg.str());
        }
    }
    old = *md;
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

void corrupt_datafile(const std::string& absname)
{
    files::PreserveFileTimes pft(absname);

    string to_corrupt = absname;
    if (sys::isdir(absname))
    {
        // Pick the first element in the dir segment for corruption
        string format = utils::require_format(absname);
        sys::Path dir(absname);
        unsigned long first = ULONG_MAX;
        string selected;
        for (sys::Path::iterator i = dir.begin(); i != dir.end(); ++i)
        {
            if (!i.isreg()) continue;
            if (strcmp(i->d_name, ".sequence") == 0 || !str::endswith(i->d_name, format)) continue;
            unsigned long cur = strtoul(i->d_name, 0, 10);
            if (cur < first)
            {
                first = cur;
                selected = i->d_name;
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

std::unique_ptr<dataset::LocalWriter> make_dataset_writer(const std::string& cfgstr, bool empty)
{
    // Parse configuration
    stringstream incfg(cfgstr);
    ConfigFile cfg;
    cfg.parse(incfg, "(memory)");
    wassert(actual(cfg.value("path").empty()).isfalse());

    // Remove the dataset directory if it exists
    if (empty && sys::isdir(cfg.value("path"))) sys::rmtree(cfg.value("path"));

    unique_ptr<dataset::LocalWriter> ds(dataset::LocalWriter::create(cfg));
    wassert(actual(ds.get()).istrue());
    return ds;
}

std::unique_ptr<Reader> make_dataset_reader(const std::string& cfgstr)
{
    // Parse configuration
    stringstream incfg(cfgstr);
    ConfigFile cfg;
    cfg.parse(incfg, "(memory)");
    wassert(actual(cfg.value("path").empty()).isfalse());

    unique_ptr<Reader> ds(Reader::create(cfg));
    wassert(actual(ds.get()).istrue());
    return ds;
}

void test_append_transaction_ok(dataset::data::Segment* dw, Metadata& md, int append_amount_adjust)
{
    typedef types::source::Blob Blob;

    // Make a snapshot of everything before appending
    unique_ptr<Source> orig_source(md.source().clone());
    size_t data_size = md.data_size();
    size_t orig_fsize = sys::size(dw->absname, 0);

    // Start the append transaction, nothing happens until commit
    off_t ofs;
    Pending p = dw->append(md, &ofs);
    wassert(actual((size_t)ofs) == orig_fsize);
    wassert(actual(sys::size(dw->absname)) == orig_fsize);
    wassert(actual_type(md.source()) == *orig_source);

    // Commit
    p.commit();

    // After commit, data is appended
    wassert(actual(sys::size(dw->absname)) == orig_fsize + data_size + append_amount_adjust);

    // And metadata is updated
    wassert(actual_type(md.source()).is_source_blob("grib1", "", dw->absname, orig_fsize, data_size));
}

void test_append_transaction_rollback(dataset::data::Segment* dw, Metadata& md)
{
    // Make a snapshot of everything before appending
    unique_ptr<Source> orig_source(md.source().clone());
    size_t orig_fsize = sys::size(dw->absname, 0);

    // Start the append transaction, nothing happens until commit
    off_t ofs;
    Pending p = dw->append(md, &ofs);
    wassert(actual((size_t)ofs) == orig_fsize);
    wassert(actual(sys::size(dw->absname, 0)) == orig_fsize);
    wassert(actual_type(md.source()) == *orig_source);

    // Rollback
    p.rollback();

    // After rollback, nothing has changed
    wassert(actual(sys::size(dw->absname, 0)) == orig_fsize);
    wassert(actual_type(md.source()) == *orig_source);
}

/// Run maintenance and see that the results are as expected
void ActualSegmentedWriter::maintenance(const MaintenanceResults& expected, bool quick)
{
    MaintenanceCollector c;
    wassert(_actual->maintenance(c, quick));

    bool ok = true;
    if (expected.files_seen != -1 && c.fileStates.size() != (unsigned)expected.files_seen)
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
    throw TestFailed(ss.str());
}

void ActualSegmentedWriter::maintenance_clean(unsigned data_count, bool quick)
{
    MaintenanceResults expected(true, data_count);
    expected.by_type[tests::DatasetTest::COUNTED_OK] = data_count;
    maintenance(expected, quick);
}

template<typename Dataset>
void ActualLocalWriter<Dataset>::repack(const LineChecker& expected, bool write)
{
    stringstream s;
    wassert(this->_actual->repack(s, write));
    wassert(expected.check(s.str()));
}

template<typename Dataset>
void ActualLocalWriter<Dataset>::repack_clean(bool write)
{
    LineChecker expected;
    expected.ignore_regexp("total bytes freed.");
    repack(expected, write);
}

template<typename Dataset>
void ActualLocalWriter<Dataset>::check(const LineChecker& expected, bool write, bool quick)
{
    stringstream s;
    wassert(this->_actual->check(s, write, quick));
    wassert(expected.check(s.str()));
}

template<typename Dataset>
void ActualLocalWriter<Dataset>::check_clean(bool write)
{
    LineChecker expected;
    check(expected, write);
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
    md.set_source_inline(format, vector<uint8_t>(size));
    md.set("origin", "GRIB1(200, 10, 100)");
    md.set("product", "GRIB1(3, 4, 5)");
    md.set("level", "GRIB1(1, 2)");
    md.set("timerange", "GRIB1(4, 5s, 6s)");
    md.set(Reftime::createPosition(Time(2014, month, day, hour, 0, 0)));
    md.set("area", "GRIB(foo=5,bar=5000)");
    md.set("proddef", "GRIB(foo=5,bar=5000)");
    md.add_note("this is a test");
    return md;
}

}

template class ActualLocalWriter<dataset::LocalWriter>;
template class ActualLocalWriter<dataset::SegmentedWriter>;
}
