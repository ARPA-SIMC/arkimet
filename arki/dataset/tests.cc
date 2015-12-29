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

dataset::segment::SegmentManager& DatasetTest::segments()
{
    if (!segment_manager)
        segment_manager = dataset::segment::SegmentManager::get(cfg).release();
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

std::string manifest_idx_fname()
{
    return dataset::index::Manifest::get_force_sqlite() ? "index.sqlite" : "MANIFEST";
}

std::string DatasetTest::arcidxfname() const
{
	return dataset::index::Manifest::get_force_sqlite() ? "index.sqlite" : "MANIFEST";
}

std::unique_ptr<Reader> DatasetTest::makeReader(const ConfigFile* wcfg)
{
    if (!wcfg) wcfg = &cfg;
    unique_ptr<Reader> ds(Reader::create(*wcfg));
    wassert(actual(ds.get()));
    return ds;
}

std::unique_ptr<Writer> DatasetTest::makeWriter(const ConfigFile* wcfg)
{
    if (!wcfg) wcfg = &cfg;
    unique_ptr<Writer> ds(Writer::create(*wcfg));
    wassert(actual(ds.get()));
    return ds;
}

std::unique_ptr<Checker> DatasetTest::makeChecker(const ConfigFile* wcfg)
{
    if (!wcfg) wcfg = &cfg;
    unique_ptr<Checker> ds(Checker::create(*wcfg));
    wassert(actual(ds.get()));
    return ds;
}

std::unique_ptr<dataset::SegmentedReader> DatasetTest::makeLocalReader(const ConfigFile* wcfg)
{
    auto ds = makeReader(wcfg);
    wassert(actual(ds.get()));
    unique_ptr<dataset::SegmentedReader> wl(dynamic_cast<dataset::SegmentedReader*>(ds.release()));
    wassert(actual(wl.get()));
    return wl;
}

std::unique_ptr<dataset::SegmentedWriter> DatasetTest::makeLocalWriter(const ConfigFile* wcfg)
{
    auto ds = makeWriter(wcfg);
    wassert(actual(ds.get()));
    unique_ptr<dataset::SegmentedWriter> wl(dynamic_cast<dataset::SegmentedWriter*>(ds.release()));
    wassert(actual(wl.get()));
    return wl;
}

std::unique_ptr<dataset::SegmentedChecker> DatasetTest::makeLocalChecker(const ConfigFile* wcfg)
{
    auto ds = makeChecker(wcfg);
    wassert(actual(ds.get()));
    unique_ptr<dataset::SegmentedChecker> wl(dynamic_cast<dataset::SegmentedChecker*>(ds.release()));
    wassert(actual(wl.get()));
    return wl;
}

std::unique_ptr<dataset::ondisk2::Reader> DatasetTest::makeOndisk2Reader(const ConfigFile* wcfg)
{
    auto ds = makeReader(wcfg);
    wassert(actual(ds.get()));
    unique_ptr<dataset::ondisk2::Reader> wl(dynamic_cast<dataset::ondisk2::Reader*>(ds.release()));
    wassert(actual(wl.get()));
    return wl;
}

std::unique_ptr<dataset::ondisk2::Writer> DatasetTest::makeOndisk2Writer(const ConfigFile* wcfg)
{
    auto ds = makeWriter(wcfg);
    wassert(actual(ds.get()));
    unique_ptr<dataset::ondisk2::Writer> wl(dynamic_cast<dataset::ondisk2::Writer*>(ds.release()));
    wassert(actual(wl.get()));
    return wl;
}

std::unique_ptr<dataset::ondisk2::Checker> DatasetTest::makeOndisk2Checker(const ConfigFile* wcfg)
{
    auto ds = makeChecker(wcfg);
    wassert(actual(ds.get()));
    unique_ptr<dataset::ondisk2::Checker> wl(dynamic_cast<dataset::ondisk2::Checker*>(ds.release()));
    wassert(actual(wl.get()));
    return wl;
}

std::unique_ptr<dataset::simple::Reader> DatasetTest::makeSimpleReader(const ConfigFile* wcfg)
{
    auto ds = makeReader(wcfg);
    wassert(actual(ds.get()));
    unique_ptr<dataset::simple::Reader> wl(dynamic_cast<dataset::simple::Reader*>(ds.release()));
    wassert(actual(wl.get()));
    return wl;
}

std::unique_ptr<dataset::simple::Writer> DatasetTest::makeSimpleWriter(const ConfigFile* wcfg)
{
    auto ds = makeWriter(wcfg);
    wassert(actual(ds.get()));
    unique_ptr<dataset::simple::Writer> wl(dynamic_cast<dataset::simple::Writer*>(ds.release()));
    wassert(actual(wl.get()));
    return wl;
}

std::unique_ptr<dataset::simple::Checker> DatasetTest::makeSimpleChecker(const ConfigFile* wcfg)
{
    auto ds = makeChecker(wcfg);
    wassert(actual(ds.get()));
    unique_ptr<dataset::simple::Checker> wl(dynamic_cast<dataset::simple::Checker*>(ds.release()));
    wassert(actual(wl.get()));
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
        metadata::Collection data(testfile);
        for (auto& md: data)
        {
            Writer::AcquireResult res = writer->acquire(*md);
            ensure_equals(res, Writer::ACQ_OK);
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
    unique_ptr<dataset::SegmentedChecker> writer(makeLocalChecker(wcfg));
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
        unique_ptr<LocalChecker> writer(makeLocalChecker());
        NullReporter r;
        wassert(writer->repack(r, true));
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
        if (i != tests::DatasetTest::COUNTED_OK && counts[i])
            return false;
    return true;
}

void MaintenanceCollector::operator()(const std::string& file, dataset::segment::State state)
{
    using namespace arki::dataset;
    fileStates[file] = state;
    if (state.is_ok())                  ++counts[tests::DatasetTest::COUNTED_OK];
    if (state.has(SEGMENT_ARCHIVE_AGE)) ++counts[tests::DatasetTest::COUNTED_ARCHIVE_AGE];
    if (state.has(SEGMENT_DELETE_AGE))  ++counts[tests::DatasetTest::COUNTED_DELETE_AGE];
    if (state.has(SEGMENT_DIRTY))       ++counts[tests::DatasetTest::COUNTED_DIRTY];
    if (state.has(SEGMENT_NEW))         ++counts[tests::DatasetTest::COUNTED_NEW];
    if (state.has(SEGMENT_UNALIGNED))   ++counts[tests::DatasetTest::COUNTED_UNALIGNED];
    if (state.has(SEGMENT_DELETED))     ++counts[tests::DatasetTest::COUNTED_DELETED];
    if (state.has(SEGMENT_CORRUPTED))   ++counts[tests::DatasetTest::COUNTED_CORRUPTED];
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
    "archive age",
    "delete age",
    "dirty",
    "new",
    "unaligned",
    "deleted",
    "corrupted",
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

std::unique_ptr<dataset::LocalChecker> make_dataset_checker(const std::string& cfgstr)
{
    // Parse configuration
    stringstream incfg(cfgstr);
    ConfigFile cfg;
    cfg.parse(incfg, "(memory)");
    wassert(actual(cfg.value("path").empty()).isfalse());

    unique_ptr<dataset::LocalChecker> ds(dataset::LocalChecker::create(cfg));
    wassert(actual(ds.get()).istrue());
    return ds;
}

void test_append_transaction_ok(dataset::segment::Segment* dw, Metadata& md, int append_amount_adjust)
{
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

void test_append_transaction_rollback(dataset::segment::Segment* dw, Metadata& md)
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
void ActualSegmentedChecker::maintenance(const MaintenanceResults& expected, bool quick)
{
    MaintenanceCollector c;
    wassert(_actual->maintenance([&](const std::string& relpath, segment::State state) { c(relpath, state); }, quick));

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

void ActualSegmentedChecker::maintenance_clean(unsigned data_count, bool quick)
{
    MaintenanceResults expected(true, data_count);
    expected.by_type[tests::DatasetTest::COUNTED_OK] = data_count;
    maintenance(expected, quick);
}


ReporterExpected::OperationMatch::OperationMatch(const std::string& dsname, const std::string& operation, const std::string& message)
    : dsname(dsname), operation(operation), message(message)
{
}

std::string ReporterExpected::OperationMatch::error_unmatched(const std::string& type) const
{
    string msg = "expected operation not matched: " + dsname + ":" + type + " not found in " + operation + " output";
    if (!message.empty())
        msg += " (matching '" + message + "')";
    return msg;
}

ReporterExpected::SegmentMatch::SegmentMatch(const std::string& dsname, const std::string& relpath, const std::string& message)
    : dsname(dsname), relpath(relpath), message(message)
{
}

std::string ReporterExpected::SegmentMatch::error_unmatched(const std::string& operation) const
{
    string msg = "expected segment not matched: " + dsname + ":" + relpath + " not found in " + operation + " output";
    if (!message.empty())
        msg += " (matching '" + message + "')";
    return msg;
}

struct MainteanceCheckResult
{
    std::string type;
    bool matched = false;

    MainteanceCheckResult(const std::string& type) : type(type) {}
};

struct OperationResult : public MainteanceCheckResult
{
    std::string dsname;
    std::string operation;
    std::string message;

    OperationResult(const std::string& type, const std::string& dsname, const std::string& operation, const std::string& message=std::string())
        : MainteanceCheckResult(type), dsname(dsname), operation(operation), message(message) {}

    bool match(const ReporterExpected::OperationMatch& m) const
    {
        if (m.dsname != dsname) return false;
        if (m.operation != operation) return false;
        if (!m.message.empty() && message.find(m.message) == string::npos) return false;
        return true;
    }

    string error_unmatched() const
    {
        return "operation output not matched: " + type + " on " + dsname + ":" + operation + ": " + message;
    }
};

struct SegmentResult : public MainteanceCheckResult
{
    std::string dsname;
    std::string relpath;
    std::string message;

    SegmentResult(const std::string& operation, const std::string& dsname, const std::string& relpath, const std::string& message=std::string())
        : MainteanceCheckResult(operation), dsname(dsname), relpath(relpath), message(message) {}

    bool match(const ReporterExpected::SegmentMatch& m) const
    {
        if (m.dsname != dsname) return false;
        if (m.relpath != relpath) return false;
        if (!m.message.empty() && message.find(m.message) == string::npos) return false;
        return true;
    }

    string error_unmatched() const
    {
        return "segment output not matched: " + type + " on " + dsname + ":" + relpath + ": " + message;
    }
};

template<typename Matcher, typename Result>
struct MainteanceCheckResults : public std::vector<Result>
{
    void match(const std::string& type, const std::vector<Matcher>& matches, vector<string>& issues)
    {
        // Track which rules were not matched
        std::set<const Matcher*> unmatched;
        for (const auto& m: matches)
            unmatched.insert(&m);

        for (auto& r: *this)
        {
            if (r.type != type) continue;
            for (const auto& m: matches)
                if (r.match(m))
                {
                    r.matched = true;
                    unmatched.erase(&m);
                }
        }

        // Signal the unmatched rules for this operation
        for (const auto& m: unmatched)
            issues.emplace_back(m->error_unmatched(type));
    }

    void count_equals(const std::string& type, int expected, vector<string>& issues)
    {
        if (expected == -1) return;
        size_t count = 0;
        for (const auto& r: *this)
        {
            if (r.type != type) continue;
            ++count;
        }

        // Signal the counts that differ
        if (count != (unsigned)expected)
            issues.emplace_back(type + " had " + std::to_string(count) + " results but " + std::to_string(expected) + " were expected");
        else
        {
            // If the count is correct, mark all results for this operation as
            // matched, so that one can just do a test matching count of files
            // by operation type and not the details
            for (auto& r: *this)
                if (r.type == type)
                    r.matched = true;
        }
    }

    void report_unmatched(vector<string>& issues, const std::string& type_filter=std::string())
    {
        for (const auto& r: *this)
        {
            if (!type_filter.empty() && r.type != type_filter) continue;
            if (r.matched) continue;
            issues.emplace_back(r.error_unmatched());
        }
    }
};

struct CollectReporter : public dataset::Reporter
{
    typedef MainteanceCheckResults<ReporterExpected::OperationMatch, OperationResult> OperationResults;
    typedef MainteanceCheckResults<ReporterExpected::SegmentMatch, SegmentResult> SegmentResults;

    OperationResults op_results;
    SegmentResults seg_results;

    void operation_progress(const Base& ds, const std::string& operation, const std::string& message) override { op_results.emplace_back("progress", ds.name(), operation, message); }
    void operation_manual_intervention(const Base& ds, const std::string& operation, const std::string& message) override { op_results.emplace_back("manual_intervention", ds.name(), operation, message); }
    void operation_aborted(const Base& ds, const std::string& operation, const std::string& message) override { op_results.emplace_back("aborted", ds.name(), operation, message); }
    void operation_report(const Base& ds, const std::string& operation, const std::string& message) override { op_results.emplace_back("report", ds.name(), operation, message); }

    void segment_repack(const Base& ds, const std::string& relpath, const std::string& message) override { seg_results.emplace_back("repacked", ds.name(), relpath, message); }
    void segment_archive(const Base& ds, const std::string& relpath, const std::string& message) override { seg_results.emplace_back("archived", ds.name(), relpath, message); }
    void segment_delete(const Base& ds, const std::string& relpath, const std::string& message) override { seg_results.emplace_back("deleted", ds.name(), relpath, message); }
    void segment_deindex(const Base& ds, const std::string& relpath, const std::string& message) override { seg_results.emplace_back("deindexed", ds.name(), relpath, message); }
    void segment_rescan(const Base& ds, const std::string& relpath, const std::string& message) override { seg_results.emplace_back("rescanned", ds.name(), relpath, message); }

    void check(const ReporterExpected& expected)
    {
        vector<string> issues;

        op_results.match("progress", expected.progress, issues);
        op_results.match("manual_intervention", expected.manual_intervention, issues);
        op_results.match("aborted", expected.aborted, issues);
        op_results.match("report", expected.report, issues);

        op_results.report_unmatched(issues, "manual_intervention");
        op_results.report_unmatched(issues, "aborted");

        seg_results.match("repacked", expected.repacked, issues);
        seg_results.match("archived", expected.archived, issues);
        seg_results.match("deleted", expected.deleted, issues);
        seg_results.match("deindexed", expected.deindexed, issues);
        seg_results.match("rescanned", expected.rescanned, issues);

        seg_results.count_equals("repacked", expected.count_repacked, issues);
        seg_results.count_equals("archived", expected.count_archived, issues);
        seg_results.count_equals("deleted", expected.count_deleted, issues);
        seg_results.count_equals("deindexed", expected.count_deindexed, issues);
        seg_results.count_equals("rescanned", expected.count_rescanned, issues);

        seg_results.report_unmatched(issues);

        if (!issues.empty())
        {
            std::stringstream ss;
            ss << issues.size() << " mismatches in maintenance results:" << endl;
            for (const auto& m: issues)
                ss << "  " << m << endl;
            throw TestFailed(ss.str());
        }
    }
};

template<typename Dataset>
void ActualChecker<Dataset>::repack(const ReporterExpected& expected, bool write)
{
    CollectReporter reporter;
    wassert(this->_actual->repack(reporter, write));
    wassert(reporter.check(expected));
}

template<typename Dataset>
void ActualChecker<Dataset>::check(const ReporterExpected& expected, bool write, bool quick)
{
    CollectReporter reporter;
    wassert(this->_actual->check(reporter, write, quick));
    wassert(reporter.check(expected));
}

template<typename Dataset>
void ActualChecker<Dataset>::repack_clean(bool write)
{
    ReporterExpected e;
    repack(e, write);
}

template<typename Dataset>
void ActualChecker<Dataset>::check_clean(bool write, bool quick)
{
    ReporterExpected e;
    check(e, write, quick);
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

template class ActualChecker<Checker>;
template class ActualChecker<dataset::LocalChecker>;
template class ActualChecker<dataset::SegmentedChecker>;
}
