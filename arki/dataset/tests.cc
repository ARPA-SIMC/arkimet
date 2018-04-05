#include "tests.h"
#include "arki/metadata.h"
#include "arki/metadata/collection.h"
#include "arki/dataset/local.h"
#include "arki/dataset/ondisk2/reader.h"
#include "arki/dataset/ondisk2/writer.h"
#include "arki/dataset/ondisk2/checker.h"
#include "arki/dataset/simple/reader.h"
#include "arki/dataset/simple/writer.h"
#include "arki/dataset/simple/checker.h"
#include "arki/dataset/iseg/reader.h"
#include "arki/dataset/iseg/writer.h"
#include "arki/dataset/iseg/checker.h"
#include "arki/dataset/index/manifest.h"
#include "arki/dataset/reporter.h"
#include "arki/dispatcher.h"
#include "arki/scan/grib.h"
#include "arki/scan/vm2.h"
#include "arki/utils.h"
#include "arki/utils/files.h"
#include "arki/types/timerange.h"
#include "arki/types/area.h"
#include "arki/types/proddef.h"
#include "arki/types/source/blob.h"
#include "arki/utils/string.h"
#include "arki/utils/sys.h"
#include "arki/nag.h"
#include <algorithm>
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
using arki::core::Time;

namespace arki {
namespace tests {

int days_since(int year, int month, int day)
{
    // Data are from 07, 08, 10 2007
    Time threshold(year, month, day);
    Time now(Time::create_now());
    long long int duration = Time::duration(threshold, now);
    return duration/(3600*24);
}

DatasetTest::DatasetTest(const std::string& cfg_instance)
    : cfg_instance(cfg_instance), ds_name("testds"), ds_root(sys::abspath("testds"))
{
    //if (default_datasettest_config)
        //cfg = *default_datasettest_config;
}
DatasetTest::~DatasetTest()
{
    if (segment_manager) delete segment_manager;
}

void DatasetTest::test_setup(const std::string& cfg_default)
{
    cfg.clear();
    cfg.parse(cfg_default + "\n" + cfg_instance + "\n");
    cfg.setValue("path", ds_root);
    cfg.setValue("name", ds_name);
    if (sys::exists(ds_root))
        sys::rmtree(ds_root);
}

void DatasetTest::test_teardown()
{
    delete segment_manager;
    segment_manager = nullptr;
    m_config.reset();
}

void DatasetTest::test_reread_config()
{
    test_teardown();
    config();
}

void DatasetTest::reset_test(const std::string& cfg_default)
{
    test_teardown();
    test_setup(cfg_default);
}

const Config& DatasetTest::config()
{
    if (!m_config)
    {
        sys::mkdir_ifmissing(ds_root);
        sys::write_file(str::joinpath(ds_root, "config"), cfg.serialize());
        m_config = dataset::Config::create(cfg);
    }
    return *m_config;
}

std::shared_ptr<const dataset::Config> DatasetTest::dataset_config()
{
    config();
    return m_config;
}

std::shared_ptr<const dataset::LocalConfig> DatasetTest::local_config()
{
    config();
    return dynamic_pointer_cast<const dataset::LocalConfig>(m_config);
}

std::shared_ptr<const dataset::ondisk2::Config> DatasetTest::ondisk2_config()
{
    config();
    return dynamic_pointer_cast<const dataset::ondisk2::Config>(m_config);
}

dataset::segment::Manager& DatasetTest::segments()
{
    if (!segment_manager)
    {
        const dataset::segmented::Config* c = dynamic_cast<const dataset::segmented::Config*>(dataset_config().get());
        if (!c) throw std::runtime_error("DatasetTest::segments called on a non-segmented dataset");
        segment_manager = c->create_segment_manager().release();
    }
    return *segment_manager;
}

std::string DatasetTest::idxfname(const ConfigFile* wcfg) const
{
    if (!wcfg) wcfg = &cfg;
    if (wcfg->value("type") == "ondisk2")
        return "index.sqlite";
    else if (wcfg->value("index_type") == "sqlite")
        return "index.sqlite";
    else
        return dataset::index::Manifest::get_force_sqlite() ? "index.sqlite" : "MANIFEST";
}

std::string DatasetTest::destfile(const testdata::Element& el) const
{
    char buf[32];
    if (cfg.value("shard").empty())
        snprintf(buf, 32, "%04d/%02d-%02d.%s", el.time.ye, el.time.mo, el.time.da, el.md.source().format.c_str());
    else
        snprintf(buf, 32, "%04d/%02d/%02d.%s", el.time.ye, el.time.mo, el.time.da, el.md.source().format.c_str());
    return buf;
}

std::string DatasetTest::archive_destfile(const testdata::Element& el) const
{
    char buf[64];
    snprintf(buf, 64, ".archive/last/%04d/%02d-%02d.%s", el.time.ye, el.time.mo, el.time.da, el.md.source().format.c_str());
    return buf;
}

std::set<std::string> DatasetTest::destfiles(const testdata::Fixture& f) const
{
    std::set<std::string> fnames;
    for (unsigned i = 0; i < 3; ++i)
        fnames.insert(destfile(f.test_data[i]));
    return fnames;
}

unsigned DatasetTest::count_dataset_files(const testdata::Fixture& f) const
{
    return destfiles(f).size();
}

std::string manifest_idx_fname()
{
    return dataset::index::Manifest::get_force_sqlite() ? "index.sqlite" : "MANIFEST";
}

segmented::State DatasetTest::scan_state(bool quick)
{
    // OstreamReporter nr(cerr);
    NullReporter nr;
    auto checker = makeSegmentedChecker();
    return checker->scan(nr, quick);
}

segmented::State DatasetTest::scan_state(const Matcher& matcher, bool quick)
{
    // OstreamReporter nr(cerr);
    NullReporter nr;
    auto checker = makeSegmentedChecker();
    return checker->scan_filtered(matcher, nr, quick);
}

std::unique_ptr<dataset::segmented::Reader> DatasetTest::makeSegmentedReader()
{
    auto ds = config().create_reader();
    dataset::segmented::Reader* r = dynamic_cast<dataset::segmented::Reader*>(ds.get());
    if (!r) throw std::runtime_error("makeSegmentedReader called while testing a non-segmented dataset");
    ds.release();
    return unique_ptr<dataset::segmented::Reader>(r);
}

std::unique_ptr<dataset::segmented::Writer> DatasetTest::makeSegmentedWriter()
{
    auto ds = config().create_writer();
    dataset::segmented::Writer* r = dynamic_cast<dataset::segmented::Writer*>(ds.get());
    if (!r) throw std::runtime_error("makeSegmentedWriter called while testing a non-segmented dataset");
    ds.release();
    return unique_ptr<dataset::segmented::Writer>(r);
}

std::unique_ptr<dataset::segmented::Checker> DatasetTest::makeSegmentedChecker()
{
    auto ds = config().create_checker();
    dataset::segmented::Checker* r = dynamic_cast<dataset::segmented::Checker*>(ds.get());
    if (!r) throw std::runtime_error("makeSegmentedChecker called while testing a non-segmented dataset");
    ds.release();
    return unique_ptr<dataset::segmented::Checker>(r);
}

std::unique_ptr<dataset::ondisk2::Reader> DatasetTest::makeOndisk2Reader()
{
    auto ds = config().create_reader();
    dataset::ondisk2::Reader* r = dynamic_cast<dataset::ondisk2::Reader*>(ds.get());
    if (!r) throw std::runtime_error("makeOndisk2Reader called while testing a non-ondisk2 dataset");
    ds.release();
    return unique_ptr<dataset::ondisk2::Reader>(r);
}

std::unique_ptr<dataset::ondisk2::Writer> DatasetTest::makeOndisk2Writer()
{
    auto ds = config().create_writer();
    dataset::ondisk2::Writer* r = dynamic_cast<dataset::ondisk2::Writer*>(ds.get());
    if (!r) throw std::runtime_error("makeOndisk2Writer called while testing a non-ondisk2 dataset");
    ds.release();
    return unique_ptr<dataset::ondisk2::Writer>(r);
}

std::unique_ptr<dataset::ondisk2::Checker> DatasetTest::makeOndisk2Checker()
{
    auto ds = config().create_checker();
    dataset::ondisk2::Checker* r = dynamic_cast<dataset::ondisk2::Checker*>(ds.get());
    if (!r) throw std::runtime_error("makeOndisk2Checker called while testing a non-ondisk2 dataset");
    ds.release();
    return unique_ptr<dataset::ondisk2::Checker>(r);
}

std::unique_ptr<dataset::simple::Reader> DatasetTest::makeSimpleReader()
{
    auto ds = config().create_reader();
    dataset::simple::Reader* r = dynamic_cast<dataset::simple::Reader*>(ds.get());
    if (!r) throw std::runtime_error("makeSimpleReader called while testing a non-simple dataset");
    ds.release();
    return unique_ptr<dataset::simple::Reader>(r);
}

std::unique_ptr<dataset::simple::Writer> DatasetTest::makeSimpleWriter()
{
    auto ds = config().create_writer();
    dataset::simple::Writer* r = dynamic_cast<dataset::simple::Writer*>(ds.get());
    if (!r) throw std::runtime_error("makeSimpleWriter called while testing a non-simple dataset");
    ds.release();
    return unique_ptr<dataset::simple::Writer>(r);
}

std::unique_ptr<dataset::simple::Checker> DatasetTest::makeSimpleChecker()
{
    auto ds = config().create_checker();
    dataset::simple::Checker* r = dynamic_cast<dataset::simple::Checker*>(ds.get());
    if (!r) throw std::runtime_error("makeSimpleChecker called while testing a non-simple dataset");
    ds.release();
    return unique_ptr<dataset::simple::Checker>(r);
}

std::unique_ptr<dataset::iseg::Reader> DatasetTest::makeIsegReader()
{
    auto ds = config().create_reader();
    dataset::iseg::Reader* r = dynamic_cast<dataset::iseg::Reader*>(ds.get());
    if (!r) throw std::runtime_error("makeIsegReader called while testing a non-iseg dataset");
    ds.release();
    return unique_ptr<dataset::iseg::Reader>(r);
}

std::unique_ptr<dataset::iseg::Writer> DatasetTest::makeIsegWriter()
{
    auto ds = config().create_writer();
    dataset::iseg::Writer* r = dynamic_cast<dataset::iseg::Writer*>(ds.get());
    if (!r) throw std::runtime_error("makeIsegWriter called while testing a non-iseg dataset");
    ds.release();
    return unique_ptr<dataset::iseg::Writer>(r);
}

std::unique_ptr<dataset::iseg::Checker> DatasetTest::makeIsegChecker()
{
    auto ds = config().create_checker();
    dataset::iseg::Checker* r = dynamic_cast<dataset::iseg::Checker*>(ds.get());
    if (!r) throw std::runtime_error("makeIsegChecker called while testing a non-iseg dataset");
    ds.release();
    return unique_ptr<dataset::iseg::Checker>(r);
}

void DatasetTest::clean()
{
    if (sys::exists(ds_root)) sys::rmtree(ds_root);
    sys::mkdir_ifmissing(ds_root);
    sys::write_file(str::joinpath(ds_root, "config"), cfg.serialize());
    import_results.clear();
}

void DatasetTest::import(const std::string& testfile)
{
    {
        std::unique_ptr<Writer> writer(config().create_writer());
        metadata::TestCollection data(testfile);
        auto batch = data.make_import_batch();
        writer->acquire_batch(batch);
        for (const auto& e: batch)
        {
            wassert(actual(e->dataset_name) == config().name);
            wassert(actual(e->result) == ACQ_OK);
            import_results.push_back(e->md);
            import_results.back().sourceBlob().unlock();
        }
    }

    utils::files::removeDontpackFlagfile(ds_root);
}

void DatasetTest::import(Metadata& md, dataset::WriterAcquireResult expected_result)
{
    import_results.push_back(md);
    std::unique_ptr<Writer> writer(config().create_writer());
    WriterAcquireResult res = writer->acquire(import_results.back());
    wassert(actual(res) == expected_result);
}

void DatasetTest::clean_and_import(const std::string& testfile)
{
    clean();
    import(testfile);
}

metadata::Collection DatasetTest::query(const dataset::DataQuery& q)
{
    return metadata::Collection(*config().create_reader(), q);
}

void DatasetTest::ensure_localds_clean(size_t filecount, size_t resultcount, bool quick)
{
    nag::TestCollect tc;
    auto state = scan_state(quick);
    wassert(actual(state.count(SEGMENT_OK)) == filecount);
    wassert(actual(state.size()) == filecount);

    auto reader = makeSegmentedReader();
    metadata::Collection mdc(*reader, Matcher());
    wassert(actual(mdc.size()) == resultcount);

    if (filecount > 0 && reader->type() != "iseg")
        wassert(actual_file(str::joinpath(reader->path(), idxfname())).exists());
    tc.clear();
}

void DatasetTest::all_clean(size_t segment_count)
{
    auto state = scan_state();
    try {
        wassert(actual(state.size()) == segment_count);
        wassert(actual(state.count(SEGMENT_OK)) == segment_count);
    } catch (...) {
        fprintf(stderr, "Dump of unexpected state:\n");
        state.dump(stderr);
        throw;
    }
}

void DatasetTest::import_all(const testdata::Fixture& fixture)
{
    clean();

    auto writer = config().create_writer();
    for (int i = 0; i < 3; ++i)
    {
        import_results.push_back(fixture.test_data[i].md);
        WriterAcquireResult res = writer->acquire(import_results.back());
        wassert(actual(res) == ACQ_OK);
        import_results.back().sourceBlob().unlock();
    }

    utils::files::removeDontpackFlagfile(cfg.value("path"));
}

void DatasetTest::import_all_packed(const testdata::Fixture& fixture)
{
    wassert(import_all(fixture));
    wassert(repack());
}

void DatasetTest::repack()
{
    // Pack the dataset in case something imported data out of order
    auto checker = config().create_checker();
    NullReporter r;
    wassert(checker->repack(r, true));
}

void DatasetTest::query_results(const std::vector<int>& expected)
{
    query_results(Matcher(), expected);
}

void DatasetTest::query_results(const dataset::DataQuery& q, const std::vector<int>& expected)
{
    vector<int> found;
    config().create_reader()->query_data(q, [&](unique_ptr<Metadata>&& md) {
        unsigned idx;
        for (idx = 0; idx < import_results.size(); ++idx)
            if (import_results[idx].compare_items(*md) == 0)
                break;
        if (idx == import_results.size())
            found.push_back(-1);
        else
            found.push_back(idx);
        return true;
    });

    wassert(actual(str::join(", ", found)) == str::join(", ", expected));
}

void DatasetTest::make_unaligned(const std::string& segment)
{
    auto checker = makeSegmentedChecker();
    if (checker->type() == "iseg")
    {
        sys::unlink(str::joinpath(local_config()->path, segment + ".index"));
    } else if (checker->type() == "ondisk2") {
        checker->test_remove_index(segment);
        files::createDontpackFlagfile(local_config()->path);
    } else if (checker->type() == "simple") {
        // Set the metadata to be older than the data
        sys::touch(str::joinpath(local_config()->path, segment + ".metadata"), 1496167200);
    } else {
        throw std::runtime_error("make_unaligned called on unsupported dataset type " + checker->type());
    }
}

}

namespace tests {

void test_append_transaction_ok(dataset::segment::Writer* dw, Metadata& md, int append_amount_adjust)
{
    // Make a snapshot of everything before appending
    unique_ptr<Source> orig_source(md.source().clone());
    size_t data_size = md.data_size();
    size_t orig_fsize = sys::size(dw->absname, 0);

    // Start the append transaction, nothing happens until commit
    const types::source::Blob& new_source = dw->append(md);
    wassert(actual((size_t)new_source.offset) == orig_fsize);
    wassert(actual((size_t)new_source.size) == data_size);
    wassert(actual(new_source.basedir) == sys::getcwd());
    wassert(actual(new_source.filename) == dw->relname);
    wassert(actual(sys::size(dw->absname)) == orig_fsize + data_size + append_amount_adjust);
    wassert(actual_type(md.source()) == *orig_source);

    // Commit
    dw->commit();

    // After commit, data is appended
    wassert(actual(sys::size(dw->absname)) == orig_fsize + data_size + append_amount_adjust);

    // And metadata is updated
    wassert(actual_type(md.source()).is_source_blob("grib", sys::getcwd(), dw->relname, orig_fsize, data_size));
}

void test_append_transaction_rollback(dataset::segment::Writer* dw, Metadata& md, int append_amount_adjust)
{
    // Make a snapshot of everything before appending
    unique_ptr<Source> orig_source(md.source().clone());
    size_t orig_fsize = sys::size(dw->absname, 0);

    // Start the append transaction, nothing happens until commit
    const types::source::Blob& new_source = dw->append(md);
    wassert(actual((size_t)new_source.offset) == orig_fsize);
    wassert(actual(sys::size(dw->absname, 0)) == orig_fsize + new_source.size + append_amount_adjust);
    wassert(actual_type(md.source()) == *orig_source);

    // Rollback
    dw->rollback();

    // After rollback, nothing has changed
    wassert(actual(sys::size(dw->absname, 0)) == orig_fsize);
    wassert(actual_type(md.source()) == *orig_source);
}


ReporterExpected::OperationMatch::OperationMatch(const std::string& dsname, const std::string& operation, const std::string& message)
    : name(dsname), operation(operation), message(message)
{
}

std::string ReporterExpected::OperationMatch::error_unmatched(const std::string& type) const
{
    string msg = "expected operation not matched: " + name + ":" + type + " not found in " + operation + " output";
    if (!message.empty())
        msg += " (matching '" + message + "')";
    return msg;
}

ReporterExpected::ReporterExpected(unsigned flags)
    : flags(flags)
{
}


ReporterExpected::SegmentMatch::SegmentMatch(const std::string& dsname, const std::string& relpath, const std::string& message)
    : name(dsname + ":" + relpath), message(message)
{
}

std::string ReporterExpected::SegmentMatch::error_unmatched(const std::string& operation) const
{
    string msg = "expected segment not matched: " + name + " not found in " + operation + " output";
    if (!message.empty())
        msg += " (matching '" + message + "')";
    return msg;
}

void ReporterExpected::clear()
{
    progress.clear();
    manual_intervention.clear();
    aborted.clear();
    report.clear();

    repacked.clear();
    archived.clear();
    deleted.clear();
    deindexed.clear();
    rescanned.clear();
    tarred.clear();
    issue51.clear();

    count_repacked = -1;
    count_archived = -1;
    count_deleted = -1;
    count_deindexed = -1;
    count_rescanned = -1;
    count_tarred = -1;
    count_issue51 = -1;
}


struct MaintenanceCheckResult
{
    std::string type;
    // dataset or dataset:segment
    std::string name;
    bool matched = false;

    MaintenanceCheckResult(const std::string& type, const std::string& name) : type(type), name(name) {}
};

struct OperationResult : public MaintenanceCheckResult
{
    std::string operation;
    std::string message;

    OperationResult(const std::string& type, const std::string& dsname, const std::string& operation, const std::string& message=std::string())
        : MaintenanceCheckResult(type, dsname), operation(operation), message(message) {}

    bool match(const ReporterExpected::OperationMatch& m) const
    {
        if (m.name != name) return false;
        if (m.operation != operation) return false;
        if (!m.message.empty() && message.find(m.message) == string::npos) return false;
        return true;
    }

    string error_unmatched() const
    {
        return "operation output not matched: " + type + " on " + name + ":" + operation + ": " + message;
    }

    void dump(FILE* out) const
    {
        fprintf(out, "type %s, name %s, op %s, msg %s, matched %s\n",
                type.c_str(), name.c_str(), operation.c_str(), message.c_str(), matched ? "true" : "false");
    }
};

struct SegmentResult : public MaintenanceCheckResult
{
    std::string message;

    SegmentResult(const std::string& operation, const std::string& dsname, const std::string& relpath, const std::string& message=std::string())
        : MaintenanceCheckResult(operation, dsname + ":" + relpath), message(message) {}

    bool match(const ReporterExpected::SegmentMatch& m) const
    {
        if (m.name != name) return false;
        if (!m.message.empty() && message.find(m.message) == string::npos) return false;
        return true;
    }

    string error_unmatched() const
    {
        return "segment output not matched: " + type + " on " + name + ": " + message;
    }

    void dump(FILE* out) const
    {
        fprintf(out, "type %s, name %s, msg %s, matched %s\n",
                type.c_str(), name.c_str(), message.c_str(), matched ? "true" : "false");
    }
};

template<typename Matcher, typename Result>
struct MaintenanceCheckResults : public std::vector<Result>
{
    std::map<std::string, std::vector<std::string>> extra_info;

    void store_extra_info(const std::string& name, const std::string& message)
    {
        extra_info[name].emplace_back(message);
    }

    void report_extra_info(const std::string& name, vector<string>& issues)
    {
        auto i = extra_info.find(name);
        if (i == extra_info.end()) return;
        for (const auto& msg: i->second)
            issues.emplace_back(name + " extra info: " + msg);
    }

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
        {
            issues.emplace_back(m->error_unmatched(type));
            report_extra_info(m->name, issues);
        }
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
            report_extra_info(r.name, issues);
        }
    }

    void dump(FILE* out) const
    {
        for (const auto& r: *this)
        {
            r.dump(out);
            auto i = extra_info.find(r.name);
            if (i == extra_info.end()) continue;
            for (const auto& line: i->second)
                fprintf(out, " extra info: %s\n", line.c_str());
        }
    }
};

struct CollectReporter : public dataset::Reporter
{
    typedef MaintenanceCheckResults<ReporterExpected::OperationMatch, OperationResult> OperationResults;
    typedef MaintenanceCheckResults<ReporterExpected::SegmentMatch, SegmentResult> SegmentResults;

    std::stringstream report;
    OstreamReporter recorder;
    OperationResults op_results;
    SegmentResults seg_results;

    CollectReporter() : recorder(report) {}

    void operation_progress(const std::string& ds, const std::string& operation, const std::string& message) override
    {
        recorder.operation_progress(ds, operation, message);
        op_results.emplace_back("progress", ds, operation, message);
    }
    void operation_manual_intervention(const std::string& ds, const std::string& operation, const std::string& message) override
    {
        recorder.operation_manual_intervention(ds, operation, message);
        op_results.emplace_back("manual_intervention", ds, operation, message);
    }
    void operation_aborted(const std::string& ds, const std::string& operation, const std::string& message) override
    {
        recorder.operation_aborted(ds, operation, message);
        op_results.emplace_back("aborted", ds, operation, message);
    }
    void operation_report(const std::string& ds, const std::string& operation, const std::string& message) override
    {
        recorder.operation_report(ds, operation, message);
        op_results.emplace_back("report", ds, operation, message);
    }

    void segment_info(const std::string& ds, const std::string& relpath, const std::string& message) override
    {
        recorder.segment_info(ds, relpath, message);
        seg_results.store_extra_info(ds + ":" + relpath, message);
    }
    void segment_repack(const std::string& ds, const std::string& relpath, const std::string& message) override
    {
        recorder.segment_repack(ds, relpath, message);
        seg_results.emplace_back("repacked", ds, relpath, message);
    }
    void segment_archive(const std::string& ds, const std::string& relpath, const std::string& message) override
    {
        recorder.segment_archive(ds, relpath, message);
        seg_results.emplace_back("archived", ds, relpath, message);
    }
    void segment_delete(const std::string& ds, const std::string& relpath, const std::string& message) override
    {
        recorder.segment_delete(ds, relpath, message);
        seg_results.emplace_back("deleted", ds, relpath, message);
    }
    void segment_deindex(const std::string& ds, const std::string& relpath, const std::string& message) override
    {
        recorder.segment_deindex(ds, relpath, message);
        seg_results.emplace_back("deindexed", ds, relpath, message);
    }
    void segment_rescan(const std::string& ds, const std::string& relpath, const std::string& message) override
    {
        recorder.segment_rescan(ds, relpath, message);
        seg_results.emplace_back("rescanned", ds, relpath, message);
    }
    void segment_tar(const std::string& ds, const std::string& relpath, const std::string& message) override
    {
        recorder.segment_tar(ds, relpath, message);
        seg_results.emplace_back("tarred", ds, relpath, message);
    }
    void segment_issue51(const std::string& ds, const std::string& relpath, const std::string& message) override
    {
        recorder.segment_issue51(ds, relpath, message);
        seg_results.emplace_back("issue51", ds, relpath, message);
    }

    void check(const ReporterExpected& expected)
    {
        vector<string> issues;

        op_results.match("progress", expected.progress, issues);
        op_results.match("manual_intervention", expected.manual_intervention, issues);
        op_results.match("aborted", expected.aborted, issues);
        op_results.match("report", expected.report, issues);

        op_results.report_unmatched(issues, "manual_intervention");
        op_results.report_unmatched(issues, "aborted");
        if (expected.flags & ReporterExpected::ENFORCE_REPORTS)
            op_results.report_unmatched(issues, "report");

        seg_results.match("repacked", expected.repacked, issues);
        seg_results.match("archived", expected.archived, issues);
        seg_results.match("deleted", expected.deleted, issues);
        seg_results.match("deindexed", expected.deindexed, issues);
        seg_results.match("rescanned", expected.rescanned, issues);
        seg_results.match("tarred", expected.tarred, issues);
        seg_results.match("issue51", expected.issue51, issues);

        seg_results.count_equals("repacked", expected.count_repacked, issues);
        seg_results.count_equals("archived", expected.count_archived, issues);
        seg_results.count_equals("deleted", expected.count_deleted, issues);
        seg_results.count_equals("deindexed", expected.count_deindexed, issues);
        seg_results.count_equals("rescanned", expected.count_rescanned, issues);
        seg_results.count_equals("tarred", expected.count_tarred, issues);
        seg_results.count_equals("issue51", expected.count_issue51, issues);

        seg_results.report_unmatched(issues);

        if (!issues.empty())
        {
            std::stringstream ss;
            ss << issues.size() << " mismatches in maintenance results:" << endl;
            for (const auto& m: issues)
                ss << "  " << m << endl;
            ss << "Reporter output:" << endl;
            ss << report.str();
            throw TestFailed(ss.str());
        }
    }

    void dump(FILE* out) const
    {
        fprintf(out, "Operation:\n");
        op_results.dump(out);
        fprintf(out, "Segment:\n");
        seg_results.dump(out);
    }
};


template<typename Dataset>
void ActualWriter<Dataset>::import(Metadata& md)
{
    auto res = wcallchecked(this->_actual->acquire(md));
    if (res != dataset::ACQ_OK)
    {
        std::stringstream ss;
        switch (res)
        {
            case ACQ_ERROR_DUPLICATE:
                ss << "ACQ_ERROR_DUPLICATE when importing data. notes:" << endl;
                break;
            case ACQ_ERROR:
                ss << "ACQ_ERROR when importing data. notes:" << endl;
                break;
            default:
                ss << "Error " << (int)res << " when importing data. notes:" << endl;
                break;
        }

        for (const auto& note: md.notes())
            ss << "\t" << note << endl;

        throw TestFailed(ss.str());
    }
}

template<typename Dataset>
void ActualWriter<Dataset>::import(metadata::Collection& mds, dataset::Writer::ReplaceStrategy strategy)
{
    WriterBatch batch;
    batch.reserve(mds.size());
    for (auto& md: mds)
        batch.emplace_back(make_shared<WriterBatchElement>(*md));
    wassert(this->_actual->acquire_batch(batch, strategy));

    std::stringstream ss;
    for (const auto& e: batch)
    {
        if (e->result == dataset::ACQ_OK) continue;
        switch (e->result)
        {
            case ACQ_ERROR_DUPLICATE:
                ss << "ACQ_ERROR_DUPLICATE when importing data. notes:" << endl;
                break;
            case ACQ_ERROR:
                ss << "ACQ_ERROR when importing data. notes:" << endl;
                break;
            default:
                ss << "Error " << (int)e->result << " when importing data. notes:" << endl;
                break;
        }

        for (const auto& note: e->md.notes())
            ss << "\t" << note << endl;
    }
    if (!ss.str().empty())
        throw TestFailed(ss.str());
}


template<typename Dataset>
void ActualChecker<Dataset>::repack(const ReporterExpected& expected, bool write)
{
    CollectReporter reporter;
    wassert(this->_actual->repack(reporter, write));
    // reporter.dump(stderr);
    wassert(reporter.check(expected));
}

template<typename Dataset>
void ActualChecker<Dataset>::repack_filtered(const Matcher& matcher, const ReporterExpected& expected, bool write)
{
    CollectReporter reporter;
    wassert(this->_actual->repack_filtered(matcher, reporter, write));
    // reporter.dump(stderr);
    wassert(reporter.check(expected));
}

template<typename Dataset>
void ActualChecker<Dataset>::check(const ReporterExpected& expected, bool write, bool quick)
{
    CollectReporter reporter;
    wassert(this->_actual->check(reporter, write, quick));
    // reporter.dump(stderr);
    wassert(reporter.check(expected));
}

template<typename Dataset>
void ActualChecker<Dataset>::check_filtered(const Matcher& matcher, const ReporterExpected& expected, bool write, bool quick)
{
    CollectReporter reporter;
    wassert(this->_actual->check_filtered(matcher, reporter, write, quick));
    //reporter.dump(stderr);
    wassert(reporter.check(expected));
}

template<typename Dataset>
void ActualChecker<Dataset>::check_issue51(const ReporterExpected& expected, bool write)
{
    CollectReporter reporter;
    dataset::CheckerConfig opts(reporter, !write);
    wassert(this->_actual->check_issue51(opts));
    // reporter.dump(stderr);
    wassert(reporter.check(expected));
}

template<typename Dataset>
void ActualChecker<Dataset>::repack_clean(bool write)
{
    ReporterExpected e;
    repack(e, write);
}

template<typename Dataset>
void ActualChecker<Dataset>::repack_filtered_clean(const Matcher& matcher, bool write)
{
    ReporterExpected e;
    repack_filtered(matcher, e, write);
}

template<typename Dataset>
void ActualChecker<Dataset>::check_clean(bool write, bool quick)
{
    ReporterExpected e;
    check(e, write, quick);
}

template<typename Dataset>
void ActualChecker<Dataset>::check_filtered_clean(const Matcher& matcher, bool write, bool quick)
{
    ReporterExpected e;
    check_filtered(matcher, e, write, quick);
}

template<typename Dataset>
void ActualChecker<Dataset>::check_issue51_clean(bool write)
{
    ReporterExpected e;
    check_issue51(e, write);
}

template<typename Dataset>
void ActualChecker<Dataset>::remove_all(const ReporterExpected& expected, bool write)
{
    CollectReporter reporter;
    dataset::CheckerConfig opts(reporter, !write);
    wassert(this->_actual->remove_all(opts));
    // reporter.dump(stderr);
    wassert(reporter.check(expected));
}

template<typename Dataset>
void ActualChecker<Dataset>::remove_all_filtered(const Matcher& matcher, const ReporterExpected& expected, bool write)
{
    CollectReporter reporter;
    dataset::CheckerConfig opts(reporter, !write);
    opts.segment_filter = matcher;
    wassert(this->_actual->remove_all(opts));
    // reporter.dump(stderr);
    wassert(reporter.check(expected));
}

}

namespace testdata {

void Element::set(const Metadata& md, const std::string& matcher)
{
    const types::reftime::Position* rt = md.get<types::reftime::Position>();
    this->md = md;
    this->time = rt->time;
    this->matcher = Matcher::parse(matcher);
}

std::string Element::data()
{
    auto res = md.getData();
    return std::string(res.begin(), res.end());
}

void Fixture::finalise_init()
{
    // Compute selective_cutoff
    selective_cutoff = min({test_data[0].time, test_data[1].time, test_data[2].time});
    ++selective_cutoff.mo;
    selective_cutoff.normalise();
}

unsigned Fixture::selective_days_since() const
{
    return tests::days_since(selective_cutoff.ye, selective_cutoff.mo, selective_cutoff.da);
}

Element& Fixture::earliest_element()
{
    Element* res = nullptr;

    for (auto& e: test_data)
        if (!res || *e.md.get(TYPE_REFTIME) < *res->md.get(TYPE_REFTIME))
            res = &e;

    return *res;
}

GRIBData::GRIBData()
{
#ifndef HAVE_GRIBAPI
    throw TestSkipped();
#endif
    metadata::TestCollection mdc("inbound/fixture.grib1");
    format = "grib";
    test_data[0].set(mdc[0], "reftime:=2007-07-08");
    test_data[1].set(mdc[1], "reftime:=2007-07-07");
    test_data[2].set(mdc[2], "reftime:=2007-10-09");
    finalise_init();
}

BUFRData::BUFRData()
{
#ifndef HAVE_DBALLE
    throw TestSkipped();
#endif
    metadata::TestCollection mdc("inbound/fixture.bufr");
    format = "bufr";
    test_data[0].set(mdc[0], "reftime:=2007-07-08");
    test_data[1].set(mdc[1], "reftime:=2007-07-07");
    test_data[2].set(mdc[2], "reftime:=2007-10-09");
    finalise_init();
}

VM2Data::VM2Data()
{
#ifndef HAVE_VM2
    throw TestSkipped();
#endif
    metadata::TestCollection mdc("inbound/fixture.vm2");
    format = "vm2";
    test_data[0].set(mdc[0], "reftime:=2007-07-08");
    test_data[1].set(mdc[1], "reftime:=2007-07-07");
    test_data[2].set(mdc[2], "reftime:=2007-10-09");
    finalise_init();
}

ODIMData::ODIMData()
{
#ifndef HAVE_HDF5
    throw TestSkipped();
#endif
    metadata::TestCollection mdc;
    format = "odimh5";
    mdc.scan_from_file("inbound/fixture.h5/00.h5", true);
    mdc.scan_from_file("inbound/fixture.h5/01.h5", true);
    mdc.scan_from_file("inbound/fixture.h5/02.h5", true);
    test_data[0].set(mdc[0], "reftime:=2007-07-08");
    test_data[1].set(mdc[1], "reftime:=2007-07-07");
    test_data[2].set(mdc[2], "reftime:=2007-10-09");
    finalise_init();
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

template class ActualWriter<dataset::Writer>;
template class ActualWriter<dataset::LocalWriter>;
template class ActualWriter<dataset::segmented::Writer>;
template class ActualChecker<dataset::Checker>;
template class ActualChecker<dataset::LocalChecker>;
template class ActualChecker<dataset::segmented::Checker>;
}
