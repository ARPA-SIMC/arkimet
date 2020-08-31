#include "tests.h"
#include "arki/segment/tests.h"
#include "arki/metadata.h"
#include "arki/metadata/collection.h"
#include "arki/dataset/query.h"
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
#include "arki/dataset/session.h"
#include "arki/scan/grib.h"
#include "arki/scan/vm2.h"
#include "arki/utils/files.h"
#include "arki/types/timerange.h"
#include "arki/types/area.h"
#include "arki/types/proddef.h"
#include "arki/types/source/blob.h"
#include "arki/utils/string.h"
#include "arki/utils/sys.h"
#include "arki/segment/dir.h"
#include "arki/nag.h"
#include <algorithm>
#include <cstring>
#include <limits.h>
#include <sys/time.h>
#include <sys/types.h>
#include <sys/stat.h>

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


bool State::has(const std::string& relpath) const
{
    return find(relpath) != end();
}

const segmented::SegmentState& State::get(const std::string& seg) const
{
    auto res = find(seg);
    if (res == end())
        throw std::runtime_error("segment " + seg + " not found in state");
    return res->second;
}

unsigned State::count(segment::State state) const
{
    unsigned res = 0;
    for (const auto& i: *this)
        if (i.second.state == state)
            ++res;
    return res;
}

void State::dump(FILE* out) const
{
    for (const auto& i: *this)
        fprintf(out, "%s: %s %s to %s\n", i.first.c_str(), i.second.state.to_string().c_str(), i.second.begin.to_iso8601(' ').c_str(), i.second.until.to_iso8601(' ').c_str());
}


DatasetTest::DatasetTest(const std::string& cfg_instance, TestVariant variant)
    : variant(variant), cfg(std::make_shared<core::cfg::Section>()), cfg_instance(cfg_instance), ds_name("testds"), ds_root(sys::abspath("testds"))
{
    //if (default_datasettest_config)
        //cfg = *default_datasettest_config;
}
DatasetTest::~DatasetTest()
{
}

void DatasetTest::test_setup(const std::string& cfg_default)
{
    std::string cfg_all = cfg_instance + "\n" + cfg_default + "\n";
    cfg = core::cfg::Section::parse(cfg_all);
    cfg->set("path", ds_root);
    cfg->set("name", ds_name);
    if (sys::exists(ds_root))
        sys::rmtree(ds_root);
}

void DatasetTest::test_teardown()
{
    m_dataset.reset();
    m_session.reset();
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

void DatasetTest::set_session(std::shared_ptr<dataset::Session> session)
{
    test_teardown();
    m_session = session;
    config();
}

std::shared_ptr<dataset::Session> DatasetTest::session()
{
    if (!m_session.get())
    {
        switch (variant)
        {
            case TEST_NORMAL:
                m_session = std::make_shared<dataset::Session>();
                break;
            case TEST_FORCE_DIR:
                m_session = std::make_shared<DirSegmentsSession>();
                break;
        }
    }
    return m_session;
}

Dataset& DatasetTest::config()
{
    if (!m_dataset)
    {
        sys::mkdir_ifmissing(ds_root);
        sys::File out(str::joinpath(ds_root, "config"), O_WRONLY | O_CREAT | O_TRUNC, 0666);
        cfg->write(out);
        m_dataset = session()->dataset(*cfg);
    }
    return *m_dataset;
}

std::shared_ptr<dataset::Dataset> DatasetTest::dataset_config()
{
    config();
    return m_dataset;
}

std::shared_ptr<dataset::local::Dataset> DatasetTest::local_config()
{
    config();
    return dynamic_pointer_cast<dataset::local::Dataset>(m_dataset);
}

std::shared_ptr<dataset::ondisk2::Dataset> DatasetTest::ondisk2_config()
{
    config();
    return dynamic_pointer_cast<dataset::ondisk2::Dataset>(m_dataset);
}

std::string DatasetTest::idxfname(const core::cfg::Section* wcfg) const
{
    if (!wcfg) wcfg = cfg.get();
    if (wcfg->value("type") == "ondisk2")
        return "index.sqlite";
    else if (wcfg->value("index_type") == "sqlite")
        return "index.sqlite";
    else
        return dataset::index::Manifest::get_force_sqlite() ? "index.sqlite" : "MANIFEST";
}

std::string DatasetTest::destfile(const Metadata& md) const
{
    const auto* rt = md.get<types::reftime::Position>();
    char buf[32];
    if (cfg->value("shard").empty())
        snprintf(buf, 32, "%04d/%02d-%02d.%s", rt->time.ye, rt->time.mo, rt->time.da, md.source().format.c_str());
    else
        snprintf(buf, 32, "%04d/%02d/%02d.%s", rt->time.ye, rt->time.mo, rt->time.da, md.source().format.c_str());
    return buf;
}

std::string DatasetTest::archive_destfile(const Metadata& md) const
{
    const auto* rt = md.get<types::reftime::Position>();
    char buf[64];
    snprintf(buf, 64, ".archive/last/%04d/%02d-%02d.%s", rt->time.ye, rt->time.mo, rt->time.da, md.source().format.c_str());
    return buf;
}

std::set<std::string> DatasetTest::destfiles(const metadata::Collection& mds) const
{
    std::set<std::string> fnames;
    for (const auto& md: mds)
        fnames.insert(destfile(*md));
    return fnames;
}

unsigned DatasetTest::count_dataset_files(const metadata::Collection& mds) const
{
    return destfiles(mds).size();
}

std::string manifest_idx_fname()
{
    return dataset::index::Manifest::get_force_sqlite() ? "index.sqlite" : "MANIFEST";
}

State DatasetTest::scan_state(bool quick)
{
    CheckerConfig opts;
    //opts.reporter = make_shared<OstreamReporter>(cerr);
    auto checker = makeSegmentedChecker();
    State res;
    checker->segments_recursive(opts, [&](segmented::Checker& checker, segmented::CheckerSegment& segment) {
        res.insert(make_pair(checker.name() + ":" + segment.path_relative(), segment.scan(*opts.reporter, quick)));
    });
    return res;
}

State DatasetTest::scan_state(const Matcher& matcher, bool quick)
{
    CheckerConfig opts;
    opts.segment_filter = matcher;
    auto checker = makeSegmentedChecker();
    State res;
    checker->segments(opts, [&](segmented::CheckerSegment& segment) { res.insert(make_pair(segment.path_relative(), segment.scan(*opts.reporter, quick))); });
    return res;
}

std::shared_ptr<dataset::segmented::Reader> DatasetTest::makeSegmentedReader()
{
    auto r = dynamic_pointer_cast<dataset::segmented::Reader>(config().create_reader());
    if (!r) throw std::runtime_error("makeSegmentedReader called while testing a non-segmented dataset");
    return r;
}

std::shared_ptr<dataset::segmented::Writer> DatasetTest::makeSegmentedWriter()
{
    auto r = dynamic_pointer_cast<dataset::segmented::Writer>(config().create_writer());
    if (!r) throw std::runtime_error("makeSegmentedWriter called while testing a non-segmented dataset");
    return r;
}

std::shared_ptr<dataset::segmented::Checker> DatasetTest::makeSegmentedChecker()
{
    auto r = dynamic_pointer_cast<dataset::segmented::Checker>(config().create_checker());
    if (!r) throw std::runtime_error("makeSegmentedChecker called while testing a non-segmented dataset");
    return r;
}

std::shared_ptr<dataset::ondisk2::Reader> DatasetTest::makeOndisk2Reader()
{
    auto r = dynamic_pointer_cast<dataset::ondisk2::Reader>(config().create_reader());
    if (!r) throw std::runtime_error("makeOndisk2Reader called while testing a non-ondisk2 dataset");
    return r;
}

std::shared_ptr<dataset::ondisk2::Writer> DatasetTest::makeOndisk2Writer()
{
    auto r = dynamic_pointer_cast<dataset::ondisk2::Writer>(config().create_writer());
    if (!r) throw std::runtime_error("makeOndisk2Writer called while testing a non-ondisk2 dataset");
    return r;
}

std::shared_ptr<dataset::ondisk2::Checker> DatasetTest::makeOndisk2Checker()
{
    auto r = dynamic_pointer_cast<dataset::ondisk2::Checker>(config().create_checker());
    if (!r) throw std::runtime_error("makeOndisk2Checker called while testing a non-ondisk2 dataset");
    return r;
}

std::shared_ptr<dataset::simple::Reader> DatasetTest::makeSimpleReader()
{
    auto r = dynamic_pointer_cast<dataset::simple::Reader>(config().create_reader());
    if (!r) throw std::runtime_error("makeSimpleReader called while testing a non-simple dataset");
    return r;
}

std::shared_ptr<dataset::simple::Writer> DatasetTest::makeSimpleWriter()
{
    auto r = dynamic_pointer_cast<dataset::simple::Writer>(config().create_writer());
    if (!r) throw std::runtime_error("makeSimpleWriter called while testing a non-simple dataset");
    return r;
}

std::shared_ptr<dataset::simple::Checker> DatasetTest::makeSimpleChecker()
{
    auto r = dynamic_pointer_cast<dataset::simple::Checker>(config().create_checker());
    if (!r) throw std::runtime_error("makeSimpleChecker called while testing a non-simple dataset");
    return r;
}

std::shared_ptr<dataset::iseg::Reader> DatasetTest::makeIsegReader()
{
    auto r = dynamic_pointer_cast<dataset::iseg::Reader>(config().create_reader());
    if (!r) throw std::runtime_error("makeIsegReader called while testing a non-iseg dataset");
    return r;
}

std::shared_ptr<dataset::iseg::Writer> DatasetTest::makeIsegWriter()
{
    auto r = dynamic_pointer_cast<dataset::iseg::Writer>(config().create_writer());
    if (!r) throw std::runtime_error("makeIsegWriter called while testing a non-iseg dataset");
    return r;
}

std::shared_ptr<dataset::iseg::Checker> DatasetTest::makeIsegChecker()
{
    auto r = dynamic_pointer_cast<dataset::iseg::Checker>(config().create_checker());
    if (!r) throw std::runtime_error("makeIsegChecker called while testing a non-iseg dataset");
    return r;
}

void DatasetTest::clean()
{
    if (sys::exists(ds_root)) sys::rmtree(ds_root);
    sys::mkdir_ifmissing(ds_root);
    sys::File out(str::joinpath(ds_root, "config"), O_WRONLY | O_CREAT | O_TRUNC, 0666);
    cfg->write(out);
    import_results.clear();
}

void DatasetTest::import(metadata::Collection& mds)
{
    {
        auto writer(config().create_writer());
        auto batch = mds.make_import_batch();
        writer->acquire_batch(batch);
        for (const auto& e: batch)
        {
            wassert(actual(e->dataset_name) == config().name());
            wassert(actual(e->result) == ACQ_OK);
            import_results.push_back(e->md);
            import_results.back().sourceBlob().unlock();
            //fprintf(stderr, "IDX %s %zd: %s\n", testfile.c_str(), import_results.size(), e->md.sourceBlob().to_string().c_str());
        }
    }

    utils::files::removeDontpackFlagfile(ds_root);
}

void DatasetTest::import(const std::string& testfile)
{
    metadata::TestCollection data(testfile);
    import(data);
}

void DatasetTest::import(Metadata& md, dataset::WriterAcquireResult expected_result)
{
    import_results.push_back(md);
    auto writer(config().create_writer());
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

metadata::Collection DatasetTest::query(const std::string& q)
{
    return metadata::Collection(*config().create_reader(), q);
}

void DatasetTest::ensure_localds_clean(size_t filecount, size_t resultcount, bool quick)
{
    nag::CollectHandler tc;
    tc.install();
    auto state = scan_state(quick);
    wassert(actual(state.count(segment::SEGMENT_OK)) == filecount);
    wassert(actual(state.size()) == filecount);

    auto reader = makeSegmentedReader();
    metadata::Collection mdc(*reader, Matcher());
    wassert(actual(mdc.size()) == resultcount);

    if (filecount > 0 && reader->type() != "iseg")
        wassert(actual_file(str::joinpath(reader->dataset().path, idxfname())).exists());
    tc.clear();
}

void DatasetTest::all_clean(size_t segment_count)
{
    auto state = scan_state();
    try {
        wassert(actual(state.size()) == segment_count);
        wassert(actual(state.count(segment::SEGMENT_OK)) == segment_count);
    } catch (...) {
        fprintf(stderr, "Dump of unexpected state:\n");
        state.dump(stderr);
        throw;
    }
}

void DatasetTest::import_all(const metadata::Collection& mds)
{
    clean();

    auto writer = config().create_writer();
    for (const auto& md: mds)
    {
        import_results.push_back(*md);
        WriterAcquireResult res = writer->acquire(import_results.back());
        wassert(actual(res) == ACQ_OK);
        import_results.back().sourceBlob().unlock();
    }

    utils::files::removeDontpackFlagfile(cfg->value("path"));
}

void DatasetTest::import_all_packed(const metadata::Collection& mds)
{
    wassert(import_all(mds));
    wassert(repack());
}

void DatasetTest::repack()
{
    // Pack the dataset in case something imported data out of order
    auto checker = config().create_checker();
    CheckerConfig opts;
    opts.readonly = false;
    wassert(checker->repack(opts));
}

void DatasetTest::query_results(const std::vector<int>& expected)
{
    query_results(DataQuery(Matcher(), true), expected);
}

void DatasetTest::query_results(const dataset::DataQuery& q, const std::vector<int>& expected)
{
    vector<int> found;
    config().create_reader()->query_data(q, [&](std::shared_ptr<Metadata>&& md) {
        unsigned idx;
        for (idx = 0; idx < import_results.size(); ++idx)
            if (import_results[idx].compare_items(*md) == 0)
                break;
        if (idx == import_results.size())
            found.push_back(-1);
        else
            found.push_back(idx);
        if (q.with_data)
            md->get_data();
        return true;
    });

    wassert(actual(str::join(", ", found)) == str::join(", ", expected));
}

void DatasetTest::online_segment_exists(const std::string& relpath, const std::vector<std::string>& extensions)
{
    auto cfg = local_config();
    if (std::dynamic_pointer_cast<const simple::Dataset>(cfg))
    {
        std::vector<std::string> exts(extensions);
        exts.push_back(".metadata");
        exts.push_back(".summary");
        actual_segment(str::joinpath(cfg->path, relpath)).exists(exts);
    } else
        actual_segment(str::joinpath(cfg->path, relpath)).exists(extensions);
}

void DatasetTest::archived_segment_exists(const std::string& relpath, const std::vector<std::string>& extensions)
{
    auto cfg = local_config();
    std::vector<std::string> exts(extensions);
    exts.push_back(".metadata");
    exts.push_back(".summary");
    actual_segment(str::joinpath(cfg->path, ".archive", relpath)).exists(exts);
}

void DatasetTest::skip_if_type_simple()
{
    if (cfg->value("type") == "simple")
        throw TestSkipped("This test makes no sense on simple datasets");
}

}

namespace tests {

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
    compressed.clear();
    issue51.clear();

    count_repacked = -1;
    count_archived = -1;
    count_deleted = -1;
    count_deindexed = -1;
    count_rescanned = -1;
    count_tarred = -1;
    count_compressed = -1;
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
    void segment_compress(const std::string& ds, const std::string& relpath, const std::string& message) override
    {
        recorder.segment_compress(ds, relpath, message);
        seg_results.emplace_back("compressed", ds, relpath, message);
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
        seg_results.match("compressed", expected.compressed, issues);
        seg_results.match("issue51", expected.issue51, issues);

        seg_results.count_equals("repacked", expected.count_repacked, issues);
        seg_results.count_equals("archived", expected.count_archived, issues);
        seg_results.count_equals("deleted", expected.count_deleted, issues);
        seg_results.count_equals("deindexed", expected.count_deindexed, issues);
        seg_results.count_equals("rescanned", expected.count_rescanned, issues);
        seg_results.count_equals("tarred", expected.count_tarred, issues);
        seg_results.count_equals("compressed", expected.count_compressed, issues);
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
void ActualWriter<Dataset>::import(metadata::Collection& mds, dataset::ReplaceStrategy strategy)
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
    auto reporter = make_shared<CollectReporter>();
    dataset::CheckerConfig opts(reporter, !write);
    wassert(this->_actual->repack(opts));
    // reporter.dump(stderr);
    wassert(reporter->check(expected));
}

template<typename Dataset>
void ActualChecker<Dataset>::repack_filtered(const Matcher& matcher, const ReporterExpected& expected, bool write)
{
    auto reporter = make_shared<CollectReporter>();
    dataset::CheckerConfig opts(reporter, !write);
    opts.segment_filter = matcher;
    wassert(this->_actual->repack(opts));
    // reporter->dump(stderr);
    wassert(reporter->check(expected));
}

template<typename Dataset>
void ActualChecker<Dataset>::check(const ReporterExpected& expected, dataset::CheckerConfig& opts)
{
    auto reporter = make_shared<CollectReporter>();
    opts.reporter = reporter;
    wassert(this->_actual->check(opts));
    wassert(reporter->check(expected));
}

template<typename Dataset>
void ActualChecker<Dataset>::check(const ReporterExpected& expected, bool write, bool quick)
{
    auto reporter = make_shared<CollectReporter>();
    dataset::CheckerConfig opts(reporter, !write);
    opts.accurate = !quick;
    wassert(this->_actual->check(opts));
    // reporter->dump(stderr);
    wassert(reporter->check(expected));
}

template<typename Dataset>
void ActualChecker<Dataset>::check_filtered(const Matcher& matcher, const ReporterExpected& expected, bool write, bool quick)
{
    auto reporter = make_shared<CollectReporter>();
    dataset::CheckerConfig opts(reporter, !write);
    opts.accurate = !quick;
    opts.segment_filter = matcher;
    wassert(this->_actual->check(opts));
    //reporter.dump(stderr);
    wassert(reporter->check(expected));
}

template<typename Dataset>
void ActualChecker<Dataset>::check_issue51(const ReporterExpected& expected, bool write)
{
    auto reporter = make_shared<CollectReporter>();
    dataset::CheckerConfig opts(reporter, !write);
    wassert(this->_actual->check_issue51(opts));
    // reporter.dump(stderr);
    wassert(reporter->check(expected));
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
    auto reporter = make_shared<CollectReporter>();
    dataset::CheckerConfig opts(reporter, !write);
    wassert(this->_actual->remove_all(opts));
    // reporter.dump(stderr);
    wassert(reporter->check(expected));
}

template<typename Dataset>
void ActualChecker<Dataset>::remove_all_filtered(const Matcher& matcher, const ReporterExpected& expected, bool write)
{
    auto reporter = make_shared<CollectReporter>();
    dataset::CheckerConfig opts(reporter, !write);
    opts.segment_filter = matcher;
    wassert(this->_actual->remove_all(opts));
    // reporter.dump(stderr);
    wassert(reporter->check(expected));
}

}

template class ActualWriter<dataset::Writer>;
template class ActualWriter<dataset::local::Writer>;
template class ActualWriter<dataset::segmented::Writer>;
template class ActualChecker<dataset::Checker>;
template class ActualChecker<dataset::local::Checker>;
template class ActualChecker<dataset::segmented::Checker>;
}
