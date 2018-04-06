#include "segmented.h"
#include "step.h"
#include "time.h"
#include "ondisk2/writer.h"
#include "simple/writer.h"
#include "iseg/writer.h"
#include "maintenance.h"
#include "archive.h"
#include "reporter.h"
#include "arki/dataset/lock.h"
#include "arki/metadata.h"
#include "arki/metadata/collection.h"
#include "arki/scan/any.h"
#include "arki/types/reftime.h"
#include "arki/types/source.h"
#include "arki/utils/string.h"
#include "arki/utils/sys.h"
#include "arki/utils/files.h"
#include "arki/utils/compress.h"
#include "arki/configfile.h"
#include <algorithm>
#include <system_error>
#include <sstream>

using namespace std;
using namespace arki::utils;

namespace arki {
namespace dataset {
namespace segmented {

void SegmentState::check_age(const std::string& relpath, const Config& cfg, dataset::Reporter& reporter)
{
    core::Time archive_threshold(0, 0, 0);
    core::Time delete_threshold(0, 0, 0);
    const auto& st = SessionTime::get();

    if (cfg.archive_age != -1)
        archive_threshold = st.age_threshold(cfg.archive_age);
    if (cfg.delete_age != -1)
        delete_threshold = st.age_threshold(cfg.delete_age);

    if (delete_threshold.ye != 0 && delete_threshold >= until)
    {
        reporter.segment_info(cfg.name, relpath, "segment old enough to be deleted");
        state = state + segment::SEGMENT_DELETE_AGE;
        return;
    }

    if (archive_threshold.ye != 0 && archive_threshold >= until)
    {
        reporter.segment_info(cfg.name, relpath, "segment old enough to be archived");
        state = state + segment::SEGMENT_ARCHIVE_AGE;
        return;
    }
}

Config::Config(const ConfigFile& cfg)
    : LocalConfig(cfg),
      step_name(str::lower(cfg.value("step"))),
      force_dir_segments(cfg.value("segments") == "dir"),
      mock_data(cfg.value("mockdata") == "true"),
      offline(cfg.value("offline") == "true")
{
    if (step_name.empty())
        throw std::runtime_error("Dataset " + name + " misses step= configuration");

    string repl = cfg.value("replace");
    if (repl == "yes" || repl == "true" || repl == "always")
        default_replace_strategy = Writer::REPLACE_ALWAYS;
    else if (repl == "USN")
        default_replace_strategy = Writer::REPLACE_HIGHER_USN;
    else if (repl == "" || repl == "no" || repl == "never")
        default_replace_strategy = Writer::REPLACE_NEVER;
    else
        throw std::runtime_error("Replace strategy '" + repl + "' is not recognised in the configuration of dataset " + name);

    std::string shard = cfg.value("shard");
    m_step = Step::create(step_name);
}

Config::~Config()
{
}

bool Config::relpath_timespan(const std::string& path, core::Time& start_time, core::Time& end_time) const
{
    return step().path_timespan(path, start_time, end_time);
}

std::unique_ptr<SegmentManager> Config::create_segment_manager() const
{
    return SegmentManager::get(path, force_dir_segments, mock_data);
}

std::shared_ptr<const Config> Config::create(const ConfigFile& cfg)
{
    return std::shared_ptr<const Config>(new Config(cfg));
}


Reader::~Reader()
{
    delete m_segment_manager;
}

SegmentManager& Reader::segment_manager()
{
    if (!m_segment_manager)
        m_segment_manager = config().create_segment_manager().release();
    return *m_segment_manager;
}


Writer::~Writer()
{
    delete m_segment_manager;
}

SegmentManager& Writer::segment_manager()
{
    if (!m_segment_manager)
        m_segment_manager = config().create_segment_manager().release();
    return *m_segment_manager;
}

std::shared_ptr<segment::Writer> Writer::file(const Metadata& md, const std::string& format)
{
    const core::Time& time = md.get<types::reftime::Position>()->time;
    string relname = config().step()(time) + "." + md.source().format;
    return segment_manager().get_writer(format, relname);
}

static bool writer_batch_element_lt(const std::shared_ptr<WriterBatchElement>& a, const std::shared_ptr<WriterBatchElement>& b)
{
    const types::Type* ta = a->md.get(TYPE_REFTIME);
    const types::Type* tb = b->md.get(TYPE_REFTIME);
    if (!ta && !tb) return false;
    if (ta && !tb) return false;
    if (!ta && tb) return true;
    return *ta < *tb;
}

std::map<std::string, WriterBatch> Writer::batch_by_segment(WriterBatch& batch)
{
    // Clear dataset names, pre-process items that do not need further
    // dispatching, and divide the rest of the batch by segment
    std::map<std::string, WriterBatch> by_segment;

    if (batch.empty()) return by_segment;
    std::string format = batch[0]->md.source().format;

    for (auto& e: batch)
    {
        e->dataset_name.clear();

        if (e->md.source().format != format)
        {
            e->md.add_note("cannot acquire into dataset " + name() + ": data is in format " + e->md.source().format + " but the batch also has data in format " + format);
            e->result = ACQ_ERROR;
            continue;
        }

        auto age_check = config().check_acquire_age(e->md);
        if (age_check.first)
        {
            e->result = age_check.second;
            if (age_check.second == ACQ_OK)
                e->dataset_name = name();
            continue;
        }

        const core::Time& time = e->md.get<types::reftime::Position>()->time;
        string relname = config().step()(time) + "." + format;
        by_segment[relname].push_back(e);
    }

    for (auto& b: by_segment)
        std::sort(b.second.begin(), b.second.end(), writer_batch_element_lt);

    return by_segment;
}

void Writer::test_acquire(const ConfigFile& cfg, WriterBatch& batch, std::ostream& out)
{
    string type = str::lower(cfg.value("type"));
    if (type.empty())
        type = "local";

    if (type == "iseg")
        return dataset::iseg::Writer::test_acquire(cfg, batch, out);
    if (type == "ondisk2" || type == "test")
        return dataset::ondisk2::Writer::test_acquire(cfg, batch, out);
    if (type == "simple" || type == "error" || type == "duplicates")
        return dataset::simple::Writer::test_acquire(cfg, batch, out);

    throw std::runtime_error("cannot simulate dataset acquisition: unknown dataset type \""+type+"\"");
}

CheckerSegment::CheckerSegment(std::shared_ptr<dataset::CheckLock> lock)
    : lock(lock)
{
}

CheckerSegment::~CheckerSegment()
{
}

void CheckerSegment::tar()
{
}

void CheckerSegment::archive()
{
    // TODO: this is a hack to ensure that 'last' is created (and clean) before
    // we start moving files into it. The "created" part is not a problem:
    // releaseSegment will create all relevant paths. The "clean" part is the
    // problem, because opening a writer on an already existing path creates a
    // needs-check-do-not-pack file
    archives();

    auto wlock = lock->write_lock();

    // Get the format for this relpath
    size_t pos = segment->relname.rfind(".");
    if (pos == string::npos)
        throw std::runtime_error("cannot archive segment " + segment->absname + " because it does not have a format extension");
    string format = segment->relname.substr(pos + 1);

    // Get the time range for this relpath
    core::Time start_time, end_time;
    if (!config().relpath_timespan(segment->relname, start_time, end_time))
        throw std::runtime_error("cannot archive segment " + segment->absname + " because its name does not match the dataset step");

    // Get the contents of this segment
    metadata::Collection mdc;
    get_metadata(wlock, mdc);

    // Move the segment to the archive and deindex it
    string new_root = str::joinpath(config().path, ".archive", "last");
    string new_relpath = config().step()(start_time) + "." + format;
    string new_abspath = str::joinpath(new_root, new_relpath);
    release(new_root, new_relpath, new_abspath);

    // Acquire in the achive
    archives().index_segment(str::joinpath("last", new_relpath), move(mdc));
}

void CheckerSegment::unarchive()
{
    string arcrelpath = str::joinpath("last", segment->relname);
    archives().release_segment(arcrelpath, segment->root, segment->relname, segment->absname);

    bool compressed = scan::isCompressed(segment->absname);

    // Acquire in the achive
    metadata::Collection mdc;
    if (sys::exists(segment->absname + ".metadata"))
        mdc.read_from_file(segment->absname + ".metadata");
    else if (compressed) {
        utils::compress::TempUnzip tu(segment->absname);
        scan::scan(segment->absname, lock, mdc.inserter_func());
    } else
        scan::scan(segment->absname, lock, mdc.inserter_func());
    index(move(mdc));
}


Checker::~Checker()
{
    delete m_segment_manager;
}

SegmentManager& Checker::segment_manager()
{
    if (!m_segment_manager)
        m_segment_manager = config().create_segment_manager().release();
    return *m_segment_manager;
}

void Checker::segments_all(std::function<void(segmented::CheckerSegment& segment)> dest)
{
    segments_tracked(dest);
    segments_untracked(dest);
}

void Checker::segments_all_filtered(const Matcher& matcher, std::function<void(segmented::CheckerSegment& segment)> dest)
{
    segments_tracked_filtered(matcher, dest);
    segments_untracked_filtered(matcher, dest);
}

void Checker::segments(CheckerConfig& opts, std::function<void(segmented::CheckerSegment& segment)> dest)
{
    if (!opts.online && !config().offline) return;
    if (!opts.offline && config().offline) return;

    if (opts.segment_filter.empty())
    {
        segments_tracked(dest);
        segments_untracked(dest);
    } else {
        segments_tracked_filtered(opts.segment_filter, dest);
        segments_untracked_filtered(opts.segment_filter, dest);
    }
}

void Checker::segments_recursive(CheckerConfig& opts, std::function<void(segmented::Checker&, segmented::CheckerSegment&)> dest)
{
    if ((opts.online && !config().offline) || (opts.offline && config().offline))
        segments(opts, [&](CheckerSegment& segment) { dest(*this, segment); });
    if (opts.offline && hasArchive())
        archive().segments_recursive(opts, dest);
}

void Checker::remove_old(CheckerConfig& opts)
{
    segments(opts, [&](CheckerSegment& segment) {
        auto state = segment.scan(*opts.reporter, !opts.accurate);
        if (!state.state.has(segment::SEGMENT_DELETE_AGE)) return;
        if (opts.readonly)
            opts.reporter->segment_delete(name(), segment.path_relative(), "should be deleted");
        else
        {
            auto freed = segment.remove(true);
            opts.reporter->segment_delete(name(), segment.path_relative(), "deleted (" + std::to_string(freed) + " freed)");
        }
    });

    LocalChecker::remove_all(opts);
}

void Checker::remove_all(CheckerConfig& opts)
{
    segments(opts, [&](CheckerSegment& segment) {
        if (opts.readonly)
            opts.reporter->segment_delete(name(), segment.path_relative(), "should be deleted");
        else
        {
            auto freed = segment.remove(true);
            opts.reporter->segment_delete(name(), segment.path_relative(), "deleted (" + std::to_string(freed) + " freed)");
        }
    });

    LocalChecker::remove_all(opts);
}

void Checker::tar(CheckerConfig& opts)
{
    segments(opts, [&](CheckerSegment& segment) {
        if (segment.segment->single_file()) return;
        if (opts.readonly)
            opts.reporter->segment_tar(name(), segment.path_relative(), "should be tarred");
        else
        {
            segment.tar();
            opts.reporter->segment_tar(name(), segment.path_relative(), "tarred");
        }
    });

    LocalChecker::tar(opts);
}

void Checker::state(CheckerConfig& opts)
{
    segments(opts, [&](CheckerSegment& segment) {
        auto state = segment.scan(*opts.reporter, !opts.accurate);
        opts.reporter->segment_tar(name(), segment.path_relative(),
                state.state.to_string() + " " + state.begin.to_iso8601(' ') + " to " + state.until.to_iso8601(' '));
    });

    LocalChecker::state(opts);
}

void Checker::repack(CheckerConfig& opts, unsigned test_flags)
{
    const string& root = config().path;

    if (files::hasDontpackFlagfile(root))
    {
        opts.reporter->operation_aborted(name(), "repack", "dataset needs checking first");
        return;
    }

    unique_ptr<maintenance::Agent> repacker;
    if (opts.readonly)
        repacker.reset(new maintenance::MockRepacker(*opts.reporter, *this, test_flags));
    else
        // No safeguard against a deleted index: we catch that in the
        // constructor and create the don't pack flagfile
        repacker.reset(new maintenance::RealRepacker(*opts.reporter, *this, test_flags));

    try {
        segments(opts, [&](CheckerSegment& segment) {
            (*repacker)(segment, segment.scan(*opts.reporter, true).state);
        });
        repacker->end();
    } catch (...) {
        // FIXME: this only makes sense for ondisk2
        // FIXME: also for iseg. Add the marker at the segment level instead of
        // the dataset level, so that the try/catch can be around the single
        // segment, and cleared on a segment by segment basis
        files::createDontpackFlagfile(root);
        throw;
    }

    LocalChecker::repack(opts, test_flags);
}

void Checker::check(CheckerConfig& opts)
{
    const string& root = config().path;

    if (opts.readonly)
    {
        maintenance::MockFixer fixer(*opts.reporter, *this);
        segments(opts, [&](CheckerSegment& segment) {
            fixer(segment, segment.scan(*opts.reporter, !opts.accurate).state);
        });
        fixer.end();
    } else {
        maintenance::RealFixer fixer(*opts.reporter, *this);
        try {
            segments(opts, [&](CheckerSegment& segment) {
                fixer(segment, segment.scan(*opts.reporter, !opts.accurate).state);
            });
            fixer.end();
        } catch (...) {
            // FIXME: Add the marker at the segment level instead of
            // the dataset level, so that the try/catch can be around the single
            // segment, and cleared on a segment by segment basis
            files::createDontpackFlagfile(root);
            throw;
        }

        files::removeDontpackFlagfile(root);
    }
    LocalChecker::check(opts);
}

}
}
}
