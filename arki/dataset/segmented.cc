#include "segmented.h"
#include "session.h"
#include "step.h"
#include "time.h"
#include "ondisk2/writer.h"
#include "simple/writer.h"
#include "iseg/writer.h"
#include "maintenance.h"
#include "archive.h"
#include "reporter.h"
#include "arki/core/cfg.h"
#include "arki/dataset/lock.h"
#include "arki/metadata.h"
#include "arki/metadata/collection.h"
#include "arki/types/reftime.h"
#include "arki/types/source.h"
#include "arki/utils/string.h"
#include "arki/utils/sys.h"
#include "arki/utils/files.h"
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

Config::Config(std::shared_ptr<Session> session, const core::cfg::Section& cfg)
    : LocalConfig(session, cfg),
      step_name(str::lower(cfg.value("step"))),
      offline(cfg.value("offline") == "true"),
      smallfiles(cfg.value_bool("smallfiles"))
{
    if (cfg.has("segments")) throw std::runtime_error("segments used in config");
    if (cfg.has("mockdata")) throw std::runtime_error("mockdata used in config");
    if (step_name.empty())
        throw std::runtime_error("Dataset " + name + " misses step= configuration");

    string repl = cfg.value("replace");
    if (repl == "yes" || repl == "true" || repl == "always")
        default_replace_strategy = REPLACE_ALWAYS;
    else if (repl == "USN")
        default_replace_strategy = REPLACE_HIGHER_USN;
    else if (repl == "" || repl == "no" || repl == "never")
        default_replace_strategy = REPLACE_NEVER;
    else
        throw std::runtime_error("Replace strategy '" + repl + "' is not recognised in the configuration of dataset " + name);

    std::string shard = cfg.value("shard");
    m_step = Step::create(step_name);

    std::string gz_group_size = cfg.value("gz group size");
    if (!gz_group_size.empty())
        this->gz_group_size = std::stoul(gz_group_size);
}

Config::~Config()
{
}

bool Config::relpath_timespan(const std::string& path, core::Time& start_time, core::Time& end_time) const
{
    return step().path_timespan(path, start_time, end_time);
}

std::shared_ptr<const Config> Config::create(std::shared_ptr<Session> session, const core::cfg::Section& cfg)
{
    return std::shared_ptr<const Config>(new Config(session, cfg));
}


Reader::~Reader()
{
}


Writer::~Writer()
{
}

std::shared_ptr<segment::Writer> Writer::file(const Metadata& md, const std::string& format)
{
    const core::Time& time = md.get<types::reftime::Position>()->time;
    string relpath = config().step()(time) + "." + md.source().format;
    return config().session->segment_writer(format, config().path, relpath);
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
        string relpath = config().step()(time) + "." + format;
        by_segment[relpath].push_back(e);
    }

    for (auto& b: by_segment)
        std::stable_sort(b.second.begin(), b.second.end(), writer_batch_element_lt);

    return by_segment;
}

void Writer::test_acquire(std::shared_ptr<Session> session, const core::cfg::Section& cfg, WriterBatch& batch)
{
    string type = str::lower(cfg.value("type"));
    if (type.empty())
        type = "local";

    if (type == "iseg")
        return dataset::iseg::Writer::test_acquire(session, cfg, batch);
    if (type == "ondisk2" || type == "test")
        return dataset::ondisk2::Writer::test_acquire(session, cfg, batch);
    if (type == "simple" || type == "error" || type == "duplicates")
        return dataset::simple::Writer::test_acquire(session, cfg, batch);

    throw std::runtime_error("cannot simulate dataset acquisition: unknown dataset type \""+type+"\"");
}

CheckerSegment::CheckerSegment(std::shared_ptr<dataset::CheckLock> lock)
    : lock(lock)
{
}

CheckerSegment::~CheckerSegment()
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
    size_t pos = segment->segment().relpath.rfind(".");
    if (pos == string::npos)
        throw std::runtime_error("cannot archive segment " + segment->segment().abspath + " because it does not have a format extension");
    string format = segment->segment().relpath.substr(pos + 1);

    // Get the time range for this relpath
    core::Time start_time, end_time;
    if (!config().relpath_timespan(segment->segment().relpath, start_time, end_time))
        throw std::runtime_error("cannot archive segment " + segment->segment().abspath + " because its name does not match the dataset step");

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
    string arcrelpath = str::joinpath("last", segment->segment().relpath);
    archives().release_segment(arcrelpath, segment->segment().root, segment->segment().relpath, segment->segment().abspath);
    auto reader = segment->segment().reader(lock);
    metadata::Collection mdc;
    reader->scan(mdc.inserter_func());
    index(move(mdc));
}


Checker::~Checker()
{
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
        if (segment.segment->segment().single_file()) return;
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

void Checker::zip(CheckerConfig& opts)
{
    segments(opts, [&](CheckerSegment& segment) {
        if (segment.segment->segment().single_file()) return;
        if (opts.readonly)
            opts.reporter->segment_tar(name(), segment.path_relative(), "should be zipped");
        else
        {
            segment.zip();
            opts.reporter->segment_tar(name(), segment.path_relative(), "zipped");
        }
    });

    LocalChecker::zip(opts);
}

void Checker::compress(CheckerConfig& opts, unsigned groupsize)
{
    segments(opts, [&](CheckerSegment& segment) {
        if (!segment.segment->segment().single_file()) return;
        if (opts.readonly)
            opts.reporter->segment_compress(name(), segment.path_relative(), "should be compressed");
        else
        {
            size_t freed = segment.compress(groupsize);
            opts.reporter->segment_compress(name(), segment.path_relative(), "compressed (" + std::to_string(freed) + " freed)");
        }
    });

    LocalChecker::compress(opts, groupsize);
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

void Checker::scan_dir(const std::string& root, std::function<void(const std::string& relpath)> dest)
{
    // Trim trailing '/'
    string m_root = root;
    while (m_root.size() > 1 and m_root[m_root.size()-1] == '/')
        m_root.resize(m_root.size() - 1);

    files::PathWalk walker(m_root);
    walker.consumer = [&](const std::string& relpath, sys::Path::iterator& entry, struct stat& st) {
        // Skip '.', '..' and hidden files
        if (entry->d_name[0] == '.')
            return false;

        string name = entry->d_name;
        string abspath = str::joinpath(m_root, relpath, name);
        if (Segment::is_segment(abspath))
        {
            string basename = Segment::basename(name);
            dest(str::joinpath(relpath, basename));
            return false;
        }

        return true;
    };

    walker.walk();
}

}
}
}
