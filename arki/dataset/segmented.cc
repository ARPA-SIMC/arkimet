#include "segmented.h"
#include "session.h"
#include "step.h"
#include "time.h"
#include "simple/writer.h"
#include "iseg/writer.h"
#include "maintenance.h"
#include "archive.h"
#include "reporter.h"
#include "arki/core/cfg.h"
#include "arki/dataset/lock.h"
#include "arki/metadata.h"
#include "arki/metadata/collection.h"
#include "arki/scan.h"
#include "arki/types/reftime.h"
#include "arki/types/source.h"
#include "arki/utils/string.h"
#include "arki/utils/sys.h"
#include "arki/utils/files.h"
#include <algorithm>

using namespace std;
using namespace arki::utils;

namespace arki {
namespace dataset {
namespace segmented {

void SegmentState::check_age(const std::filesystem::path& relpath, const Dataset& dataset, dataset::Reporter& reporter)
{
    core::Time archive_threshold(0, 0, 0);
    core::Time delete_threshold(0, 0, 0);
    const auto& st = SessionTime::get();

    if (dataset.archive_age != -1)
        archive_threshold = st.age_threshold(dataset.archive_age);
    if (dataset.delete_age != -1)
        delete_threshold = st.age_threshold(dataset.delete_age);

    if (delete_threshold.ye != 0 && delete_threshold >= interval.end)
    {
        reporter.segment_info(dataset.name(), relpath, "segment old enough to be deleted");
        state += segment::SEGMENT_DELETE_AGE;
        return;
    }

    if (archive_threshold.ye != 0 && archive_threshold >= interval.end)
    {
        reporter.segment_info(dataset.name(), relpath, "segment old enough to be archived");
        state += segment::SEGMENT_ARCHIVE_AGE;
        return;
    }
}

Dataset::Dataset(std::shared_ptr<Session> session, std::shared_ptr<segment::Session> segment_session, const core::cfg::Section& cfg)
    : local::Dataset(session, cfg),
      segment_session(segment_session),
      step_name(str::lower(cfg.value("step"))),
      offline(cfg.value("offline") == "true"),
      smallfiles(cfg.value_bool("smallfiles"))
{
    if (cfg.has("segments")) throw std::runtime_error("segments used in config");
    if (cfg.has("mockdata")) throw std::runtime_error("mockdata used in config");
    if (step_name.empty())
        throw std::runtime_error("Dataset " + name() + " misses step= configuration");

    string repl = cfg.value("replace");
    if (repl == "yes" || repl == "true" || repl == "always")
        default_replace_strategy = REPLACE_ALWAYS;
    else if (repl == "USN")
        default_replace_strategy = REPLACE_HIGHER_USN;
    else if (repl == "" || repl == "no" || repl == "never")
        default_replace_strategy = REPLACE_NEVER;
    else
        throw std::runtime_error("Replace strategy '" + repl + "' is not recognised in the configuration of dataset " + name());

    m_step = Step::create(step_name);

    std::string gz_group_size = cfg.value("gz group size");
    if (!gz_group_size.empty())
        this->gz_group_size = std::stoul(gz_group_size);

    if (cfg.value("eatmydata") == "yes")
        eatmydata = true;
}

Dataset::~Dataset()
{
}

bool Dataset::relpath_timespan(const std::filesystem::path& path, core::Interval& interval) const
{
    return step().path_timespan(path, interval);
}

std::shared_ptr<segment::data::Reader> Dataset::segment_reader(const std::filesystem::path& relpath, std::shared_ptr<core::Lock> lock)
{
    return segment_session->segment_reader(scan::Scanner::format_from_filename(relpath), path, relpath, lock);
}


Reader::~Reader()
{
}


Writer::~Writer()
{
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
    auto format = batch[0]->md.source().format;

    for (auto& e: batch)
    {
        e->dataset_name.clear();

        if (e->md.source().format != format)
        {
            e->md.add_note("cannot acquire into dataset " + name() + ": data is in format " + format_name(e->md.source().format) + " but the batch also has data in format " + format_name(format));
            e->result = ACQ_ERROR;
            continue;
        }

        auto age_check = dataset().check_acquire_age(e->md);
        if (age_check.first)
        {
            e->result = age_check.second;
            if (age_check.second == ACQ_OK)
                e->dataset_name = name();
            continue;
        }

        core::Time time = e->md.get<types::reftime::Position>()->get_Position();
        auto relpath = sys::with_suffix(dataset().step()(time), "."s + format_name(format));
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

    if (type == "iseg" || type == "test")
        return dataset::iseg::Writer::test_acquire(session, cfg, batch);
    if (type == "ondisk2")
        throw std::runtime_error("ondisk2 datasets are not supported anymore. Please convert the dataset to type=iseg");
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
    auto format = scan::Scanner::format_from_filename(segment->segment().relpath);

    // Get the time range for this relpath
    core::Interval interval;
    if (!dataset().relpath_timespan(segment->segment().relpath, interval))
        throw std::runtime_error("cannot archive segment "s + segment->segment().abspath.native() + " because its name does not match the dataset step");

    // Get the contents of this segment
    metadata::Collection mdc;
    get_metadata(wlock, mdc);

    // Move the segment to the archive and deindex it
    auto new_root = dataset().path / ".archive" / "last";
    auto new_relpath = sys::with_suffix(dataset().step()(interval.begin), "."s + format_name(format));
    auto new_abspath = new_root / new_relpath;
    release(new_root, new_relpath, new_abspath);

    // Acquire in the achive
    archives()->index_segment("last" / new_relpath, move(mdc));
}

void CheckerSegment::unarchive()
{
    auto arcrelpath = "last" / segment->segment().relpath;
    archives()->release_segment(arcrelpath, segment->segment().root, segment->segment().relpath, segment->segment().abspath);
    auto reader = segment->segment().detect_data_reader(lock);
    metadata::Collection mdc;
    reader->scan(mdc.inserter_func());
    index(move(mdc));
}

void CheckerSegment::remove_data(const std::vector<uint64_t>& offsets)
{
    throw std::runtime_error(dataset().name() + ": dataset segment does not support removing items");
}


Checker::~Checker()
{
}

void Checker::segments_all(std::function<void(segmented::CheckerSegment& segment)> dest)
{
    segments_tracked(dest);
    segments_untracked(dest);
}

void Checker::segments(CheckerConfig& opts, std::function<void(segmented::CheckerSegment& segment)> dest)
{
    if (!opts.online && !dataset().offline) return;
    if (!opts.offline && dataset().offline) return;

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
    if ((opts.online && !dataset().offline) || (opts.offline && dataset().offline))
        segments(opts, [&](CheckerSegment& segment) { dest(*this, segment); });
    if (opts.offline && dataset().hasArchive())
        dynamic_pointer_cast<archive::Checker>(archive())->segments_recursive(opts, dest);
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

    local::Checker::remove_all(opts);
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

    local::Checker::remove_all(opts);
}

void Checker::tar(CheckerConfig& opts)
{
    segments(opts, [&](CheckerSegment& segment) {
        auto data = segment.segment->segment().detect_data();
        if (data->single_file()) return;
        if (opts.readonly)
            opts.reporter->segment_tar(name(), segment.path_relative(), "should be tarred");
        else
        {
            segment.tar();
            opts.reporter->segment_tar(name(), segment.path_relative(), "tarred");
        }
    });

    local::Checker::tar(opts);
}

void Checker::zip(CheckerConfig& opts)
{
    segments(opts, [&](CheckerSegment& segment) {
        auto data = segment.segment->segment().detect_data();
        if (data->single_file()) return;
        if (opts.readonly)
            opts.reporter->segment_tar(name(), segment.path_relative(), "should be zipped");
        else
        {
            segment.zip();
            opts.reporter->segment_tar(name(), segment.path_relative(), "zipped");
        }
    });

    local::Checker::zip(opts);
}

void Checker::compress(CheckerConfig& opts, unsigned groupsize)
{
    segments(opts, [&](CheckerSegment& segment) {
        auto data = segment.segment->segment().detect_data();
        if (!data->single_file()) return;
        if (opts.readonly)
            opts.reporter->segment_compress(name(), segment.path_relative(), "should be compressed");
        else
        {
            size_t freed = segment.compress(groupsize);
            opts.reporter->segment_compress(name(), segment.path_relative(), "compressed (" + std::to_string(freed) + " freed)");
        }
    });

    local::Checker::compress(opts, groupsize);
}

void Checker::state(CheckerConfig& opts)
{
    segments(opts, [&](CheckerSegment& segment) {
        auto state = segment.scan(*opts.reporter, !opts.accurate);
        opts.reporter->segment_tar(name(), segment.path_relative(),
                state.state.to_string() + " " + state.interval.begin.to_iso8601(' ') + " to " + state.interval.end.to_iso8601(' '));
    });

    local::Checker::state(opts);
}

void Checker::repack(CheckerConfig& opts, unsigned test_flags)
{
    const string& root = dataset().path;

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

    local::Checker::repack(opts, test_flags);
}

void Checker::check(CheckerConfig& opts)
{
    const string& root = dataset().path;

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
    local::Checker::check(opts);
}

void Checker::scan_dir(const std::filesystem::path& root, std::function<void(const std::filesystem::path& relpath)> dest)
{
    files::PathWalk walker(root);
    walker.consumer = [&](const std::filesystem::path& relpath, sys::Path::iterator& entry, struct stat& st) {
        // Skip '.', '..' and hidden files
        if (entry->d_name[0] == '.')
            return false;

        string name = entry->d_name;
        auto abspath = root / relpath / name;
        if (Segment::is_segment(abspath))
        {
            auto basename = Segment::basename(name);
            dest(relpath / basename);
            return false;
        }

        return true;
    };

    walker.walk();
}

}
}
}
