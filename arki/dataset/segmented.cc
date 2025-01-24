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
#include "arki/types/source/blob.h"
#include "arki/utils/string.h"
#include "arki/utils/sys.h"
#include "arki/utils/files.h"
#include "arki/nag.h"
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

std::shared_ptr<archive::Dataset> Dataset::archive()
{
    if (!m_archive)
    {
        m_archive = std::shared_ptr<archive::Dataset>(new archive::Dataset(session, path / ".archive", step_name));
        m_archive->set_parent(this);
    }
    return m_archive;
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

CheckerSegment::CheckerSegment(std::shared_ptr<const Segment> segment, std::shared_ptr<core::CheckLock> lock)
    : lock(lock), segment(segment), segment_checker(segment->checker(lock)), segment_data(segment->data()), segment_data_checker(segment_data->checker())
{
}

CheckerSegment::~CheckerSegment()
{
}

segment::Fixer::ConvertResult CheckerSegment::tar()
{
    auto fixer = segment_checker->fixer();
    auto res = fixer->tar();
    segment_data_checker = fixer->segment().data_checker();
    post_convert(fixer, res);
    return res;
}

segment::Fixer::ConvertResult CheckerSegment::zip()
{
    auto fixer = segment_checker->fixer();
    auto res = fixer->zip();
    segment_data_checker = fixer->segment().data_checker();
    post_convert(fixer, res);
    return res;
}

segment::Fixer::ConvertResult CheckerSegment::compress(unsigned groupsize)
{
    auto fixer = segment_checker->fixer();
    auto res = fixer->compress(groupsize);
    segment_data_checker = fixer->segment().data_checker();
    post_convert(fixer, res);
    return res;
}

segment::Fixer::ReorderResult CheckerSegment::repack(unsigned test_flags)
{
    metadata::Collection mds = segment_checker->scan();
    mds.sort_segment();

    segment::data::RepackConfig repack_config;
    repack_config.gz_group_size = dataset().gz_group_size;
    repack_config.test_flags = test_flags;

    auto fixer = segment_checker->fixer();
    auto res = fixer->reorder(mds, repack_config);
    post_repack(fixer, res);
    return res;
}

size_t CheckerSegment::remove(bool with_data)
{
    auto fixer = segment_checker->fixer();
    pre_remove(fixer);
    return fixer->remove(with_data);
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
    auto format = scan::Scanner::format_from_filename(segment_data_checker->segment().relpath());

    // Get the time range for this relpath
    core::Interval interval;
    if (!dataset().relpath_timespan(segment_data_checker->segment().relpath(), interval))
        throw std::runtime_error("cannot archive segment "s + segment_data_checker->segment().abspath().native() + " because its name does not match the dataset step");

    // Get the contents of this segment
    metadata::Collection mds = segment_checker->scan();

    // Move the segment to the archive and deindex it
    auto new_relpath = "last" / sys::with_suffix(dataset().step()(interval.begin), "."s + format_name(format));
    release(dataset().archive()->segment_session, new_relpath);

    // Acquire in the achive
    archives()->index_segment(new_relpath, std::move(mds));
}

void CheckerSegment::unarchive()
{
    auto arcrelpath = "last" / segment_data_checker->segment().relpath();
    metadata::Collection mdc = archives()->release_segment(arcrelpath, dataset().segment_session, segment_data_checker->segment().relpath());
    index(std::move(mdc));
}

segment::Fixer::MarkRemovedResult CheckerSegment::remove_data(const std::set<uint64_t>& offsets)
{
    auto fixer = segment_checker->fixer();
    auto res = fixer->mark_removed(offsets);
    post_remove_data(fixer, res);
    return res;
}


Checker::~Checker()
{
}

std::unique_ptr<CheckerSegment> Checker::segment_from_relpath(const std::filesystem::path& relpath)
{
    return segment(dataset().segment_session->segment_from_relpath(relpath));
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
        auto state = segment.fsck(*opts.reporter, !opts.accurate);
        if (!state.state.has(segment::SEGMENT_DELETE_AGE)) return;
        if (opts.readonly)
            opts.reporter->segment_remove(name(), segment.path_relative(), "should be deleted");
        else
        {
            auto freed = segment.remove(true);
            opts.reporter->segment_remove(name(), segment.path_relative(), "deleted (" + std::to_string(freed) + " freed)");
        }
    });

    local::Checker::remove_all(opts);
}

void Checker::remove_all(CheckerConfig& opts)
{
    segments(opts, [&](CheckerSegment& segment) {
        if (opts.readonly)
            opts.reporter->segment_remove(name(), segment.path_relative(), "should be deleted");
        else
        {
            auto freed = segment.remove(true);
            opts.reporter->segment_remove(name(), segment.path_relative(), "deleted (" + std::to_string(freed) + " freed)");
        }
    });

    local::Checker::remove_all(opts);
}

void Checker::remove(const metadata::Collection& mds)
{
    // Group mds by segment
    // TODO: this is needed for rocky8 and ubuntu jammy. Use
    // std::filesystem::path when we can rely on newer GCC
    std::unordered_map<std::string, std::set<uint64_t>> by_segment;
    // std::unordered_map<std::filesystem::path, std::set<uint64_t>> by_segment;

    // Build a todo-list of entries to delete for each segment
    for (const auto& md: mds)
    {
        const types::source::Blob* source = md->has_source_blob();
        if (!source)
            throw std::runtime_error("cannot remove metadata from dataset, because it has no Blob source");

        if (source->basedir != dataset().path)
            throw std::runtime_error("cannot remove metadata from dataset: its basedir is " + source->basedir.native() + " but this dataset is at " + dataset().path.native());

        core::Time time = md->get<types::reftime::Position>()->get_Position();
        auto relpath = sys::with_suffix(dataset().step()(time), "."s + format_name(source->format));

        if (!dataset().segment_session->is_data_segment(relpath))
            continue;

        by_segment[relpath].emplace(source->offset);
    }

    for (const auto& i: by_segment)
    {
        segment::data::WriterConfig writer_config;
        writer_config.drop_cached_data_on_commit = false;
        writer_config.eatmydata = dataset().eatmydata;

        auto segment = dataset().segment_session->segment_from_relpath(i.first);
        auto seg = this->segment(segment);
        seg->remove_data(i.second);
        arki::nag::verbose("%s: %s: %zu data marked as deleted", name().c_str(), i.first.c_str(), i.second.size());
    }
}

void Checker::tar(CheckerConfig& opts)
{
    segments(opts, [&](CheckerSegment& segment) {
        const auto& data = segment.segment_data_checker->data();
        if (data.single_file()) return;
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
        const auto& data = segment.segment_data_checker->data();
        if (data.single_file()) return;
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
        const auto& data = segment.segment_data_checker->data();
        if (!data.single_file()) return;
        if (opts.readonly)
            opts.reporter->segment_compress(name(), segment.path_relative(), "should be compressed");
        else
        {
            auto res = segment.compress(groupsize);
            auto freed = res.size_pre > res.size_post ? res.size_pre - res.size_post : 0;
            opts.reporter->segment_compress(name(), segment.path_relative(), "compressed (" + std::to_string(freed) + " freed)");
        }
    });

    local::Checker::compress(opts, groupsize);
}

void Checker::state(CheckerConfig& opts)
{
    segments(opts, [&](CheckerSegment& segment) {
        auto state = segment.fsck(*opts.reporter, !opts.accurate);
        opts.reporter->segment_info(name(), segment.path_relative(),
                state.state.to_string() + " " + state.interval.begin.to_iso8601(' ') + " to " + state.interval.end.to_iso8601(' '));
    });

    local::Checker::state(opts);
}

void Checker::repack(CheckerConfig& opts, unsigned test_flags)
{
    const auto& root = dataset().path;

    if (files::hasDontpackFlagfile(root))
    {
        opts.reporter->operation_aborted(name(), "repack", "dataset needs checking first");
        return;
    }

    std::unique_ptr<maintenance::Agent> repacker;
    if (opts.readonly)
        repacker.reset(new maintenance::MockRepacker(*opts.reporter, *this, test_flags));
    else
        // No safeguard against a deleted index: we catch that in the
        // constructor and create the don't pack flagfile
        repacker.reset(new maintenance::RealRepacker(*opts.reporter, *this, test_flags));

    try {
        segments(opts, [&](CheckerSegment& segment) {
            (*repacker)(segment, segment.fsck(*opts.reporter, true).state);
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
            fixer(segment, segment.fsck(*opts.reporter, !opts.accurate).state);
        });
        fixer.end();
    } else {
        maintenance::RealFixer fixer(*opts.reporter, *this);
        try {
            segments(opts, [&](CheckerSegment& segment) {
                fixer(segment, segment.fsck(*opts.reporter, !opts.accurate).state);
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

void Checker::scan_dir(std::function<void(std::shared_ptr<const Segment> segment)> dest)
{
    files::PathWalk walker(dataset().path);
    walker.consumer = [&](const std::filesystem::path& relpath, const sys::Path::iterator& entry, struct stat&) {
        // Skip '.', '..' and hidden files
        if (entry->d_name[0] == '.')
            return false;

        string name = entry->d_name;
        if (dataset().segment_session->is_data_segment(relpath / name))
        {
            auto basename = Segment::basename(name);
            auto segment = dataset().segment_session->segment_from_relpath(relpath / basename);
            dest(segment);
            return false;
        }

        return true;
    };

    walker.walk();
}

void Checker::test_touch_contents(time_t timestamp)
{
    segments_tracked([&](CheckerSegment& segment) {
        auto fixer = segment.segment_checker->fixer();
        fixer->test_touch_contents(timestamp);
    });
}

void Checker::test_corrupt_data(const std::filesystem::path& relpath, unsigned data_idx)
{
    auto segment = dataset().segment_session->segment_from_relpath(relpath);
    auto csegment = this->segment(segment);
    auto fixer = csegment->segment_checker->fixer();
    fixer->test_corrupt_data(data_idx);
}

void Checker::test_truncate_data(const std::filesystem::path& relpath, unsigned data_idx)
{
    auto segment = dataset().segment_session->segment_from_relpath(relpath);
    auto csegment = this->segment(segment);
    auto fixer = csegment->segment_checker->fixer();
    auto pft = segment->data()->preserve_mtime();
    fixer->test_truncate_data(data_idx);
}

void Checker::test_swap_data(const std::filesystem::path& relpath, unsigned d1_idx, unsigned d2_idx)
{
    auto segment = dataset().segment_session->segment_from_relpath(relpath);
    auto csegment = this->segment(segment);
    arki::metadata::Collection mds = csegment->segment_checker->scan();
    mds.swap(d1_idx, d2_idx);

    auto pft = segment->data()->preserve_mtime();

    segment::data::RepackConfig repack_config;
    repack_config.gz_group_size = dataset().gz_group_size;

    auto fixer = csegment->segment_checker->fixer();
    fixer->reorder(mds, repack_config);
}

void Checker::test_rename(const std::filesystem::path& relpath, const std::filesystem::path& new_relpath)
{
    auto dest = dataset().segment_session->segment_from_relpath(new_relpath);
    auto csegment = segment_from_relpath(relpath);
    auto fixer = csegment->segment_checker->fixer();
    fixer->move(dest);
}

metadata::Collection Checker::test_change_metadata(const std::filesystem::path& relpath, std::shared_ptr<Metadata> md, unsigned data_idx)
{
    auto csegment = segment_from_relpath(relpath);
    auto pmt = csegment->segment_data->preserve_mtime();

    metadata::Collection mds = csegment->segment_checker->scan();
    md->set_source(std::unique_ptr<arki::types::Source>(mds[data_idx].source().clone()));
    md->sourceBlob().unlock();
    mds.replace(data_idx, md);

    auto fixer = csegment->segment_checker->fixer();
    fixer->reindex(mds);
    return mds;
}

void Checker::test_delete_from_index(const std::filesystem::path& relpath)
{
    auto csegment = segment_from_relpath(relpath);
    auto fixer = csegment->segment_checker->fixer();
    fixer->test_mark_all_removed();
}

}
}
}
