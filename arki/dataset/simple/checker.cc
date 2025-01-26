#include "checker.h"
#include "manifest.h"
#include "arki/dataset/reporter.h"
#include "arki/dataset/step.h"
#include "arki/dataset/lock.h"
#include "arki/dataset/session.h"
#include "arki/dataset/archive.h"
#include "arki/types.h"
#include "arki/types/source/blob.h"
#include "arki/summary.h"
#include "arki/metadata.h"
#include "arki/matcher.h"
#include "arki/metadata/collection.h"
#include "arki/metadata/sort.h"
#include "arki/scan.h"
#include "arki/nag.h"
#include "arki/utils/files.h"
#include "arki/utils/sys.h"
#include "arki/utils/string.h"
#include <cstdio>
#include <cstring>

using namespace std;
using namespace arki;
using namespace arki::core;
using namespace arki::types;
using namespace arki::utils;
using arki::core::Time;

namespace arki {
namespace dataset {
namespace simple {

class CheckerSegment : public segmented::CheckerSegment
{
public:
    Checker& dataset_checker;

    CheckerSegment(Checker& checker, std::shared_ptr<const Segment> segment, std::shared_ptr<core::CheckLock> lock)
        : segmented::CheckerSegment(segment, lock), dataset_checker(checker)
    {
        if (auto hooks = dataset().test_hooks)
            hooks->on_check_lock(*segment);
    }

    std::filesystem::path path_relative() const override { return segment_data_checker->segment().relpath(); }
    const simple::Dataset& dataset() const override { return dataset_checker.dataset(); }
    simple::Dataset& dataset() override { return dataset_checker.dataset(); }
    std::shared_ptr<dataset::archive::Checker> archives() override { return dynamic_pointer_cast<dataset::archive::Checker>(dataset_checker.archive()); }

    segmented::SegmentState fsck(dataset::Reporter& reporter, bool quick=true) override
    {
        segmented::SegmentState res;

        // Get the allowed time interval from the segment name
        if (!dataset().step().path_timespan(segment->relpath(), res.interval))
        {
            reporter.segment_info(dataset_checker.name(), segment_data_checker->segment().relpath(), "segment name does not fit the step of this dataset");
            res.state += segment::SEGMENT_CORRUPTED;
            return res;
        }

        // Run segment-specific scan
        auto segment_reporter = reporter.segment_reporter(dataset_checker.name());
        auto segment_fsck = segment_checker->fsck(*segment_reporter, quick);
        res.state += segment_fsck.state;
        if (res.state.has_any(segment::SEGMENT_MISSING + segment::SEGMENT_DELETED + segment::SEGMENT_UNALIGNED))
            return res;

        // At this point we know the segment is consistent and contains data

        const auto* segment_info = dataset_checker.manifest.segment(segment->relpath());
        if (!segment_info)
        {
            //bool untrusted_index = files::hasDontpackFlagfile(dataset_checker.dataset().path);
            reporter.segment_info(dataset_checker.name(), segment->relpath(), "segment found on disk with no associated MANIFEST data");
            //return segmented::SegmentState(untrusted_index ? segment::SEGMENT_UNALIGNED : segment::SEGMENT_DELETED);
            res.state += segment::SEGMENT_UNOPTIMIZED;
        } else if (segment_info->mtime != segment_fsck.mtime) {
            //bool untrusted_index = files::hasDontpackFlagfile(dataset_checker.dataset().path);
            reporter.segment_info(dataset_checker.name(), segment->relpath(), "segment mtime does not match MANIFEST information");
            //return segmented::SegmentState(untrusted_index ? segment::SEGMENT_UNALIGNED : segment::SEGMENT_DELETED);
            res.state += segment::SEGMENT_UNOPTIMIZED;
        } else if (segment_info->time != segment_fsck.interval) {
            reporter.segment_info(dataset_checker.name(), segment->relpath(), "segment data interval does not match MANIFEST information");
            res.state += segment::SEGMENT_UNOPTIMIZED;
        }

        if (!res.interval.contains(segment_fsck.interval))
        {
            reporter.segment_info(dataset_checker.name(), segment->relpath(), "segment contents do not fit inside the step of this dataset");
            res.state += segment::SEGMENT_CORRUPTED;
            return res;
        }

        res.check_age(segment_data_checker->segment().relpath(), dataset(), reporter);

        return res;
    }

    void post_remove_data(std::shared_ptr<segment::Fixer>, segment::Fixer::MarkRemovedResult& res) override
    {
        // Reindex with the new file information
        dataset_checker.manifest.set(segment->relpath(), res.segment_mtime, res.data_timespan);
        dataset_checker.manifest.flush();
    }

    void post_repack(std::shared_ptr<segment::Fixer>, segment::Fixer::ReorderResult& res) override
    {
        // Reindex with the new file information
        dataset_checker.manifest.set_mtime(segment->relpath(), res.segment_mtime);
        dataset_checker.manifest.flush();
    }

    void pre_remove(std::shared_ptr<segment::Fixer>) override
    {
        dataset_checker.manifest.remove(segment->relpath());
        dataset_checker.manifest.flush();
    }

    void post_convert(std::shared_ptr<segment::Fixer>, segment::Fixer::ConvertResult& res) override
    {
        // Reindex with the new file information
        dataset_checker.manifest.set_mtime(segment_data_checker->segment().relpath(), res.segment_mtime);
        dataset_checker.manifest.flush();
    }

    void index(metadata::Collection&& mds) override
    {
        auto fixer = segment_checker->fixer();
        fixer->reindex(mds);

        time_t mtime = segment_data->timestamp().value();
        core::Interval interval;
        mds.expand_date_range(interval);

        // Add to manifest
        dataset_checker.manifest.set(segment->relpath(), mtime, interval);
        dataset_checker.manifest.flush();

        std::filesystem::remove(segment->abspath_iseg_index());
    }

    void rescan(dataset::Reporter& reporter) override
    {
        auto path_metadata = segment->abspath_metadata();
        auto path_summary = segment->abspath_summary();

        metadata::Collection mds;
        segment_data_checker->rescan_data(
                [&](const std::string& msg) { reporter.segment_info(dataset_checker.name(), segment_data_checker->segment().relpath(), msg); },
                lock, mds.inserter_func());

        time_t mtime = segment_data->timestamp().value();
        core::Interval interval;
        mds.expand_date_range(interval);

        auto fixer = segment_checker->fixer();
        fixer->reindex(mds);

        // Add to manifest
        dataset_checker.manifest.set(segment->relpath(), mtime, interval);
        dataset_checker.manifest.flush();
    }

    arki::metadata::Collection release(std::shared_ptr<const segment::Session> new_segment_session, const std::filesystem::path& new_relpath) override
    {
        // TODO: get a fixer lock
        metadata::Collection mds = segment_checker->scan();
        segment_data_checker->move(new_segment_session, new_relpath);
        dataset_checker.manifest.remove(segment_data_checker->segment().relpath());
        dataset_checker.manifest.flush();
        return mds;
    }
};


Checker::Checker(std::shared_ptr<simple::Dataset> dataset)
    : DatasetAccess(dataset), manifest(dataset->path, dataset->eatmydata)
{
    // Create the directory if it does not exist
    std::filesystem::create_directories(dataset->path);

    lock = dataset->check_lock_dataset();

    // If the index is missing, take note not to perform a repack until a
    // check is made
    if (!manifest::exists(dataset->path))
        files::createDontpackFlagfile(dataset->path);

    manifest.reread();
}

Checker::~Checker()
{
    manifest.flush();
}

std::string Checker::type() const { return "simple"; }

void Checker::repack(CheckerConfig& opts, unsigned test_flags)
{
    segmented::Checker::repack(opts, test_flags);
    manifest.flush();
}

void Checker::check(CheckerConfig& opts)
{
    segmented::Checker::check(opts);
    manifest.flush();
}

std::unique_ptr<segmented::CheckerSegment> Checker::segment(std::shared_ptr<const Segment> segment)
{
    return unique_ptr<segmented::CheckerSegment>(new CheckerSegment(*this, segment, lock));
}

std::unique_ptr<segmented::CheckerSegment> Checker::segment_prelocked(std::shared_ptr<const Segment> segment, std::shared_ptr<core::CheckLock> lock)
{
    return unique_ptr<segmented::CheckerSegment>(new CheckerSegment(*this, segment, lock));
}

void Checker::segments_tracked(std::function<void(segmented::CheckerSegment& segment)> dest)
{
    auto files = manifest.file_list();
    for (const auto& info: files)
    {
        auto segment = dataset().segment_session->segment_from_relpath(info.relpath);
        CheckerSegment csegment(*this, segment, lock);
        dest(csegment);
    }
}

void Checker::segments_tracked_filtered(const Matcher& matcher, std::function<void(segmented::CheckerSegment& segment)> dest)
{
    auto files = manifest.file_list(matcher);
    for (const auto& info: files)
    {
        auto segment = dataset().segment_session->segment_from_relpath(info.relpath);
        CheckerSegment csegment(*this, segment, lock);
        dest(csegment);
    }
}

void Checker::segments_untracked(std::function<void(segmented::CheckerSegment& relpath)> dest)
{
    scan_dir([&](std::shared_ptr<const Segment> segment) {
        if (manifest.segment(segment->relpath())) return;
        CheckerSegment csegment(*this, segment, lock);
        dest(csegment);
    });
}

void Checker::segments_untracked_filtered(const Matcher& matcher, std::function<void(segmented::CheckerSegment& relpath)> dest)
{
    if (matcher.empty()) return segments_untracked(dest);
    auto m = matcher.get(TYPE_REFTIME);
    if (!m) return segments_untracked(dest);

    scan_dir([&](std::shared_ptr<const Segment> segment) {
        if (manifest.segment(segment->relpath())) return;
        if (!dataset().step().pathMatches(segment->relpath(), *m)) return;
        CheckerSegment csegment(*this, segment, lock);
        dest(csegment);
    });
}

size_t Checker::vacuum(dataset::Reporter& reporter)
{
    return 0;
}

void Checker::test_delete_from_index(const std::filesystem::path& relpath)
{
    segmented::Checker::test_delete_from_index(relpath);
    utils::files::PreserveFileTimes pft(manifest.root() / "MANIFEST");
    manifest.remove(relpath);
    manifest.flush();
}

void Checker::test_invalidate_in_index(const std::filesystem::path& relpath)
{
    manifest.remove(relpath);
    manifest.flush();
}

metadata::Collection Checker::test_change_metadata(const std::filesystem::path& relpath, std::shared_ptr<Metadata> md, unsigned data_idx)
{
    auto mds = segmented::Checker::test_change_metadata(relpath, md, data_idx);
    core::Interval interval;
    mds.expand_date_range(interval);

    utils::files::PreserveFileTimes pft(manifest.root() / "MANIFEST");
    if (const auto* mseg = manifest.segment(relpath))
        manifest.set(relpath, mseg->mtime, interval);

    return mds;
}


// TODO: during checks, run file:///usr/share/doc/sqlite3-doc/pragma.html#debug
// and delete the index if it fails

void Checker::check_issue51(CheckerConfig& opts)
{
    if (!opts.online && !dataset().offline) return;
    if (!opts.offline && dataset().offline) return;

    // Broken metadata for each segment
    std::map<string, metadata::Collection> broken_mds;

    // Iterate all segments
    const auto& files = manifest.file_list();
    for (const auto& info: files)
    {
        auto csegment = segment_from_relpath(info.relpath);
        metadata::Collection mds = csegment->segment_checker->scan();
        if (mds.empty()) return;
        File datafile(csegment->segment->abspath(), O_RDONLY);
        // Iterate all metadata in the segment
        unsigned count_otherformat = 0;
        unsigned count_ok = 0;
        unsigned count_issue51 = 0;
        unsigned count_corrupted = 0;
        for (const auto& md: mds) {
            const auto& blob = md->sourceBlob();
            // Keep only segments with grib or bufr files
            if (blob.format != DataFormat::GRIB && blob.format != DataFormat::BUFR)
            {
                ++count_otherformat;
                continue;
            }
            // Read the last 4 characters
            char tail[4];
            if (datafile.pread(tail, 4, blob.offset + blob.size - 4) != 4)
            {
                opts.reporter->segment_info(name(), csegment->segment->relpath(), "cannot read 4 bytes at position " + std::to_string(blob.offset + blob.size - 4));
                return;
            }
            // Check if it ends with 7777
            if (memcmp(tail, "7777", 4) == 0)
            {
                ++count_ok;
                continue;
            }
            // If it instead ends with 777?, take note of it
            if (memcmp(tail, "777", 3) == 0)
            {
                ++count_issue51;
                broken_mds[csegment->segment->relpath()].push_back(*md);
            } else {
                ++count_corrupted;
                opts.reporter->segment_info(name(), csegment->segment->relpath(), "end marker 7777 or 777? not found at position " + std::to_string(blob.offset + blob.size - 4));
            }
        }
        nag::verbose("Checked %s:%s: %u ok, %u other formats, %u issue51, %u corrupted", name().c_str(), csegment->segment->relpath().c_str(), count_ok, count_otherformat, count_issue51, count_corrupted);
    }

    if (opts.readonly)
    {
        for (const auto& i: broken_mds)
            opts.reporter->segment_issue51(name(), i.first, "segment contains data with corrupted terminator signature");
    } else {
        for (const auto& i: broken_mds)
        {
            // Make a backup copy with .issue51 extension, if it doesn't already exist
            auto abspath = dataset().path / i.first;
            utils::files::PreserveFileTimes pf(abspath);
            auto backup = sys::with_suffix(abspath, ".issue51");
            if (!std::filesystem::exists(backup))
            {
                File src(abspath, O_RDONLY);
                File dst(backup, O_WRONLY | O_CREAT | O_EXCL, 0666);
                std::array<char, 40960> buffer;
                while (true)
                {
                    size_t sz = src.read(buffer.data(), buffer.size());
                    if (!sz) break;
                    dst.write_all_or_throw(buffer.data(), sz);
                }
            }

            // Fix the file
            File datafile(abspath, O_RDWR);
            for (const auto& md: i.second) {
                const auto& blob = md->sourceBlob();
                // Keep only segments with grib or bufr files
                if (blob.format != DataFormat::GRIB && blob.format != DataFormat::BUFR)
                    return;
                datafile.pwrite("7", 1, blob.offset + blob.size - 1);
            }
            opts.reporter->segment_issue51(name(), i.first, "fixed corrupted terminator signatures");
        }
    }

    return segmented::Checker::check_issue51(opts);
}

void Checker::test_rename(const std::filesystem::path& relpath, const std::filesystem::path& new_relpath)
{
    segmented::Checker::test_rename(relpath, new_relpath);

    utils::files::PreserveFileTimes pft(manifest.root() / "MANIFEST");
    manifest.rename(relpath, new_relpath);
    manifest.flush();
}

void Checker::test_touch_contents(time_t timestamp)
{
    segmented::Checker::test_touch_contents(timestamp);

    const auto& files = manifest.file_list();
    for (const auto& f: files)
        manifest.set_mtime(f.relpath, timestamp);
    manifest.flush();

    sys::touch(manifest.root() / "MANIFEST", timestamp);
}

}
}
}

