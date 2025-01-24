#include "arki/dataset/iseg/checker.h"
#include "arki/segment/iseg.h"
#include "arki/segment/iseg/index.h"
#include "arki/dataset/session.h"
#include "arki/dataset/step.h"
#include "arki/dataset/reporter.h"
#include "arki/dataset/lock.h"
#include "arki/dataset/archive.h"
#include "arki/dataset/index/summarycache.h"
#include "arki/types/reftime.h"
#include "arki/types/source/blob.h"
#include "arki/metadata.h"
#include "arki/matcher.h"
#include "arki/metadata/collection.h"
#include "arki/nag.h"
#include "arki/utils/sys.h"
#include "arki/utils/string.h"
#include "arki/core/binary.h"
#include <system_error>
#include <algorithm>
#include <cstring>

using namespace std;
using namespace arki;
using namespace arki::core;
using namespace arki::types;
using namespace arki::utils;
using arki::segment::iseg::CIndex;

namespace arki {
namespace dataset {
namespace iseg {

class CheckerSegment : public segmented::CheckerSegment
{
public:
    Checker& dataset_checker;

    CheckerSegment(Checker& checker, std::shared_ptr<const Segment> segment, std::shared_ptr<core::CheckLock> lock)
        : segmented::CheckerSegment(segment, lock), dataset_checker(checker)
    {
    }
    ~CheckerSegment()
    {
    }

    CIndex& idx()
    {
        return static_pointer_cast<segment::iseg::Checker>(segment_checker)->index();
    }
    std::filesystem::path path_relative() const override { return segment_data_checker->segment().relpath(); }
    const iseg::Dataset& dataset() const override { return dataset_checker.dataset(); }
    iseg::Dataset& dataset() override { return dataset_checker.dataset(); }
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

        if (!res.interval.contains(segment_fsck.interval))
        {
            reporter.segment_info(dataset_checker.name(), segment->relpath(), "segment contents do not fit inside the step of this dataset");
            res.state += segment::SEGMENT_CORRUPTED;
            return res;
        }

        res.check_age(segment_data_checker->segment().relpath(), dataset(), reporter);

        return res;
    }

    void index(metadata::Collection&& mds) override
    {
        auto fixer = segment_checker->fixer();
        fixer->reindex(mds);

        // Remove .metadata and .summary files
        std::filesystem::remove(segment->abspath_metadata());
        std::filesystem::remove(segment->abspath_summary());
    }

    void rescan(dataset::Reporter& reporter) override
    {
        auto unique_codes = idx().unique_codes();

        metadata::Collection mds;
        segment_data_checker->rescan_data(
                [&](const std::string& msg) { reporter.segment_info(dataset_checker.name(), segment_data_checker->segment().relpath(), msg); },
                lock, mds.inserter_func());

        // Filter out duplicates
        mds = mds.without_duplicates(unique_codes);

        auto fixer = segment_checker->fixer();
        fixer->reindex(mds);
    }

    arki::metadata::Collection release(std::shared_ptr<const segment::Session> new_segment_session, const std::filesystem::path& new_relpath) override
    {
        metadata::Collection mds = segment_checker->scan();
        // TODO: get a fixer lock
        segment_data_checker->move(new_segment_session, new_relpath);
        std::filesystem::remove(segment->abspath_iseg_index());
        return mds;
    }
};


Checker::Checker(std::shared_ptr<iseg::Dataset> dataset)
    : DatasetAccess(dataset)
{
    // Create the directory if it does not exist
    std::filesystem::create_directories(dataset->path);
}

std::string Checker::type() const { return "iseg"; }

void Checker::list_segments(std::function<void(std::shared_ptr<const Segment>)> dest)
{
    list_segments(Matcher(), dest);
}

void Checker::list_segments(const Matcher& matcher, std::function<void(std::shared_ptr<const Segment>)> dest)
{
    auto format = dataset().iseg_segment_session->format;
    std::vector<std::filesystem::path> seg_relpaths;
    step::SegmentQuery squery(dataset().path, format, "\\.index$", matcher);
    dataset().step().list_segments(squery, [&](std::filesystem::path&& s) {
        seg_relpaths.emplace_back(std::move(s));
    });
    std::sort(seg_relpaths.begin(), seg_relpaths.end());
    for (const auto& relpath: seg_relpaths)
        dest(dataset().segment_session->segment_from_relpath_and_format(relpath, format));
}

std::unique_ptr<segmented::CheckerSegment> Checker::segment(std::shared_ptr<const Segment> segment)
{
    auto lock = dataset().check_lock_segment(segment->relpath());
    return unique_ptr<segmented::CheckerSegment>(new CheckerSegment(*this, segment, lock));
}

std::unique_ptr<segmented::CheckerSegment> Checker::segment_prelocked(std::shared_ptr<const Segment> segment, std::shared_ptr<core::CheckLock> lock)
{
    return unique_ptr<segmented::CheckerSegment>(new CheckerSegment(*this, segment, lock));
}

void Checker::segments_tracked(std::function<void(segmented::CheckerSegment& segment)> dest)
{
    segments_tracked_filtered(Matcher(), dest);
}

void Checker::segments_tracked_filtered(const Matcher& matcher, std::function<void(segmented::CheckerSegment& segment)> dest)
{
    list_segments(matcher, [&](std::shared_ptr<const Segment> segment) {
        auto lock = dataset().check_lock_segment(segment->relpath());
        CheckerSegment csegment(*this, segment, lock);
        dest(csegment);
    });
}

void Checker::segments_untracked(std::function<void(segmented::CheckerSegment& relpath)> dest)
{
    segments_untracked_filtered(Matcher(), dest);
}

void Checker::segments_untracked_filtered(const Matcher& matcher, std::function<void(segmented::CheckerSegment& segment)> dest)
{
    step::SegmentQuery squery(dataset().path, dataset().iseg_segment_session->format, matcher);
    dataset().step().list_segments(squery, [&](std::filesystem::path&& relpath) {
        auto segment = dataset().segment_session->segment_from_relpath(relpath);
        auto lock = dataset().check_lock_segment(relpath);
        if (sys::stat(segment->abspath_iseg_index())) return;
        CheckerSegment csegment(*this, segment, lock);
        // See #279: directory segments that are empty directories are found by
        // a filesystem scan, but are not considered segments
        if (!csegment.segment_data_checker->data().exists_on_disk())
            return;
        dest(csegment);
    });
}

void Checker::check_issue51(CheckerConfig& opts)
{
    if (!opts.online && !dataset().offline) return;
    if (!opts.offline && dataset().offline) return;

    // Broken metadata for each segment
    std::map<std::filesystem::path, metadata::Collection> broken_mds;

    // Iterate all segments
    list_segments([&](std::shared_ptr<const Segment> segment) {
        auto lock = dataset().check_lock_segment(segment->relpath());
        auto idx = m_dataset->iseg_segment_session->check_index(segment, lock);
        metadata::Collection mds;
        idx->scan(mds.inserter_func(), "reftime, offset");
        if (mds.empty()) return;
        File datafile(segment->abspath(), O_RDONLY);
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
                opts.reporter->segment_info(name(), segment->relpath(), "cannot read 4 bytes at position " + std::to_string(blob.offset + blob.size - 4));
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
                broken_mds[segment->relpath()].push_back(*md);
            } else {
                ++count_corrupted;
                opts.reporter->segment_info(name(), segment->relpath(), "end marker 7777 or 777? not found at position " + std::to_string(blob.offset + blob.size - 4));
            }
        }
        nag::verbose("Checked %s:%s: %u ok, %u other formats, %u issue51, %u corrupted", name().c_str(), segment->relpath().c_str(), count_ok, count_otherformat, count_issue51, count_corrupted);
    });

    if (opts.readonly)
    {
        for (const auto& i: broken_mds)
            opts.reporter->segment_issue51(name(), i.first, "segment contains data with corrupted terminator signature");
    } else {
        for (const auto& i: broken_mds)
        {
            // Make a backup copy with .issue51 extension, if it doesn't already exist
            auto abspath = dataset().path / i.first;
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

void Checker::remove(const metadata::Collection& mds)
{
    segmented::Checker::remove(mds);

    // Months to invalidate in summary cache
    std::set<std::pair<int, int>> months;

    // Build a todo-list of entries to delete for each segment
    for (const auto& md: mds)
    {
        Time time = md->get<types::reftime::Position>()->get_Position();
        months.insert(std::make_pair(time.ye, time.mo));
    }

    index::SummaryCache scache(dataset().summary_cache_pathname);
    for (const auto& i: months)
        scache.invalidate(i.first, i.second);
}

size_t Checker::vacuum(dataset::Reporter&)
{
    return 0;
}

void Checker::test_make_overlap(const std::filesystem::path& relpath, unsigned overlap_size, unsigned data_idx)
{
    auto segment = m_dataset->iseg_segment_session->segment_from_relpath_and_format(relpath, m_dataset->iseg_segment_session->format);
    auto lock = dataset().check_lock_segment(relpath);
    auto wrlock = lock->write_lock();
    auto idx = m_dataset->iseg_segment_session->check_index(segment, lock);
    metadata::Collection mds;
    idx->query_segment(mds.inserter_func());
    dataset().segment_session->segment_data_checker(segment)->test_make_overlap(mds, overlap_size, data_idx);
    idx->test_make_overlap(overlap_size, data_idx);
}

void Checker::test_make_hole(const std::filesystem::path& relpath, unsigned hole_size, unsigned data_idx)
{
    auto segment = m_dataset->iseg_segment_session->segment_from_relpath_and_format(relpath, m_dataset->iseg_segment_session->format);
    auto lock = dataset().check_lock_segment(relpath);
    auto wrlock = lock->write_lock();
    auto idx = m_dataset->iseg_segment_session->check_index(segment, lock);
    metadata::Collection mds;
    idx->query_segment(mds.inserter_func());
    dataset().segment_session->segment_data_checker(segment)->test_make_hole(mds, hole_size, data_idx);
    idx->test_make_hole(hole_size, data_idx);
}

void Checker::test_invalidate_in_index(const std::filesystem::path& relpath)
{
    std::filesystem::remove(dataset().path / sys::with_suffix(relpath, ".index"));
}

}
}
}

