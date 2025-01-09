#include "arki/dataset/simple/checker.h"
#include "arki/dataset/index/manifest.h"
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

namespace {

struct RepackSort : public metadata::sort::Compare
{
    int compare(const Metadata& a, const Metadata& b) const override
    {
        const types::Type* rta = a.get(TYPE_REFTIME);
        const types::Type* rtb = b.get(TYPE_REFTIME);
        if (rta && !rtb) return 1;
        if (!rta && rtb) return -1;
        if (rta && rtb)
            if (int res = rta->compare(*rtb)) return res;
        if (a.sourceBlob().offset > b.sourceBlob().offset) return 1;
        if (b.sourceBlob().offset > a.sourceBlob().offset) return -1;
        return 0;
    }

    std::string toString() const override { return "reftime,offset"; }
};

}


class CheckerSegment : public segmented::CheckerSegment
{
public:
    Checker& checker;

    CheckerSegment(Checker& checker, const std::string& relpath, std::shared_ptr<dataset::CheckLock> lock)
        : segmented::CheckerSegment(lock), checker(checker)
    {
        segment = checker.dataset().session->segment_checker(scan::Scanner::format_from_filename(relpath), dataset().path, relpath);
    }

    std::filesystem::path path_relative() const override { return segment->segment().relpath; }
    const simple::Dataset& dataset() const override { return checker.dataset(); }
    simple::Dataset& dataset() override { return checker.dataset(); }
    std::shared_ptr<dataset::archive::Checker> archives() override { return dynamic_pointer_cast<dataset::archive::Checker>(checker.archive()); }

    void get_metadata(std::shared_ptr<core::Lock> lock, metadata::Collection& mds) override
    {
        auto reader = segment->data().reader(lock);
        reader->scan(mds.inserter_func());
    }

    segmented::SegmentState scan(dataset::Reporter& reporter, bool quick=true) override
    {
        if (!segment->exists_on_disk())
        {
            reporter.segment_info(checker.name(), segment->segment().relpath, "segment found in index but not on disk");
            return segmented::SegmentState(segment::SEGMENT_MISSING);
        }

        if (!checker.m_idx->has_segment(segment->segment().relpath))
        {
            if (segment->is_empty())
            {
                reporter.segment_info(checker.name(), segment->segment().relpath, "empty segment found on disk with no associated index data");
                return segmented::SegmentState(segment::SEGMENT_DELETED);
            } else {
                //bool untrusted_index = files::hasDontpackFlagfile(checker.dataset().path);
                reporter.segment_info(checker.name(), segment->segment().relpath, "segment found on disk with no associated index data");
                //return segmented::SegmentState(untrusted_index ? segment::SEGMENT_UNALIGNED : segment::SEGMENT_DELETED);
                return segmented::SegmentState(segment::SEGMENT_UNALIGNED);
            }
        }

        auto path_metadata = sys::with_suffix(segment->segment().abspath, ".metadata");
        // TODO: replace with a method of Segment
        time_t ts_data = segment->data().timestamp();
        time_t ts_md = sys::timestamp(path_metadata, 0);
        time_t ts_sum = sys::timestamp(sys::with_suffix(segment->segment().abspath, ".summary"), 0);
        time_t ts_idx = checker.m_mft->segment_mtime(segment->segment().relpath);

        segment::State state = segment::SEGMENT_OK;

        // Check timestamp consistency
        if (ts_idx != ts_data || ts_md < ts_data || ts_sum < ts_md)
        {
            if (ts_idx != ts_data)
                nag::verbose("%s: %s has a timestamp (%ld) different than the one in the index (%ld)",
                        checker.dataset().path.c_str(), segment->segment().relpath.c_str(), (long int)ts_data, (long int)ts_idx);
            if (ts_md < ts_data)
                nag::verbose("%s: %s has a timestamp (%ld) newer that its metadata (%ld)",
                        checker.dataset().path.c_str(), segment->segment().relpath.c_str(), (long int)ts_data, (long int)ts_md);
            if (ts_md < ts_data)
                nag::verbose("%s: %s metadata has a timestamp (%ld) newer that its summary (%ld)",
                        checker.dataset().path.c_str(), segment->segment().relpath.c_str(), (long int)ts_md, (long int)ts_sum);
            state = segment::SEGMENT_UNALIGNED;
        }

        // Read metadata of segment contents

        //scan_file(m_path, i.file, state, v);
        //void scan_file(const std::string& root, const std::string& relpath, segment::State state, segment::contents_func dest)

        // TODO: turn this into a Segment::exists/Segment::scan
        metadata::Collection contents;
        if (std::filesystem::exists(path_metadata))
        {
            Metadata::read_file(metadata::ReadContext(path_metadata, checker.dataset().path), [&](std::shared_ptr<Metadata> md) {
                // Tweak Blob sources replacing the file name with segment->relpath
                if (const source::Blob* s = md->has_source_blob())
                    md->set_source(Source::createBlobUnlocked(s->format, checker.dataset().path, segment->segment().relpath, s->offset, s->size));
                contents.acquire(md);
                return true;
            });
        }
        else
            // The index knows about the file, so instead of saying segment::SEGMENT_DELETED
            // because we have data without metadata, we say segment::SEGMENT_UNALIGNED
            // because the metadata needs to be regenerated
            state += segment::SEGMENT_UNALIGNED;

        RepackSort cmp;
        contents.sort(cmp); // Sort by reftime and by offset

        // Compute the span of reftimes inside the segment
        core::Interval segment_interval;
        if (contents.empty())
        {
            reporter.segment_info(checker.name(), segment->segment().relpath, "index knows of this segment but contains no data for it");
            state = segment::SEGMENT_UNALIGNED;
        } else {
            if (!contents.expand_date_range(segment_interval))
            {
                reporter.segment_info(checker.name(), segment->segment().relpath, "index data for this segment has no reference time information");
                state = segment::SEGMENT_CORRUPTED;
            } else {
                // Ensure that the reftime span fits inside the segment step
                core::Interval interval;
                if (checker.dataset().step().path_timespan(segment->segment().relpath, interval))
                {
                    if (segment_interval.begin < interval.begin || segment_interval.end > interval.end)
                    {
                        reporter.segment_info(checker.name(), segment->segment().relpath, "segment contents do not fit inside the step of this dataset");
                        state = segment::SEGMENT_CORRUPTED;
                    }
                    // Expand segment timespan to the full possible segment timespan
                    segment_interval = interval;
                } else {
                    reporter.segment_info(checker.name(), segment->segment().relpath, "segment name does not fit the step of this dataset");
                    state = segment::SEGMENT_CORRUPTED;
                }
            }
        }

        if (state.is_ok())
            state = segment->check([&](const std::string& msg) { reporter.segment_info(checker.name(), segment->segment().relpath, msg); }, contents, quick);

        auto res = segmented::SegmentState(state, segment_interval);
        res.check_age(segment->segment().relpath, checker.dataset(), reporter);
        return res;
    }

    size_t repack(unsigned test_flags) override
    {
        auto lock = checker.lock->write_lock();

        // Read the metadata
        metadata::Collection mds;
        get_metadata(lock, mds);

        // Sort by reference time and offset
        RepackSort cmp;
        mds.sort(cmp);

        return reorder(mds, test_flags);
    }

    size_t reorder(metadata::Collection& mds, unsigned test_flags) override
    {
        auto path_metadata = sys::with_suffix(segment->segment().abspath, ".metadata");
        auto path_summary = sys::with_suffix(segment->segment().abspath, ".summary");

        // Write out the data with the new order
        segment::data::RepackConfig repack_config;
        repack_config.gz_group_size = dataset().gz_group_size;
        repack_config.test_flags = test_flags;
        auto p_repack = segment->repack(checker.dataset().path, mds, repack_config);

        // Strip paths from mds sources
        mds.strip_source_paths();

        // Remove existing cached metadata, since we scramble their order
        std::filesystem::remove(path_metadata);
        std::filesystem::remove(path_summary);

        size_t size_pre = segment->size();

        p_repack.commit();

        size_t size_post = segment->size();

        // Write out the new metadata
        mds.writeAtomically(path_metadata);

        // Regenerate the summary. It is unchanged, really, but its timestamp
        // has become obsolete by now
        Summary sum;
        mds.add_to_summary(sum);
        sum.writeAtomically(path_summary);


        // Reindex with the new file information
        time_t mtime = segment->data().timestamp();
        checker.m_mft->acquire(segment->segment().relpath, mtime, sum);

        return size_pre - size_post;
    }

    size_t remove(bool with_data) override
    {
        checker.m_mft->remove(segment->segment().relpath);
        std::filesystem::remove(sys::with_suffix(segment->segment().abspath, ".metadata"));
        std::filesystem::remove(sys::with_suffix(segment->segment().abspath, ".summary"));
        if (!with_data) return 0;
        return segment->remove();
    }

    void tar() override
    {
        if (std::filesystem::exists(sys::with_suffix(segment->segment().abspath, ".tar")))
            return;

        auto path_metadata = sys::with_suffix(segment->segment().abspath, ".metadata");
        auto path_summary = sys::with_suffix(segment->segment().abspath, ".summary");
        auto lock = checker.lock->write_lock();

        metadata::Collection mds;
        get_metadata(lock, mds);

        // Remove existing cached metadata, since we scramble their order
        std::filesystem::remove(path_metadata);
        std::filesystem::remove(path_summary);

        // Create the .tar segment
        segment = segment->tar(mds);

        // Write out the new metadata
        mds.writeAtomically(path_metadata);

        // Regenerate the summary. It is unchanged, really, but its timestamp
        // has become obsolete by now
        Summary sum;
        mds.add_to_summary(sum);
        sum.writeAtomically(path_summary);

        // Reindex with the new file information
        time_t mtime = segment->data().timestamp();
        checker.m_mft->acquire(segment->segment().relpath, mtime, sum);
    }

    void zip() override
    {
        if (std::filesystem::exists(sys::with_suffix(segment->segment().abspath, ".zip")))
            return;

        auto path_metadata = sys::with_suffix(segment->segment().abspath, ".metadata");
        auto path_summary = sys::with_suffix(segment->segment().abspath, ".summary");
        auto lock = checker.lock->write_lock();

        metadata::Collection mds;
        get_metadata(lock, mds);

        // Remove existing cached metadata, since we scramble their order
        std::filesystem::remove(path_metadata);
        std::filesystem::remove(path_summary);

        // Create the .tar segment
        segment = segment->zip(mds);

        // Write out the new metadata
        mds.writeAtomically(path_metadata);

        // Regenerate the summary. It is unchanged, really, but its timestamp
        // has become obsolete by now
        Summary sum;
        mds.add_to_summary(sum);
        sum.writeAtomically(path_summary);

        // Reindex with the new file information
        time_t mtime = segment->data().timestamp();
        checker.m_mft->acquire(segment->segment().relpath, mtime, sum);
    }

    size_t compress(unsigned groupsize) override
    {
        if (std::filesystem::exists(sys::with_suffix(segment->segment().abspath, ".gz")) ||
                std::filesystem::exists(sys::with_suffix(segment->segment().abspath, ".gz.idx")))
            return 0;

        auto path_metadata = sys::with_suffix(segment->segment().abspath, ".metadata");
        auto path_summary = sys::with_suffix(segment->segment().abspath, ".summary");
        auto lock = checker.lock->write_lock();

        metadata::Collection mds;
        get_metadata(lock, mds);

        // Remove existing cached metadata, since we scramble their order
        std::filesystem::remove(path_metadata);
        std::filesystem::remove(path_summary);

        // Create the .tar segment
        size_t old_size = segment->size();
        segment = segment->compress(mds, groupsize);
        size_t new_size = segment->size();

        // Write out the new metadata
        mds.writeAtomically(path_metadata);

        // Regenerate the summary. It is unchanged, really, but its timestamp
        // has become obsolete by now
        Summary sum;
        mds.add_to_summary(sum);
        sum.writeAtomically(path_summary);

        // Reindex with the new file information
        time_t mtime = segment->data().timestamp();
        checker.m_mft->acquire(segment->segment().relpath, mtime, sum);

        if (old_size > new_size)
            return old_size - new_size;
        else
            return 0;
    }

    void index(metadata::Collection&& mds) override
    {
        time_t mtime = segment->data().timestamp();

        // Iterate the metadata, computing the summary and making the data
        // paths relative
        mds.strip_source_paths();
        Summary sum;
        mds.add_to_summary(sum);

        // Regenerate .metadata and .summary
        mds.writeAtomically(sys::with_suffix(segment->segment().abspath, ".metadata"));
        sum.writeAtomically(sys::with_suffix(segment->segment().abspath, ".summary"));

        // Add to manifest
        checker.m_mft->acquire(segment->segment().relpath, mtime, sum);
        checker.m_mft->flush();
    }

    void rescan(dataset::Reporter& reporter) override
    {
        auto path_metadata = sys::with_suffix(segment->segment().abspath, ".metadata");
        auto path_summary = sys::with_suffix(segment->segment().abspath, ".summary");

        // Delete cached info to force a full rescan
        std::filesystem::remove(path_metadata);
        std::filesystem::remove(path_summary);

        auto dirname = segment->segment().abspath.parent_path();
        auto basename = segment->segment().abspath.filename();

        metadata::Collection mds;
        segment->rescan_data(
                [&](const std::string& msg) { reporter.segment_info(checker.name(), segment->segment().relpath, msg); },
                lock, [&](std::shared_ptr<Metadata> md) {
                    auto& source = md->sourceBlob();
                    md->set_source(Source::createBlobUnlocked(segment->segment().format, dirname, basename, source.offset, source.size));
                    mds.acquire(md);
                    return true;
                });

        Summary sum;
        for (const auto& md: mds)
            sum.add(*md);

        // Regenerate .metadata and .summary
        mds.writeAtomically(path_metadata);
        sum.writeAtomically(path_summary);

        // Add to manifest
        checker.m_mft->acquire(segment->segment().relpath, segment->data().timestamp(), sum);
        checker.m_mft->flush();
    }

    void release(const std::filesystem::path& new_root, const std::filesystem::path& new_relpath, const std::filesystem::path& new_abspath) override
    {
        checker.m_mft->remove(segment->segment().relpath);
        segment = segment->move(new_root, new_relpath, new_abspath);
    }
};


Checker::Checker(std::shared_ptr<simple::Dataset> dataset)
    : DatasetAccess(dataset), m_mft(0)
{
    // Create the directory if it does not exist
    std::filesystem::create_directories(dataset->path);

    lock = dataset->check_lock_dataset();

    // If the index is missing, take note not to perform a repack until a
    // check is made
    if (!index::Manifest::exists(dataset->path))
        files::createDontpackFlagfile(dataset->path);

    unique_ptr<index::Manifest> mft = index::Manifest::create(dataset, dataset->index_type);
    m_mft = mft.release();
    m_mft->lock = lock;
    m_mft->openRW();
    m_idx = m_mft;
}

Checker::~Checker()
{
    m_mft->flush();
    delete m_idx;
}

std::string Checker::type() const { return "simple"; }

void Checker::repack(CheckerConfig& opts, unsigned test_flags)
{
    segmented::Checker::repack(opts, test_flags);
    m_mft->flush();
}

void Checker::check(CheckerConfig& opts)
{
    segmented::Checker::check(opts);
    m_mft->flush();
}

std::unique_ptr<segmented::CheckerSegment> Checker::segment(const std::filesystem::path& relpath)
{
    return unique_ptr<segmented::CheckerSegment>(new CheckerSegment(*this, relpath, lock));
}

std::unique_ptr<segmented::CheckerSegment> Checker::segment_prelocked(const std::filesystem::path& relpath, std::shared_ptr<dataset::CheckLock> lock)
{
    return unique_ptr<segmented::CheckerSegment>(new CheckerSegment(*this, relpath, lock));
}

void Checker::segments_tracked(std::function<void(segmented::CheckerSegment& segment)> dest)
{
    std::vector<std::string> names;
    m_idx->list_segments([&](const std::string& relpath) { names.push_back(relpath); });

    for (const auto& relpath: names)
    {
        CheckerSegment segment(*this, relpath, lock);
        dest(segment);
    }
}

void Checker::segments_tracked_filtered(const Matcher& matcher, std::function<void(segmented::CheckerSegment& segment)> dest)
{
    // TODO: implement filtering
    std::vector<std::string> names;
    m_idx->list_segments_filtered(matcher, [&](const std::filesystem::path& relpath) { names.push_back(relpath); });

    for (const auto& relpath: names)
    {
        CheckerSegment segment(*this, relpath, lock);
        dest(segment);
    }
}

void Checker::segments_untracked(std::function<void(segmented::CheckerSegment& relpath)> dest)
{
    scan_dir(dataset().path, [&](const std::filesystem::path& relpath) {
        if (m_idx->has_segment(relpath)) return;
        CheckerSegment segment(*this, relpath, lock);
        dest(segment);
    });
}

void Checker::segments_untracked_filtered(const Matcher& matcher, std::function<void(segmented::CheckerSegment& relpath)> dest)
{
    if (matcher.empty()) return segments_untracked(dest);
    auto m = matcher.get(TYPE_REFTIME);
    if (!m) return segments_untracked(dest);

    scan_dir(dataset().path, [&](const std::filesystem::path& relpath) {
        if (m_idx->has_segment(relpath)) return;
        if (!dataset().step().pathMatches(relpath, *m)) return;
        CheckerSegment segment(*this, relpath, lock);
        dest(segment);
    });
}

size_t Checker::vacuum(dataset::Reporter& reporter)
{
    reporter.operation_progress(name(), "repack", "running VACUUM ANALYZE on the dataset index, if applicable");
    return m_mft->vacuum();
}

void Checker::test_delete_from_index(const std::filesystem::path& relpath)
{
    m_idx->test_deindex(relpath);
    string pathname = dataset().path / relpath;
    std::filesystem::remove(pathname + ".metadata");
    std::filesystem::remove(pathname + ".summary");
}

void Checker::test_invalidate_in_index(const std::filesystem::path& relpath)
{
    m_idx->test_deindex(relpath);
    sys::touch(sys::with_suffix(dataset().path / relpath, ".metadata"), 1496167200);
}

std::shared_ptr<Metadata> Checker::test_change_metadata(const std::filesystem::path& relpath, std::shared_ptr<Metadata> md, unsigned data_idx)
{
    auto md_pathname = sys::with_suffix(dataset().path / relpath, ".metadata");
    metadata::Collection mds;
    mds.read_from_file(md_pathname);
    md->set_source(std::unique_ptr<arki::types::Source>(mds[data_idx].source().clone()));
    mds.replace(data_idx, md);
    mds.writeAtomically(md_pathname);
    return md;
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
    m_idx->list_segments([&](const std::filesystem::path& relpath) {
        metadata::Collection mds;
        m_idx->query_segment(relpath, mds.inserter_func());
        if (mds.empty()) return;
        File datafile(dataset().path / relpath, O_RDONLY);
        // Iterate all metadata in the segment
        unsigned count_otherformat = 0;
        unsigned count_ok = 0;
        unsigned count_issue51 = 0;
        unsigned count_corrupted = 0;
        for (const auto& md: mds) {
            const auto& blob = md->sourceBlob();
            // Keep only segments with grib or bufr files
            if (blob.format != "grib" && blob.format != "bufr")
            {
                ++count_otherformat;
                continue;
            }
            // Read the last 4 characters
            char tail[4];
            if (datafile.pread(tail, 4, blob.offset + blob.size - 4) != 4)
            {
                opts.reporter->segment_info(name(), relpath, "cannot read 4 bytes at position " + std::to_string(blob.offset + blob.size - 4));
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
                broken_mds[relpath].push_back(*md);
            } else {
                ++count_corrupted;
                opts.reporter->segment_info(name(), relpath, "end marker 7777 or 777? not found at position " + std::to_string(blob.offset + blob.size - 4));
            }
        }
        nag::verbose("Checked %s:%s: %u ok, %u other formats, %u issue51, %u corrupted", name().c_str(), relpath.c_str(), count_ok, count_otherformat, count_issue51, count_corrupted);
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
                if (blob.format != "grib" && blob.format != "bufr")
                    return;
                datafile.pwrite("7", 1, blob.offset + blob.size - 1);
            }
            opts.reporter->segment_issue51(name(), i.first, "fixed corrupted terminator signatures");
        }
    }

    return segmented::Checker::check_issue51(opts);
}

void Checker::test_make_overlap(const std::filesystem::path& relpath, unsigned overlap_size, unsigned data_idx)
{
    metadata::Collection mds;
    m_idx->query_segment(relpath, mds.inserter_func());
    dataset().session->segment_checker(scan::Scanner::format_from_filename(relpath), dataset().path, relpath)->test_make_overlap(mds, overlap_size, data_idx);
    m_idx->test_make_overlap(relpath, overlap_size, data_idx);
}

void Checker::test_make_hole(const std::filesystem::path& relpath, unsigned hole_size, unsigned data_idx)
{
    metadata::Collection mds;
    m_idx->query_segment(relpath, mds.inserter_func());
    dataset().session->segment_checker(scan::Scanner::format_from_filename(relpath), dataset().path, relpath)->test_make_hole(mds, hole_size, data_idx);
    m_idx->test_make_hole(relpath, hole_size, data_idx);
}

void Checker::test_corrupt_data(const std::filesystem::path& relpath, unsigned data_idx)
{
    metadata::Collection mds;
    m_idx->query_segment(relpath, mds.inserter_func());
    dataset().session->segment_checker(scan::Scanner::format_from_filename(relpath), dataset().path, relpath)->test_corrupt(mds, data_idx);
}

void Checker::test_truncate_data(const std::filesystem::path& relpath, unsigned data_idx)
{
    metadata::Collection mds;
    m_idx->query_segment(relpath, mds.inserter_func());
    dataset().session->segment_checker(scan::Scanner::format_from_filename(relpath), dataset().path, relpath)->test_truncate_by_data(mds, data_idx);
}

void Checker::test_swap_data(const std::filesystem::path& relpath, unsigned d1_idx, unsigned d2_idx)
{
    metadata::Collection mds;
    m_idx->query_segment(relpath, mds.inserter_func());
    mds.swap(d1_idx, d2_idx);

    segment(relpath)->reorder(mds);
}

void Checker::test_rename(const std::filesystem::path& relpath, const std::filesystem::path& new_relpath)
{
    auto segment = dataset().session->segment_checker(scan::Scanner::format_from_filename(relpath), dataset().path, relpath);
    m_idx->test_rename(relpath, new_relpath);
    segment->move(dataset().path, new_relpath, dataset().path / new_relpath);
}

}
}
}

