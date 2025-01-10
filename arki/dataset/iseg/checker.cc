#include "arki/dataset/iseg/checker.h"
#include "arki/segment/index/iseg.h"
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
using arki::segment::index::iseg::CIndex;

namespace arki {
namespace dataset {
namespace iseg {

namespace {

/// Create unique strings from metadata
struct IDMaker
{
    std::set<types::Code> components;

    IDMaker(const std::set<types::Code>& components) : components(components) {}

    vector<uint8_t> make_string(const Metadata& md) const
    {
        vector<uint8_t> res;
        core::BinaryEncoder enc(res);
        for (set<types::Code>::const_iterator i = components.begin(); i != components.end(); ++i)
            if (const Type* t = md.get(*i))
                t->encodeBinary(enc);
        return res;
    }
};

}


class CheckerSegment : public segmented::CheckerSegment
{
public:
    Checker& checker;
    CIndex* m_idx = nullptr;

    CheckerSegment(Checker& checker, const std::string& relpath)
        : CheckerSegment(checker, relpath, checker.dataset().check_lock_segment(relpath)) {}
    CheckerSegment(Checker& checker, const std::string& relpath, std::shared_ptr<dataset::CheckLock> lock)
        : segmented::CheckerSegment(lock), checker(checker)
    {
        segment = checker.dataset().session->segment_checker(checker.dataset().iseg.format, checker.dataset().path, relpath);
    }
    ~CheckerSegment()
    {
        delete m_idx;
    }

    CIndex& idx()
    {
        if (!m_idx)
            m_idx = new CIndex(checker.m_dataset->iseg, checker.m_dataset, path_relative(), lock);
        return *m_idx;
    }
    std::filesystem::path path_relative() const override { return segment->segment().relpath; }
    const iseg::Dataset& dataset() const override { return checker.dataset(); }
    iseg::Dataset& dataset() override { return checker.dataset(); }
    std::shared_ptr<dataset::archive::Checker> archives() override { return dynamic_pointer_cast<dataset::archive::Checker>(checker.archive()); }

    void get_metadata(std::shared_ptr<core::Lock> lock, metadata::Collection& mds) override
    {
        idx().scan(mds.inserter_func(), "reftime, offset");
    }

    segmented::SegmentState scan(dataset::Reporter& reporter, bool quick=true) override
    {
        if (!segment->exists_on_disk())
            return segmented::SegmentState(segment::SEGMENT_MISSING);

        if (!sys::stat(checker.dataset().path / sys::with_suffix(segment->segment().relpath, ".index")))
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

#if 0
        /**
         * Although iseg could detect if the data of a segment is newer than its
         * index, the timestamp of the index is updated by various kinds of sqlite
         * operations, making the test rather useless, because it's likely that the
         * index timestamp would get updated before the mismatch is detected.
         */
        string abspath = str::joinpath(dataset().path, relpath);
        if (sys::timestamp(abspath) > sys::timestamp(abspath + ".index"))
        {
            segments_state.insert(make_pair(relpath, segmented::SegmentState(segment::SEGMENT_UNALIGNED)));
            return;
        }
#endif

        metadata::Collection mds;
        idx().scan(mds.inserter_func(), "reftime, offset");
        segment::State state(segment::SEGMENT_OK);

        // Compute the span of reftimes inside the segment
        core::Interval segment_interval;
        if (mds.empty())
        {
            reporter.segment_info(checker.name(), segment->segment().relpath, "index knows of this segment but contains no data for it");
            state = segment::SEGMENT_DELETED;
        } else {
            if (!mds.expand_date_range(segment_interval))
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
            state = segment->check([&](const std::string& msg) { reporter.segment_info(checker.name(), segment->segment().relpath, msg); }, mds, quick);

        auto res = segmented::SegmentState(state, segment_interval);
        res.check_age(segment->segment().relpath, checker.dataset(), reporter);
        return res;
    }

    void tar() override
    {
        if (std::filesystem::exists(sys::with_suffix(segment->segment().abspath, ".tar")))
            return;

        auto write_lock = lock->write_lock();
        Pending p = idx().begin_transaction();

        // Rescan file
        metadata::Collection mds;
        idx().scan(mds.inserter_func(), "reftime, offset");

        // Create the .tar segment
        segment = segment->tar(mds);

        // Reindex the new metadata
        idx().reset();
        for (metadata::Collection::const_iterator i = mds.begin(); i != mds.end(); ++i)
        {
            const source::Blob& source = (*i)->sourceBlob();
            if (idx().index(**i, source.offset))
                throw std::runtime_error("duplicate detected while tarring segment");
        }

        // Remove the .metadata file if present, because we are shuffling the
        // data file and it will not be valid anymore
        auto mdpathname = sys::with_suffix(segment->segment().abspath, ".metadata");
        if (std::filesystem::exists(mdpathname))
            if (unlink(mdpathname.c_str()) < 0)
            {
                stringstream ss;
                ss << "cannot remove obsolete metadata file " << mdpathname;
                throw std::system_error(errno, std::system_category(), ss.str());
            }

        // Commit the changes in the database
        p.commit();
    }

    void zip() override
    {
        if (std::filesystem::exists(sys::with_suffix(segment->segment().abspath, ".zip")))
            return;

        auto write_lock = lock->write_lock();
        Pending p = idx().begin_transaction();

        // Rescan file
        metadata::Collection mds;
        idx().scan(mds.inserter_func(), "reftime, offset");

        // Create the .tar segment
        segment = segment->zip(mds);

        // Reindex the new metadata
        idx().reset();
        for (metadata::Collection::const_iterator i = mds.begin(); i != mds.end(); ++i)
        {
            const source::Blob& source = (*i)->sourceBlob();
            if (idx().index(**i, source.offset))
                throw std::runtime_error("duplicate detected while zipping segment");
        }

        // Remove the .metadata file if present, because we are shuffling the
        // data file and it will not be valid anymore
        auto mdpathname = sys::with_suffix(segment->segment().abspath, ".metadata");
        if (std::filesystem::exists(mdpathname))
            if (unlink(mdpathname.c_str()) < 0)
            {
                stringstream ss;
                ss << "cannot remove obsolete metadata file " << mdpathname;
                throw std::system_error(errno, std::system_category(), ss.str());
            }

        // Commit the changes in the database
        p.commit();
    }

    size_t compress(unsigned groupsize) override
    {
        if (std::filesystem::exists(sys::with_suffix(segment->segment().abspath, ".gz")) ||
                std::filesystem::exists(sys::with_suffix(segment->segment().abspath, ".gz.idx")))
            return 0;

        auto write_lock = lock->write_lock();
        Pending p = idx().begin_transaction();

        // Rescan file
        metadata::Collection mds;
        idx().scan(mds.inserter_func(), "reftime, offset");

        // Create the .tar segment
        size_t old_size = segment->size();
        segment = segment->compress(mds, groupsize);
        size_t new_size = segment->size();

        // Reindex the new metadata
        idx().reset();
        for (metadata::Collection::const_iterator i = mds.begin(); i != mds.end(); ++i)
        {
            const source::Blob& source = (*i)->sourceBlob();
            if (idx().index(**i, source.offset))
                throw std::runtime_error("duplicate detected while compressing segment");
        }

        // Remove the .metadata file if present, because we are shuffling the
        // data file and it will not be valid anymore
        auto mdpathname = sys::with_suffix(segment->segment().abspath, ".metadata");
        if (std::filesystem::exists(mdpathname))
            if (unlink(mdpathname.c_str()) < 0)
            {
                stringstream ss;
                ss << "cannot remove obsolete metadata file " << mdpathname;
                throw std::system_error(errno, std::system_category(), ss.str());
            }

        // Commit the changes in the database
        p.commit();

        if (old_size > new_size)
            return old_size - new_size;
        else
            return 0;
    }

    void remove_data(const std::vector<uint64_t>& offsets) override
    {
        if (!segment->exists_on_disk())
            return;

        // Delete from DB, and get file name
        Pending p_del = idx().begin_transaction();

        for (const auto& offset: offsets)
            idx().remove(offset);

        // Commit delete from DB
        p_del.commit();
    }

    size_t repack(unsigned test_flags=0) override
    {
        // Lock away writes and reads
        auto write_lock = lock->write_lock();
        Pending p = idx().begin_transaction();

        metadata::Collection mds;
        idx().scan(mds.inserter_func(), "reftime, offset");

        auto res = reorder_segment_backend(p, mds, test_flags);

        //reporter.operation_progress(checker.name(), "repack", "running VACUUM ANALYZE on segment " + segment->relpath);
        idx().vacuum();

        return res;
    }

    size_t reorder(metadata::Collection& mds, unsigned test_flags) override
    {
        // Lock away writes and reads
        auto write_lock = lock->write_lock();
        Pending p = idx().begin_transaction();

        return reorder_segment_backend(p, mds, test_flags);
    }

    size_t reorder_segment_backend(Pending& p, metadata::Collection& mds, unsigned test_flags)
    {
        // Make a copy of the file with the right data in it, sorted by
        // reftime, and update the offsets in the index
        segment::data::RepackConfig repack_config;
        repack_config.gz_group_size = dataset().gz_group_size;
        repack_config.test_flags = test_flags;
        Pending p_repack = segment->repack(checker.dataset().path, mds, repack_config);

        // Reindex mds
        idx().reset();
        for (metadata::Collection::const_iterator i = mds.begin(); i != mds.end(); ++i)
        {
            const source::Blob& source = (*i)->sourceBlob();
            if (idx().index(**i, source.offset))
                throw std::runtime_error("duplicate detected while reordering segment");
        }

        size_t size_pre = segment->size();

        // Remove the .metadata file if present, because we are shuffling the
        // data file and it will not be valid anymore
        auto mdpathname = sys::with_suffix(segment->segment().abspath, ".metadata");
        if (std::filesystem::exists(mdpathname))
            if (unlink(mdpathname.c_str()) < 0)
            {
                stringstream ss;
                ss << "cannot remove obsolete metadata file " << mdpathname;
                throw std::system_error(errno, std::system_category(), ss.str());
            }

        // Commit the changes in the file system
        p_repack.commit();

        // Commit the changes in the database
        p.commit();

        size_t size_post = segment->size();

        return size_pre - size_post;
    }

    size_t remove(bool with_data) override
    {
        auto idx_abspath = sys::with_suffix(segment->segment().abspath, ".index");
        size_t res = 0;
        if (std::filesystem::exists(idx_abspath))
        {
            res = sys::size(idx_abspath);
            sys::unlink(idx_abspath);
        }
        if (!with_data) return res;
        return res + segment->remove();
    }

    void index(metadata::Collection&& mds) override
    {
        // Add to index
        auto write_lock = lock->write_lock();
        Pending p_idx = idx().begin_transaction();
        for (auto& md: mds)
            if (idx().index(*md, md->sourceBlob().offset))
                throw std::runtime_error("duplicate detected while reindexing segment");
        p_idx.commit();

        // Remove .metadata and .summary files
        std::filesystem::remove(sys::with_suffix(segment->segment().abspath, ".metadata"));
        std::filesystem::remove(sys::with_suffix(segment->segment().abspath, ".summary"));
    }

    void rescan(dataset::Reporter& reporter) override
    {
        metadata::Collection mds;
        segment->rescan_data(
                [&](const std::string& msg) { reporter.segment_info(checker.name(), segment->segment().relpath, msg); },
                lock, mds.inserter_func());

        // Lock away writes and reads
        auto write_lock = lock->write_lock();
        Pending p = idx().begin_transaction();

        // Remove from the index all data about the file
        idx().reset();

        // Scan the list of metadata, looking for duplicates and marking all
        // the duplicates except the last one as deleted
        IDMaker id_maker(idx().unique_codes());

        map<vector<uint8_t>, const Metadata*> finddupes;
        for (const auto& md: mds)
        {
            vector<uint8_t> id = id_maker.make_string(*md);
            if (id.empty())
                continue;
            auto dup = finddupes.find(id);
            if (dup == finddupes.end())
                finddupes.insert(make_pair(id, md.get()));
            else
                dup->second = md.get();
        }

        // Send the remaining metadata to the reindexer
        auto basename = segment->segment().relpath.filename();
        for (const auto& i: finddupes)
        {
            const Metadata& md = *i.second;
            const source::Blob& blob = md.sourceBlob();
            try {
                if (blob.filename.filename() != basename)
                    throw std::runtime_error("cannot rescan " + segment->segment().relpath.native() + ": metadata points to the wrong file: " + blob.filename.native());
                if (std::unique_ptr<types::source::Blob> old = idx().index(md, blob.offset))
                {
                    stringstream ss;
                    ss << "cannot reindex " << basename << ": data item at offset " << blob.offset << " has a duplicate at offset " << old->offset << ": manual fix is required";
                    throw runtime_error(ss.str());
                }
            } catch (std::exception& e) {
                stringstream ss;
                ss << "cannot reindex " << basename << ": failed to reindex data item at offset " << blob.offset << ": " << e.what();
                throw runtime_error(ss.str());
                // sqlite will take care of transaction consistency
            }
        }

        // TODO: if scan fails, remove all info from the index and rename the
        // file to something like .broken

        // Commit the changes on the database
        p.commit();

        // TODO: remove relevant summary
    }

    void release(const std::filesystem::path& new_root, const std::filesystem::path& new_relpath, const std::filesystem::path& new_abspath) override
    {
        std::filesystem::remove(checker.dataset().path / sys::with_suffix(segment->segment().relpath, ".index"));
        segment = segment->move(new_root, new_relpath, new_abspath);
    }
};


Checker::Checker(std::shared_ptr<iseg::Dataset> dataset)
    : DatasetAccess(dataset)
{
    // Create the directory if it does not exist
    std::filesystem::create_directories(dataset->path);
}

std::string Checker::type() const { return "iseg"; }

void Checker::list_segments(std::function<void(const std::filesystem::path& relpath)> dest)
{
    list_segments(Matcher(), dest);
}

void Checker::list_segments(const Matcher& matcher, std::function<void(const std::filesystem::path& relpath)> dest)
{
    std::vector<std::filesystem::path> seg_relpaths;
    step::SegmentQuery squery(dataset().path, dataset().iseg.format, "\\.index$", matcher);
    dataset().step().list_segments(squery, [&](std::filesystem::path&& s) {
        seg_relpaths.emplace_back(move(s));
    });
    std::sort(seg_relpaths.begin(), seg_relpaths.end());
    for (const auto& relpath: seg_relpaths)
        dest(relpath);
}

std::unique_ptr<segmented::CheckerSegment> Checker::segment(const std::filesystem::path& relpath)
{
    return unique_ptr<segmented::CheckerSegment>(new CheckerSegment(*this, relpath));
}

std::unique_ptr<segmented::CheckerSegment> Checker::segment_prelocked(const std::filesystem::path& relpath, std::shared_ptr<dataset::CheckLock> lock)
{
    return unique_ptr<segmented::CheckerSegment>(new CheckerSegment(*this, relpath, lock));
}

void Checker::segments_tracked(std::function<void(segmented::CheckerSegment& segment)> dest)
{
    segments_tracked_filtered(Matcher(), dest);
}

void Checker::segments_tracked_filtered(const Matcher& matcher, std::function<void(segmented::CheckerSegment& segment)> dest)
{
    list_segments(matcher, [&](const std::filesystem::path& relpath) {
        CheckerSegment segment(*this, relpath);
        dest(segment);
    });
}

void Checker::segments_untracked(std::function<void(segmented::CheckerSegment& relpath)> dest)
{
    segments_untracked_filtered(Matcher(), dest);
}

void Checker::segments_untracked_filtered(const Matcher& matcher, std::function<void(segmented::CheckerSegment& segment)> dest)
{
    step::SegmentQuery squery(dataset().path, dataset().iseg.format, matcher);
    dataset().step().list_segments(squery, [&](std::filesystem::path&& relpath) {
        if (sys::stat(dataset().path / sys::with_suffix(relpath, ".index"))) return;
        CheckerSegment segment(*this, relpath);
        // See #279: directory segments that are empty directories are found by
        // a filesystem scan, but are not considered segments
        if (!segment.segment->exists_on_disk())
            return;
        dest(segment);
    });
}

void Checker::check_issue51(CheckerConfig& opts)
{
    if (!opts.online && !dataset().offline) return;
    if (!opts.offline && dataset().offline) return;

    // Broken metadata for each segment
    std::map<std::filesystem::path, metadata::Collection> broken_mds;

    // Iterate all segments
    list_segments([&](const std::filesystem::path& relpath) {
        auto lock = dataset().check_lock_segment(relpath);
        CIndex idx(m_dataset->iseg, m_dataset, relpath, lock);
        metadata::Collection mds;
        idx.scan(mds.inserter_func(), "reftime, offset");
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
            if (blob.format != DataFormat::GRIB && blob.format != DataFormat::BUFR)
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
    // Group mds by segment
    std::unordered_map<std::string, std::vector<uint64_t>> by_segment;
    // Take note of months to invalidate in summary cache
    std::set<std::pair<int, int>> months;

    // Build a todo-list of entries to delete for each segment
    for (const auto& md: mds)
    {
        const types::source::Blob* source = md->has_source_blob();
        if (!source)
            throw std::runtime_error("cannot remove metadata from dataset, because it has no Blob source");

        if (source->basedir != dataset().path)
            throw std::runtime_error("cannot remove metadata from dataset: its basedir is " + source->basedir.native() + " but this dataset is at " + dataset().path.native());

        Time time = md->get<types::reftime::Position>()->get_Position();
        auto relpath = sys::with_suffix(dataset().step()(time), "."s + format_name(dataset().iseg.format));

        if (!Segment::is_segment(dataset().path / relpath))
            continue;

        by_segment[relpath].push_back(source->offset);
        months.insert(std::make_pair(time.ye, time.mo));
    }

    for (const auto& i: by_segment)
    {
        segment::data::WriterConfig writer_config;
        writer_config.drop_cached_data_on_commit = false;
        writer_config.eatmydata = dataset().eatmydata;

        auto seg = segment(i.first);
        seg->remove_data(i.second);
        arki::nag::verbose("%s: %s: %zu data marked as deleted", name().c_str(), i.first.c_str(), i.second.size());
    }

    index::SummaryCache scache(dataset().summary_cache_pathname);
    for (const auto& i: months)
        scache.invalidate(i.first, i.second);
}

size_t Checker::vacuum(dataset::Reporter& reporter)
{
    return 0;
}

void Checker::test_make_overlap(const std::filesystem::path& relpath, unsigned overlap_size, unsigned data_idx)
{
    auto lock = dataset().check_lock_segment(relpath);
    auto wrlock = lock->write_lock();
    CIndex idx(m_dataset->iseg, m_dataset, relpath, lock);
    metadata::Collection mds;
    idx.query_segment(mds.inserter_func());
    dataset().session->segment_checker(dataset().iseg.format, dataset().path, relpath)->test_make_overlap(mds, overlap_size, data_idx);
    idx.test_make_overlap(overlap_size, data_idx);
}

void Checker::test_make_hole(const std::filesystem::path& relpath, unsigned hole_size, unsigned data_idx)
{
    auto lock = dataset().check_lock_segment(relpath);
    auto wrlock = lock->write_lock();
    CIndex idx(m_dataset->iseg, m_dataset, relpath, lock);
    metadata::Collection mds;
    idx.query_segment(mds.inserter_func());
    dataset().session->segment_checker(dataset().iseg.format, dataset().path, relpath)->test_make_hole(mds, hole_size, data_idx);
    idx.test_make_hole(hole_size, data_idx);
}

void Checker::test_corrupt_data(const std::filesystem::path& relpath, unsigned data_idx)
{
    auto lock = dataset().check_lock_segment(relpath);
    auto wrlock = lock->write_lock();
    CIndex idx(m_dataset->iseg, m_dataset, relpath, lock);
    metadata::Collection mds;
    idx.query_segment(mds.inserter_func());
    dataset().session->segment_checker(dataset().iseg.format, dataset().path, relpath)->test_corrupt(mds, data_idx);
}

void Checker::test_truncate_data(const std::filesystem::path& relpath, unsigned data_idx)
{
    auto lock = dataset().check_lock_segment(relpath);
    auto wrlock = lock->write_lock();
    CIndex idx(m_dataset->iseg, m_dataset, relpath, lock);
    metadata::Collection mds;
    idx.query_segment(mds.inserter_func());
    dataset().session->segment_checker(dataset().iseg.format, dataset().path, relpath)->test_truncate_by_data(mds, data_idx);
}

void Checker::test_swap_data(const std::filesystem::path& relpath, unsigned d1_idx, unsigned d2_idx)
{
    auto lock = dataset().check_lock_segment(relpath);
    metadata::Collection mds;
    {
        CIndex idx(m_dataset->iseg, m_dataset, relpath, lock);
        idx.scan(mds.inserter_func(), "offset");
        mds.swap(d1_idx, d2_idx);
    }
    segment_prelocked(relpath, lock)->reorder(mds);
}

void Checker::test_rename(const std::filesystem::path& relpath, const std::filesystem::path& new_relpath)
{
    auto lock = dataset().check_lock_segment(relpath);
    auto wrlock = lock->write_lock();

    auto abspath = dataset().path / relpath;
    auto new_abspath = dataset().path / new_relpath;

    auto segment = dataset().session->segment_checker(dataset().iseg.format, dataset().path, relpath);
    segment->move(dataset().path, new_relpath, new_abspath);

    std::filesystem::rename(
            sys::with_suffix(abspath, ".index"),
            sys::with_suffix(new_abspath, ".index"));

}

std::shared_ptr<Metadata> Checker::test_change_metadata(const std::filesystem::path& relpath, std::shared_ptr<Metadata> md, unsigned data_idx)
{
    auto lock = dataset().check_lock_segment(relpath);
    auto wrlock = lock->write_lock();
    CIndex idx(m_dataset->iseg, m_dataset, relpath, lock);
    metadata::Collection mds;
    idx.query_segment(mds.inserter_func());
    md->set_source(std::unique_ptr<arki::types::Source>(mds[data_idx].source().clone()));
    md->sourceBlob().unlock();
    mds.replace(data_idx, md);

    // Reindex mds
    idx.reset();
    for (auto& m: mds)
    {
        const source::Blob& source = m->sourceBlob();
        if (idx.index(*m, source.offset))
            throw std::runtime_error("duplicate detected in test_change_metadata");
    }

    return mds.get(data_idx);
}

void Checker::test_delete_from_index(const std::filesystem::path& relpath)
{
    auto lock = dataset().check_lock_segment(relpath);
    auto wrlock = lock->write_lock();
    CIndex idx(m_dataset->iseg, m_dataset, relpath, lock);
    idx.reset();
}

void Checker::test_invalidate_in_index(const std::filesystem::path& relpath)
{
    std::filesystem::remove(dataset().path / sys::with_suffix(relpath, ".index"));
}

}
}
}

