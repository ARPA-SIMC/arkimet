#include "metadata.h"
#include "arki/core/lock.h"
#include "arki/metadata.h"
#include "arki/metadata/inbound.h"
#include "arki/metadata/reader.h"
#include "arki/query.h"
#include "arki/types/source/blob.h"
#include "arki/utils/sys.h"
#include "data.h"
#include "reporter.h"

using namespace arki::utils;

namespace arki::segment::metadata {

namespace {

/**
 * This is unsafe for use in uncontrolled paths.
 *
 * Here however we are operating inside datasets, and we can afford a
 * predictable ending.
 */
std::filesystem::path path_tmp(const std::filesystem::path& path)
{
    auto res(path);
    res += ".tmp";
    return res;
}

struct AtomicWriterWithSummary
{
    const Segment& segment;
    Summary sum;
    sys::File out_md;
    sys::File out_sum;

    explicit AtomicWriterWithSummary(const Segment& segment)
        : segment(segment), out_md(path_tmp(segment.abspath_metadata()),
                                   O_WRONLY | O_TRUNC | O_CREAT | O_EXCL, 0666),
          out_sum(path_tmp(segment.abspath_summary()),
                  O_WRONLY | O_TRUNC | O_CREAT | O_EXCL, 0666)
    {
    }

    ~AtomicWriterWithSummary() { rollback(); }

    void write(arki::metadata::Collection& mds)
    {
        mds.prepare_for_segment_metadata();
        mds.add_to_summary(sum);
        std::vector<uint8_t> sum_encoded = sum.encode(true);
        // TODO: is this tracing still needed?
        // iotrace::trace_file(segment.abspath_summary(), 0, sum_encoded.size(),
        // "write summary");

        mds.write_to(out_md);
        out_sum.write_all_or_retry(sum_encoded);

        // Syncronize metadata and summary timestamps
        struct stat st_md;
        out_md.fstat(st_md);
        ::timespec ts_md[2];
        ts_md[0] = st_md.st_atim;
        ts_md[1] = st_md.st_mtim;

        out_sum.futimens(ts_md);
    }

    void commit()
    {
        if (out_md)
        {
            out_md.close();
            std::filesystem::rename(out_md.path(), segment.abspath_metadata());
        }
        if (out_sum)
        {
            out_sum.close();
            std::filesystem::rename(out_sum.path(), segment.abspath_summary());
        }
    }

    void rollback()
    {
        if (out_md)
        {
            out_md.close();
            ::unlink(out_md.path().c_str());
        }
        if (out_sum)
        {
            out_sum.close();
            ::unlink(out_sum.path().c_str());
        }
    }
};

} // namespace

Index::Index(const Segment& segment)
    : segment(segment), md_path(segment.abspath_metadata())
{
}

bool Index::read_all(std::shared_ptr<arki::segment::Reader> reader,
                     metadata_dest_func dest)
{
    core::File in(md_path, O_RDONLY);
    arki::metadata::BinaryReader md_reader(in, segment.root());
    // This generates filenames relative to the metadata
    // We need to use m_path as the dirname, and prepend dirname(*i) to the
    // filenames
    return md_reader.read_all([&](std::shared_ptr<Metadata> md) {
        // TODO: if metadata has VALUE (using smallfiles) there is no need to
        // lock its source

        // Tweak Blob sources replacing basedir and prepending a directory to
        // the file name
        if (const types::source::Blob* s = md->has_source_blob())
        {
            if (reader)
                md->set_source(types::Source::createBlob(
                    segment.format(), segment.root(), segment.relpath(),
                    s->offset, s->size, reader));
            else
                md->set_source(types::Source::createBlobUnlocked(
                    segment.format(), segment.root(), segment.relpath(),
                    s->offset, s->size));
        }
        return dest(md);
    });
}

arki::metadata::Collection
Index::query_data(const Matcher& matcher,
                  std::shared_ptr<arki::segment::Reader> reader)
{
    arki::metadata::Collection res;

    core::File in(md_path, O_RDONLY);
    arki::metadata::BinaryReader md_reader(in, segment.root());

    // This generates filenames relative to the metadata
    // We need to use m_path as the dirname, and prepend dirname(*i) to the
    // filenames
    md_reader.read_all([&](std::shared_ptr<Metadata> md) {
        // Filter using the matcher in the query
        if (!matcher(*md))
            return true;

        // TODO: if metadata has VALUE (using smallfiles) there is no need to
        // lock its source

        // Tweak Blob sources replacing basedir and prepending a directory to
        // the file name
        if (const types::source::Blob* s = md->has_source_blob())
        {
            if (reader)
                md->set_source(types::Source::createBlob(
                    segment.format(), segment.root(), segment.relpath(),
                    s->offset, s->size, reader));
            else
                md->set_source(types::Source::createBlobUnlocked(
                    segment.format(), segment.root(), segment.relpath(),
                    s->offset, s->size));
        }
        res.acquire(std::move(md));
        return true;
    });

    return res;
}

void Index::query_summary(const Matcher& matcher, Summary& summary)
{
    core::File in(md_path, O_RDONLY);
    arki::metadata::BinaryReader md_reader(in, segment.root());
    md_reader.read_all([&](std::shared_ptr<Metadata> md) {
        if (!matcher(*md))
            return true;
        summary.add(*md);
        return true;
    });
}

Reader::Reader(std::shared_ptr<const Segment> segment,
               std::shared_ptr<const core::ReadLock> lock)
    : segment::Reader(segment, lock),
      data_reader(segment->data()->reader(lock)), index(*segment)
{
}

Reader::~Reader() {}

std::vector<uint8_t> Reader::read(const types::source::Blob& src)
{
    return data_reader->read(src);
}
stream::SendResult Reader::stream(const types::source::Blob& src,
                                  StreamOutput& out)
{
    return data_reader->stream(src, out);
}

bool Reader::read_all(metadata_dest_func dest)
{
    auto reader = m_segment->reader(lock);
    return index.read_all(reader, dest);
}

bool Reader::query_data(const query::Data& q, metadata_dest_func dest)
{
    std::shared_ptr<arki::segment::Reader> reader;
    if (q.with_data)
        reader = m_segment->reader(lock);

    auto mdbuf = index.query_data(q.matcher, reader);

    // Sort and output the rest
    if (q.sorter)
        mdbuf.sort(*q.sorter);

    // pass it to consumer
    return mdbuf.move_to(dest);
}

void Reader::query_summary(const Matcher& matcher, Summary& summary)
{
    auto summary_path = m_segment->abspath_summary();
    if (sys::access(summary_path, R_OK))
    {
        Summary s;
        s.read_file(summary_path);
        s.filter(matcher, summary);
    }
    else
    {
        // Resummarize from metadata
        index.query_summary(matcher, summary);
    }
}

/*
 * Writer
 */

Writer::Writer(std::shared_ptr<const Segment> segment,
               std::shared_ptr<core::AppendLock> lock)
    : segment::Writer(segment, lock)
{
    // Read the metadata
    auto reader = segment->reader(lock);
    reader->read_all(mds.inserter_func());

    // Read the summary
    if (!mds.empty())
        mds.add_to_summary(sum);
}

Writer::~Writer() {}

std::shared_ptr<segment::data::Writer>
Writer::get_data_writer(const segment::WriterConfig& config) const
{
    std::filesystem::create_directories(m_segment->abspath().parent_path());
    auto data = m_segment->data();
    return data->writer(config);
}

void Writer::add(const Metadata& md, const types::source::Blob& source)
{
    using namespace arki::types;

    // Replace the pathname with its basename
    auto copy(md.clone());
    if (!segment().session().smallfiles)
        copy->unset(TYPE_VALUE);
    copy->set_source(Source::createBlobUnlocked(source.format, segment().root(),
                                                segment().relpath(),
                                                source.offset, source.size));
    sum.add(*copy);
    mds.acquire(std::move(copy));
}

void Writer::write_metadata()
{
    auto path_md  = segment().abspath_metadata();
    auto path_sum = segment().abspath_summary();

    mds.prepare_for_segment_metadata();
    mds.writeAtomically(path_md);
    sum.writeAtomically(path_sum);
}

Writer::AcquireResult Writer::acquire(arki::metadata::InboundBatch& batch,
                                      const WriterConfig& config)
{
    auto data_writer = get_data_writer(config);
    try
    {
        for (auto& e : batch)
        {
            e->destination.clear();
            const types::source::Blob& new_source = data_writer->append(*e->md);
            add(*e->md, new_source);
            e->result      = arki::metadata::Inbound::Result::OK;
            e->destination = config.destination_name;
        }
    }
    catch (std::exception& e)
    {
        // returning here before commit means no action is taken
        batch.set_all_error("Failed to store in '" + config.destination_name +
                            "': " + e.what());
        AcquireResult res;
        res.count_ok      = 0;
        res.count_failed  = batch.size();
        res.segment_mtime = segment().data()->timestamp().value_or(0);
        if (sum.empty())
            res.data_timespan = core::Interval();
        else
            res.data_timespan = sum.get_reference_time();
        return res;
    }

    data_writer->commit();
    write_metadata();

    auto ts = segment().data()->timestamp();
    if (!ts)
        throw std::runtime_error(segment().abspath().native() +
                                 ": segment not found after importing");

    // Synchronize summary and metadata timestamps.
    // This is not normally needed, as the files are written and flushed in
    // the correct order.
    //
    // When running with filesystem sync disabled (eatmydata or
    // systemd-nspawn --suppress-sync) however there are cases where the
    // summary timestamp ends up one second earlier than the metadata
    // timestamp, which then gets flagged by a dataset check
    sys::touch(segment().abspath_metadata(), ts.value());
    sys::touch(segment().abspath_summary(), ts.value());

    AcquireResult res;
    res.count_ok      = batch.size();
    res.count_failed  = 0;
    res.segment_mtime = ts.value();
    res.data_timespan = sum.get_reference_time();
    return res;
}

/*
 * Checker
 */

arki::metadata::Collection Checker::scan()
{
    arki::metadata::Collection res;

    auto md_abspath = m_segment->abspath_metadata();
    if (auto st_md = sys::stat(md_abspath))
    {
        if (auto data_ts = m_data->timestamp())
        {
            // Metadata exists and it looks new enough: use it
            auto reader = m_segment->reader(lock);
            std::filesystem::path basedir(m_segment->abspath().parent_path());
            sys::File in(md_abspath, O_RDONLY);
            arki::metadata::BinaryReader md_reader(in, basedir);
            md_reader.read_all([&](std::shared_ptr<Metadata> md) {
                const auto& source = md->sourceBlob();
                md->set_source(types::Source::createBlob(
                    segment().format(), segment().root(), segment().relpath(),
                    source.offset, source.size, reader));
                res.acquire(md);
                return true;
            });
        }
        else
        {
            std::stringstream buf;
            buf << m_segment->abspath()
                << ": cannot scan segment since its data is missing";
            throw std::runtime_error(buf.str());
        }
    }
    else
    {
        // Rescan the file
        auto data_reader = m_data->reader(lock);
        data_reader->scan_data([&](std::shared_ptr<Metadata> md) {
            res.acquire(md);
            return true;
        });
    }

    return res;
}

Checker::FsckResult Checker::fsck(segment::Reporter& reporter, bool quick)
{
    Checker::FsckResult res;

    auto ts_data = m_data->timestamp();
    if (!ts_data)
    {
        reporter.info(segment(), "segment data not found on disk");
        res.state = SEGMENT_MISSING;
        return res;
    }
    res.mtime = ts_data.value();
    res.size  = data().size();

    time_t ts_md = sys::timestamp(segment().abspath_metadata(), 0);
    if (!ts_md)
    {
        // Data not found on disk
        if (data().is_empty())
        {
            reporter.info(
                segment(),
                "empty segment found on disk with no associated metadata");
            res.state = SEGMENT_DELETED;
        }
        else
        {
            reporter.info(segment(),
                          "segment found on disk with no associated metadata");
            res.state = SEGMENT_UNALIGNED;
        }
        return res;
    }

    if (ts_md < ts_data.value())
    {
        reporter.info(segment(), "data is newer than metadata");
        res.state = segment::SEGMENT_UNALIGNED;
        return res;
    }

    auto mds = scan();

    if (mds.empty())
    {
        reporter.info(segment(),
                      "metadata reports that the segment is fully deleted");
        res.state += SEGMENT_DELETED;
    }
    else
    {
        // Compute the span of reftimes inside the segment
        mds.sort_segment();
        if (!mds.expand_date_range(res.interval))
        {
            reporter.info(segment(), "metadata contains data for this segment "
                                     "but no reference time information");
            res.state += SEGMENT_CORRUPTED;
        }
        else
        {
            auto data_checker = m_data->checker();
            res.state += data_checker->check(
                [&](const std::string& msg) { reporter.info(segment(), msg); },
                mds, quick);
        }

        time_t ts_sum = sys::timestamp(segment().abspath_summary(), 0);
        if (ts_sum < ts_md)
        {
            std::stringstream buf;
            buf << "metadata (ts:" << ts_md
                << ") is newer than summary (ts:" << ts_sum << ")";
            reporter.info(segment(), buf.str());
            res.state += segment::SEGMENT_UNOPTIMIZED;
        }
    }

    return res;
}

bool Checker::scan_data(segment::Reporter& reporter, metadata_dest_func dest)
{
    auto data_checker = m_data->checker();
    return data_checker->rescan_data(
        [&](const std::string& message) { reporter.info(segment(), message); },
        lock, dest);
}

std::shared_ptr<segment::Fixer> Checker::fixer()
{
    return std::make_shared<Fixer>(shared_from_this(), lock->write_lock());
}

Fixer::MarkRemovedResult Fixer::mark_removed(const std::set<uint64_t>& offsets)
{
    MarkRemovedResult res;

    // Load current metadata
    auto mds = checker().scan();

    // Remove matching offsets
    mds = mds.without_data(offsets);

    if (mds.empty())
    {
        auto path_metadata = segment().abspath_metadata();
        auto path_summary  = segment().abspath_summary();
        mds.writeAtomically(path_metadata);
        std::filesystem::remove(segment().abspath_summary());
        res.data_timespan = core::Interval();
    }
    else
    {
        // Write out the new metadata
        AtomicWriterWithSummary writer(segment());
        writer.write(mds);
        writer.commit();
        res.data_timespan = writer.sum.get_reference_time();
    }
    res.segment_mtime = get_data_mtime_after_fix("removal in metadata");
    return res;
}

Fixer::ReorderResult Fixer::reorder(arki::metadata::Collection& mds,
                                    const segment::RepackConfig& repack_config)
{
    ReorderResult res;
    auto path_metadata = segment().abspath_metadata();
    auto path_summary  = segment().abspath_summary();
    res.size_pre       = data().size();

    // Write out the data with the new order
    auto data_checker = data().checker();
    auto p_repack     = data_checker->repack(mds, repack_config);

    // Remove existing cached metadata, since we scramble their order
    std::filesystem::remove(path_metadata);

    p_repack.commit();

    res.size_post = data().size();

    // Write out the new metadata
    mds.prepare_for_segment_metadata();
    mds.writeAtomically(path_metadata);

    res.segment_mtime = get_data_mtime_after_fix("reorder");

    // Sync timestamps. We need it for the summary since it is now older than
    // the other files, and since we read the segment data timestamp we can
    // sync it to all extra files
    sys::touch_ifexists(path_metadata, res.segment_mtime);
    sys::touch_ifexists(path_summary, res.segment_mtime);

    return res;
}

size_t Fixer::remove(bool with_data)
{
    size_t res = 0;
    res += remove_ifexists(segment().abspath_metadata());
    res += remove_ifexists(segment().abspath_summary());
    if (!with_data)
        return res;

    return res + remove_data();
}

size_t Fixer::remove_data()
{
    auto data_checker = data().checker();
    return data_checker->remove();
}

Fixer::ConvertResult Fixer::tar()
{
    ConvertResult res;
    if (std::filesystem::exists(sys::with_suffix(segment().abspath(), ".tar")))
    {
        auto ts = data().timestamp();
        if (!ts)
        {
            std::stringstream buf;
            buf << segment().abspath()
                << ": tar segment already exists but cannot be accessed";
            throw std::runtime_error(buf.str());
        }
        res.segment_mtime = ts.value();
        return res;
    }
    res.size_pre = data().size();

    auto path_metadata = segment().abspath_metadata();
    auto path_summary  = segment().abspath_summary();
    auto data_checker  = data().checker();

    // Rescan file and sort for repacking
    auto mds = checker().scan();
    mds.sort_segment();

    std::filesystem::remove(path_metadata);

    // Create the .tar segment
    auto new_data_checker = data_checker->tar(mds);
    res.size_post         = new_data_checker->data().size();

    // Write out the new metadata
    mds.prepare_for_segment_metadata();
    mds.writeAtomically(path_metadata);

    checker().update_data();
    res.segment_mtime = get_data_mtime_after_fix("conversion to tar");

    // The summary is unchanged: touch it to confirm it as still valid
    sys::touch(path_summary, res.segment_mtime);

    return res;
}

Fixer::ConvertResult Fixer::zip()
{
    ConvertResult res;
    if (std::filesystem::exists(sys::with_suffix(segment().abspath(), ".zip")))
    {
        auto ts = data().timestamp();
        if (!ts)
        {
            std::stringstream buf;
            buf << segment().abspath()
                << ": zip segment already exists but cannot be accessed";
            throw std::runtime_error(buf.str());
        }
        res.segment_mtime = ts.value();
        return res;
    }
    res.size_pre = data().size();

    auto path_metadata = segment().abspath_metadata();
    auto path_summary  = segment().abspath_summary();
    auto data_checker  = data().checker();

    // Rescan file and sort for repacking
    auto mds = checker().scan();
    mds.sort_segment();

    std::filesystem::remove(path_metadata);

    // Create the .zip segment
    auto new_data_checker = data_checker->zip(mds);
    res.size_post         = new_data_checker->data().size();

    // Write out the new metadata
    mds.prepare_for_segment_metadata();
    mds.writeAtomically(path_metadata);

    checker().update_data();
    res.segment_mtime = get_data_mtime_after_fix("conversion to zip");

    // The summary is unchanged: touch it to confirm it as still valid
    sys::touch(path_summary, res.segment_mtime);

    return res;
}

Fixer::ConvertResult Fixer::compress(unsigned groupsize)
{
    ConvertResult res;
    if (std::filesystem::exists(sys::with_suffix(segment().abspath(), ".gz")) or
        std::filesystem::exists(
            sys::with_suffix(segment().abspath(), ".gz.idx")))
    {
        auto ts = data().timestamp();
        if (!ts)
        {
            std::stringstream buf;
            buf << segment().abspath()
                << ": gz segment already exists but cannot be accessed";
            throw std::runtime_error(buf.str());
        }
        res.segment_mtime = ts.value();
        return res;
    }
    res.size_pre = data().size();

    auto path_metadata = segment().abspath_metadata();
    auto path_summary  = segment().abspath_summary();
    auto data_checker  = data().checker();

    // Rescan file and sort for repacking
    auto mds = checker().scan();
    mds.sort_segment();

    std::filesystem::remove(path_metadata);

    // Create the .zip segment
    auto new_data_checker = data_checker->compress(mds, groupsize);
    res.size_post         = new_data_checker->data().size();

    // Write out the new metadata
    mds.prepare_for_segment_metadata();
    mds.writeAtomically(path_metadata);

    checker().update_data();
    res.segment_mtime = get_data_mtime_after_fix("conversion to gz");

    // The summary is unchanged: touch it to confirm it as still valid
    sys::touch(path_summary, res.segment_mtime);

    return res;
}

void Fixer::reindex(arki::metadata::Collection& mds)
{
    AtomicWriterWithSummary writer(segment());
    writer.write(mds);
    writer.commit();
}

void Fixer::move(std::shared_ptr<arki::Segment> dest)
{
    segment::Fixer::move(dest);
    sys::rename_ifexists(segment().abspath_metadata(),
                         dest->abspath_metadata());
    sys::rename_ifexists(segment().abspath_summary(), dest->abspath_summary());
}

void Fixer::test_touch_contents(time_t timestamp)
{
    segment::Fixer::test_touch_contents(timestamp);
    sys::touch(segment().abspath_metadata(), timestamp);
    sys::touch(segment().abspath_summary(), timestamp);
}

void Fixer::test_mark_all_removed()
{
    arki::metadata::Collection().writeAtomicallyPreservingTimestamp(
        segment().abspath_metadata());
    std::filesystem::remove(segment().abspath_summary());
}

void Fixer::test_make_overlap(unsigned overlap_size, unsigned data_idx)
{
    auto mds          = checker().scan();
    auto data_checker = data().checker();
    data_checker->test_make_overlap(mds, overlap_size, data_idx);

    auto pathname = segment().abspath_metadata();
    utils::files::PreserveFileTimes pf(pathname);
    sys::File fd(pathname, O_RDWR);
    fd.lseek(0);
    mds.write_to(fd);
    fd.ftruncate(fd.lseek(0, SEEK_CUR));
}

void Fixer::test_make_hole(unsigned hole_size, unsigned data_idx)
{
    auto mds          = checker().scan();
    auto data_checker = data().checker();
    data_checker->test_make_hole(mds, hole_size, data_idx);

    auto pathname = segment().abspath_metadata();
    utils::files::PreserveFileTimes pf(pathname);
    {
        sys::File fd(pathname, O_RDWR);
        fd.lseek(0);
        mds.prepare_for_segment_metadata();
        mds.write_to(fd);
        fd.ftruncate(fd.lseek(0, SEEK_CUR));
        fd.close();
    }
}

} // namespace arki::segment::metadata
