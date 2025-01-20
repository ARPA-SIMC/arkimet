#include "metadata.h"
#include "data.h"
#include "arki/query.h"
#include "arki/core/lock.h"
#include "arki/utils/sys.h"
#include "arki/metadata.h"
#include "arki/metadata/collection.h"
#include "arki/types/source/blob.h"

using namespace arki::utils;

namespace arki::segment::metadata {

Index::Index(const Segment& segment)
    : segment(segment), md_path(segment.abspath_metadata())
{
}

bool Index::read_all(std::shared_ptr<arki::segment::data::Reader> reader, metadata_dest_func dest)
{
    // This generates filenames relative to the metadata
    // We need to use m_path as the dirname, and prepend dirname(*i) to the filenames
    return Metadata::read_file(md_path, [&](std::shared_ptr<Metadata> md) {
        // TODO: if metadata has VALUE (using smallfiles) there is no need to lock its source

        // Tweak Blob sources replacing basedir and prepending a directory to the file name
        if (const types::source::Blob* s = md->has_source_blob())
        {
            if (reader)
                md->set_source(types::Source::createBlob(segment.format(), segment.root(), segment.relpath(), s->offset, s->size, reader));
            else
                md->set_source(types::Source::createBlobUnlocked(segment.format(), segment.root(), segment.relpath(), s->offset, s->size));
        }
        return dest(md);
    });
}

arki::metadata::Collection Index::query_data(const Matcher& matcher, std::shared_ptr<arki::segment::data::Reader> reader)
{
    arki::metadata::Collection res;

    // This generates filenames relative to the metadata
    // We need to use m_path as the dirname, and prepend dirname(*i) to the filenames
    Metadata::read_file(md_path, [&](std::shared_ptr<Metadata> md) {
        // Filter using the matcher in the query
        if (!matcher(*md)) return true;

        // TODO: if metadata has VALUE (using smallfiles) there is no need to lock its source

        // Tweak Blob sources replacing basedir and prepending a directory to the file name
        if (const types::source::Blob* s = md->has_source_blob())
        {
            if (reader)
                md->set_source(types::Source::createBlob(segment.format(), segment.root(), segment.relpath(), s->offset, s->size, reader));
            else
                md->set_source(types::Source::createBlobUnlocked(segment.format(), segment.root(), segment.relpath(), s->offset, s->size));
        }
        res.acquire(std::move(md));
        return true;
    });

    return res;
}

void Index::query_summary(const Matcher& matcher, Summary& summary)
{
    Metadata::read_file(md_path, [&](std::shared_ptr<Metadata> md) {
        if (!matcher(*md))
            return true;
        summary.add(*md);
        return true;
    });
}

Reader::Reader(std::shared_ptr<const Segment> segment, std::shared_ptr<const core::ReadLock> lock)
    : segment::Reader(segment, lock), index(*segment)
{
}

Reader::~Reader()
{
}

bool Reader::read_all(metadata_dest_func dest)
{
    auto reader = m_segment->session().segment_data_reader(m_segment, lock);
    return index.read_all(reader, dest);
}

bool Reader::query_data(const query::Data& q, metadata_dest_func dest)
{
    std::shared_ptr<arki::segment::data::Reader> reader;
    if (q.with_data)
        reader = m_segment->session().segment_data_reader(m_segment, lock);

    auto mdbuf = index.query_data(q.matcher, reader);

    // Sort and output the rest
    if (q.sorter) mdbuf.sort(*q.sorter);

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
    } else {
        // Resummarize from metadata
        index.query_summary(matcher, summary);
    }
}


arki::metadata::Collection Checker::scan()
{
    arki::metadata::Collection res;

    auto md_abspath = m_segment->abspath_metadata();
    if (auto st_md = sys::stat(md_abspath))
    {
        if (auto data_ts = m_data->timestamp())
        {
            // Metadata exists and it looks new enough: use it
            auto data_reader = m_data->reader(lock);
            std::filesystem::path root(m_segment->abspath().parent_path());
            arki::Metadata::read_file(arki::metadata::ReadContext(md_abspath, root), [&](std::shared_ptr<Metadata> md) {
                md->sourceBlob().lock(data_reader);
                res.acquire(md);
                return true;
            });
        } else {
            std::stringstream buf;
            buf << m_segment->abspath() << ": cannot scan segment since its data is missing";
            throw std::runtime_error(buf.str());
        }
    } else {
        // Rescan the file
        auto data_reader = m_data->reader(lock);
        data_reader->scan_data([&](std::shared_ptr<Metadata> md) {
            res.acquire(md);
            return true;
        });
    }

    return res;
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

    auto path_metadata = segment().abspath_metadata();
    auto path_summary = segment().abspath_summary();
    if (mds.empty())
    {
        mds.writeAtomically(path_metadata);
        std::filesystem::remove(segment().abspath_summary());
        res.data_timespan = core::Interval();
    } else {

        Summary sum;
        mds.add_to_summary(sum);

        // Write out the new metadata
        mds.prepare_for_segment_metadata();
        mds.writeAtomically(path_metadata);
        sum.writeAtomically(path_summary);

        res.data_timespan = sum.get_reference_time();
    }
    res.segment_mtime = get_data_mtime_after_fix("removal in metadata");
    return res;
}

Fixer::ReorderResult Fixer::reorder(arki::metadata::Collection& mds, const segment::data::RepackConfig& repack_config)
{
    ReorderResult res;
    auto path_metadata = segment().abspath_metadata();
    auto path_summary = segment().abspath_summary();

    // Write out the data with the new order
    auto data_checker = data().checker(false);
    auto p_repack = data_checker->repack(mds, repack_config);

    // Remove existing cached metadata, since we scramble their order
    std::filesystem::remove(path_metadata);

    res.size_pre = data_checker->size();

    p_repack.commit();

    res.size_post = data_checker->size();

    // Write out the new metadata
    mds.prepare_for_segment_metadata();
    mds.writeAtomically(path_metadata);

    res.segment_mtime = get_data_mtime_after_fix("reorder");

    // Sync timestamps. We need it for the summary since it is now older than
    // the other files, and since we read the segment data timestamp we can
    // sync it to all extra files
    sys::touch(path_metadata, res.segment_mtime);
    sys::touch(path_summary, res.segment_mtime);

    return res;
}

size_t Fixer::remove(bool with_data)
{
    size_t res = 0;
    res += remove_ifexists(segment().abspath_metadata());
    res += remove_ifexists(segment().abspath_summary());
    if (!with_data)
        return res;

    auto data_checker = data().checker(false);
    return res + data_checker->remove();
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
            buf << segment().abspath() << ": tar segment already exists but cannot be accessed";
            throw std::runtime_error(buf.str());
        }
        res.segment_mtime = ts.value();
        return res;
    }

    auto path_metadata = segment().abspath_metadata();
    auto path_summary = segment().abspath_summary();

    auto data_checker = data().checker(false);
    res.size_pre = data_checker->size();

    // Rescan file and sort for repacking
    auto mds = checker().scan();
    mds.sort();

    std::filesystem::remove(path_metadata);

    // Create the .tar segment
    auto new_data_checker = data_checker->tar(mds);
    res.size_post = new_data_checker->size();

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
            buf << segment().abspath() << ": zip segment already exists but cannot be accessed";
            throw std::runtime_error(buf.str());
        }
        res.segment_mtime = ts.value();
        return res;
    }

    auto path_metadata = segment().abspath_metadata();
    auto path_summary = segment().abspath_summary();

    auto data_checker = data().checker(false);
    res.size_pre = data_checker->size();

    // Rescan file and sort for repacking
    auto mds = checker().scan();
    mds.sort();

    std::filesystem::remove(path_metadata);

    // Create the .zip segment
    auto new_data_checker = data_checker->zip(mds);
    res.size_post = new_data_checker->size();

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
    if (std::filesystem::exists(sys::with_suffix(segment().abspath(), ".gz"))
                or std::filesystem::exists(sys::with_suffix(segment().abspath(), ".gz.idx")))
    {
        auto ts = data().timestamp();
        if (!ts)
        {
            std::stringstream buf;
            buf << segment().abspath() << ": gz segment already exists but cannot be accessed";
            throw std::runtime_error(buf.str());
        }
        res.segment_mtime = ts.value();
        return res;
    }

    auto path_metadata = segment().abspath_metadata();
    auto path_summary = segment().abspath_summary();

    auto data_checker = data().checker(false);
    res.size_pre = data_checker->size();

    // Rescan file and sort for repacking
    auto mds = checker().scan();
    mds.sort();

    std::filesystem::remove(path_metadata);

    // Create the .zip segment
    auto new_data_checker = data_checker->compress(mds, groupsize);
    res.size_post = new_data_checker->size();

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
    Summary sum;
    mds.add_to_summary(sum);
    mds.prepare_for_segment_metadata();
    mds.writeAtomically(segment().abspath_metadata());
    sum.writeAtomically(segment().abspath_summary());
}

}
