#include "metadata.h"
#include "data.h"
#include "arki/query.h"
#include "arki/utils/sys.h"
#include "arki/metadata.h"
#include "arki/metadata/collection.h"
#include "arki/types/source/blob.h"

using namespace arki::utils;

namespace arki::segment::metadata {

Index::Index(const Segment& segment, const std::filesystem::path& md_path)
    : segment(segment), md_path(md_path)
{
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
    : segment::Reader(segment, lock), index(*segment, sys::with_suffix(segment->abspath(), ".metadata"))
{
}

bool Reader::query_data(const query::Data& q, metadata_dest_func dest)
{
    std::shared_ptr<arki::segment::data::Reader> reader;
    if (q.with_data)
        reader = m_segment->session().segment_data_reader(m_segment->format(), m_segment->relpath(), lock);

    auto mdbuf = index.query_data(q.matcher, reader);

    // Sort and output the rest
    if (q.sorter) mdbuf.sort(*q.sorter);

    // pass it to consumer
    return mdbuf.move_to(dest);
}

void Reader::query_summary(const Matcher& matcher, Summary& summary)
{
    auto summary_path = sys::with_suffix(m_segment->abspath(), ".summary");
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

}
