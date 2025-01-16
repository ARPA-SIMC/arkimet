#include "reader.h"
#include "arki/dataset/lock.h"
#include "arki/dataset/simple/manifest.h"
#include "arki/query.h"
#include "arki/query/progress.h"
#include "arki/types/source.h"
#include "arki/types/source/blob.h"
#include "arki/metadata.h"
#include "arki/metadata/sort.h"
#include "arki/utils/sys.h"
#include <algorithm>

using namespace std;
using namespace arki::utils;

namespace arki::dataset::simple {

bool Reader::hasWorkingIndex() const
{
    return manifest::exists(dataset().path);
}

bool Reader::impl_query_data(const query::Data& q, metadata_dest_func dest)
{
    query::TrackProgress track(q.progress);
    dest = track.wrap(dest);

    auto lock = dataset().read_lock_dataset();

    // Query archives first
    if (!local::Reader::impl_query_data(q, dest))
        return false;

    // Get list of segments matching the query
    manifest.reread();
    auto segmentinfos = manifest.file_list(q.matcher);

    // segmentinfos is sorted by relpath: resort by time
    std::sort(segmentinfos.begin(), segmentinfos.end(), [](const auto& a, const auto& b) { return a.time.begin < b.time.begin; });

    // TODO: do we need to sort individual results, or can we rely on checker to warn if
    //       segments overlap or if a segment is not sorted?
    //       (note that a segment not being sorted can be common; however
    //       if they don't overlap then we can just sort metadata on a
    //       segment-by-segment basis)

    std::shared_ptr<metadata::sort::Compare> compare;
    if (q.sorter)
        compare = q.sorter;
    else
        // If no sorter is provided, sort by reftime in case segment files have
        // not been sorted before archiving
        compare = metadata::sort::Compare::parse("reftime");

    metadata::sort::Stream sorter(*compare, dest);
    for (const auto& segmentinfo: segmentinfos)
    {
        // TODO: delegate to segment reader
        auto segment = dataset().segment_session->segment_from_relpath(segmentinfo.relpath);
        auto prepend_fname = segmentinfo.relpath.parent_path();
        auto fullpath = sys::with_suffix(segment->abspath(), ".metadata");
        if (!std::filesystem::exists(fullpath)) continue;
        std::shared_ptr<arki::segment::data::Reader> reader;
        if (q.with_data)
            reader = dataset().segment_session->segment_reader(segment, lock);
        // This generates filenames relative to the metadata
        // We need to use m_path as the dirname, and prepend dirname(*i) to the filenames
        Metadata::read_file(fullpath, [&](std::shared_ptr<Metadata> md) {
            // Filter using the matcher in the query
            if (!q.matcher(*md)) return true;

            // Tweak Blob sources replacing basedir and prepending a directory to the file name
            if (const types::source::Blob* s = md->has_source_blob())
            {
                if (q.with_data)
                    md->set_source(types::Source::createBlob(s->format, dataset().path, prepend_fname / s->filename, s->offset, s->size, reader));
                else
                    md->set_source(types::Source::createBlobUnlocked(s->format, dataset().path, prepend_fname / s->filename, s->offset, s->size));
            }
            return sorter.add(md);
        });
#if 0
        auto segment = dataset().segment_session->segment_from_relpath(segmentinfo.relpath);
        auto reader = segment->reader(lock);
        reader->query_data(q, [&](std::shared_ptr<Metadata> md) {
            return sorter.add(md);
        });
#endif
        if (!sorter.flush())
            return track.done(false);
    }

    return track.done(true);
}

void Reader::query_segments_for_summary(const Matcher& matcher, Summary& summary)
{
    manifest.reread();
    auto segmentinfos = manifest.file_list(matcher);
    for (const auto& segmentinfo: segmentinfos)
    {
        // TODO: create a segment and delegate to Segment reader
        auto pathname = dataset().path / segmentinfo.relpath;
        auto summary_path = sys::with_suffix(pathname, ".summary");

        // Silently skip files that have been deleted
        if (!sys::access(summary_path, R_OK))
            continue;

        // TODO: we can resummarize from metadata (can be done by new Segment reader)

        Summary s;
        s.read_file(summary_path);
        s.filter(matcher, summary);
    }
}

void Reader::impl_query_summary(const Matcher& matcher, Summary& summary)
{
    auto lock = dataset().read_lock_dataset();

    // Query the archives first
    local::Reader::impl_query_summary(matcher, summary);

    // If the matcher discriminates on reference times, query the individual segments
    if (matcher.get(TYPE_REFTIME))
    {
        query_segments_for_summary(matcher, summary);
        return;
    }

    // The matcher does not contain reftime, we can work with a
    // global summary
    auto cache_pathname = dataset().path / "summary";

    if (sys::access(cache_pathname, R_OK))
    {
        Summary s;
        s.read_file(cache_pathname);
        s.filter(matcher, summary);
    } else if (sys::access(dataset().path, W_OK)) {
        // Rebuild the cache
        Summary s;
        query_segments_for_summary(Matcher(), s);

        // Save the summary
        s.writeAtomically(cache_pathname);

        // Query the newly generated summary that we still have
        // in memory
        s.filter(matcher, summary);
    } else
        query_segments_for_summary(matcher, summary);
}

Reader::Reader(std::shared_ptr<simple::Dataset> dataset)
    : DatasetAccess(dataset), manifest(dataset->path)
{
    // Create the directory if it does not exist
    std::filesystem::create_directories(dataset->path);
}

Reader::~Reader()
{
}

std::string Reader::type() const { return "simple"; }

bool Reader::is_dataset(const std::filesystem::path& dir)
{
    return manifest::exists(dir);
}

core::Interval Reader::get_stored_time_interval()
{
    auto lock = dataset().read_lock_dataset();
    manifest.reread();
    return manifest.get_stored_time_interval();
}

}
