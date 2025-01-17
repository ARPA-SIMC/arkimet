#include "scan.h"
#include "data.h"
#include "arki/query.h"
#include "arki/metadata.h"
#include "arki/metadata/collection.h"
#include "arki/types/source/blob.h"

namespace arki::segment::scan {

Reader::~Reader()
{
}

bool Reader::read_all(metadata_dest_func dest)
{
    auto reader = m_segment->session().segment_data_reader(m_segment, lock);
    return reader->scan_data(dest);
}

bool Reader::query_data(const query::Data& q, metadata_dest_func dest)
{
    arki::metadata::Collection mdbuf;

    auto reader = m_segment->session().segment_data_reader(m_segment, lock);
    reader->scan_data([&](auto md) {
        if (q.matcher(*md))
            mdbuf.acquire(md);
        if (not q.with_data)
            md->sourceBlob().unlock();
        return true;
    });

    // Sort and output the rest
    if (q.sorter) mdbuf.sort(*q.sorter);

    // pass it to consumer
    return mdbuf.move_to(dest);
}

void Reader::query_summary(const Matcher& matcher, Summary& summary)
{
    auto reader = m_segment->session().segment_data_reader(m_segment, lock);
    reader->scan_data([&](auto md) {
        if (matcher(*md))
            summary.add(*md);
        return true;
    });
}

}
