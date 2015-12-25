#include "indexed.h"
#include "index.h"

namespace arki {
namespace dataset {

IndexedReader::IndexedReader(const ConfigFile& cfg)
    : SegmentedReader(cfg)
{
}

IndexedReader::~IndexedReader()
{
    delete m_idx;
}

void IndexedReader::query_data(const dataset::DataQuery& q, metadata_dest_func dest)
{
    LocalReader::query_data(q, dest);
    if (!m_idx) return;
    // FIXME: this is cargo culted from the old ondisk2 reader: what is the use case for this?
    if (!m_idx->query_data(q, dest))
        throw std::runtime_error("cannot query " + m_path + ": index could not be used");
}

void IndexedReader::query_summary(const Matcher& matcher, Summary& summary)
{
    // Query the archives first
    LocalReader::query_summary(matcher, summary);
    if (!m_idx) return;
    // FIXME: this is cargo culted from the old ondisk2 reader: what is the use case for this?
    if (!m_idx->query_summary(matcher, summary))
        throw std::runtime_error("cannot query " + m_path + ": index could not be used");
}

size_t IndexedReader::produce_nth(metadata_dest_func cons, size_t idx)
{
    size_t res = LocalReader::produce_nth(cons, idx);
    if (m_idx)
        res += m_idx->produce_nth(cons, idx);
    return res;
}


IndexedWriter::IndexedWriter(const ConfigFile& cfg)
    : SegmentedWriter(cfg)
{
}

IndexedWriter::~IndexedWriter()
{
    delete m_idx;
}

}
}

