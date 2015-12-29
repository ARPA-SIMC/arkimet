#include "indexed.h"
#include "index.h"
#include "maintenance.h"

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


IndexedWriter::IndexedWriter(const ConfigFile& cfg)
    : SegmentedWriter(cfg)
{
}

IndexedWriter::~IndexedWriter()
{
    delete m_idx;
}


IndexedChecker::IndexedChecker(const ConfigFile& cfg)
    : SegmentedChecker(cfg)
{
}

IndexedChecker::~IndexedChecker()
{
    delete m_idx;
}

#if 0
void IndexedChecker::maintenance(segment::state_func v, bool quick)
{
    // TODO: run file:///usr/share/doc/sqlite3-doc/pragma.html#debug
    // and delete the index if it fails

    maintenance::CheckAge ca(v, *m_tf, m_archive_age, m_delete_age);
    maintenance::FindMissing fm(m_path, [&](const std::string& relname, segment::State state) { ca(relname, state); });
    m_idx->scan_files([&](const std::string& relname, segment::State state, const metadata::Collection& mds) {
        if (!state.is_ok())
            fm.check(relname, state);
        else
            fm.check(relname, m_segment_manager->check(relname, mds, quick));
    });
    fm.end();

    SegmentedWriter::maintenance(v, quick);
}
#endif

void IndexedChecker::removeAll(Reporter& reporter, bool writable)
{
    m_idx->list_segments([&](const std::string& relpath) {
        if (writable)
        {
            size_t freed = removeFile(relpath, true);
            reporter.segment_delete(*this, relpath, "deleted (" + std::to_string(freed) + " freed)");
        } else
            reporter.segment_delete(*this, relpath, "should be deleted");
    });
    SegmentedChecker::removeAll(reporter, writable);
}

}
}

