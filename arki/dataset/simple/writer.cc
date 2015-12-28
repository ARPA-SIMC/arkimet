#include "arki/dataset/simple/writer.h"
#include "arki/dataset/index/manifest.h"
#include "arki/dataset/simple/datafile.h"
#include "arki/dataset/maintenance.h"
#include "arki/dataset/segment.h"
#include "arki/types/assigneddataset.h"
#include "arki/types/source/blob.h"
#include "arki/summary.h"
#include "arki/types/reftime.h"
#include "arki/matcher.h"
#include "arki/metadata/collection.h"
#include "arki/utils/files.h"
#include "arki/scan/any.h"
#include "arki/postprocess.h"
#include "arki/sort.h"
#include "arki/nag.h"
#include "arki/utils/sys.h"
#include "arki/utils/string.h"
#include <ctime>
#include <cstdio>
#include <sys/types.h>
#include <sys/stat.h>
#include <fcntl.h>
#include <unistd.h>

using namespace std;
using namespace arki;
using namespace arki::types;
using namespace arki::utils;

namespace arki {
namespace dataset {
namespace simple {

Writer::Writer(const ConfigFile& cfg)
    : IndexedWriter(cfg), m_mft(0)
{
    // If the index is missing, take note not to perform a repack until a
    // check is made
    if (!index::Manifest::exists(m_path))
        files::createDontpackFlagfile(m_path);

    unique_ptr<index::Manifest> mft = index::Manifest::create(m_path, &cfg);
    m_mft = mft.release();
    m_mft->openRW();
    m_idx = m_mft;
}

Writer::~Writer()
{
    flush();
}

segment::Segment* Writer::file(const Metadata& md, const std::string& format)
{
    segment::Segment* writer = SegmentedWriter::file(md, format);
    if (!writer->payload)
        writer->payload = new datafile::MdBuf(writer->absname);
    return writer;
}

Writer::AcquireResult Writer::acquire(Metadata& md, ReplaceStrategy replace)
{
    // TODO: refuse if md is before "archive age"
    segment::Segment* writer = file(md, md.source().format);
    datafile::MdBuf* mdbuf = static_cast<datafile::MdBuf*>(writer->payload);

    // Try appending
    try {
        off_t offset = writer->append(md);
        auto assigned_dataset = types::AssignedDataset::create(m_name, writer->relname + ":" + to_string(offset));
        auto source = types::source::Blob::create(md.source().format, m_path, writer->relname, offset, md.data_size());
        md.set_source(move(source));
        mdbuf->add(md);
        m_mft->acquire(writer->relname, sys::timestamp(mdbuf->pathname, 0), mdbuf->sum);
        md.set(move(assigned_dataset));
        return ACQ_OK;
    } catch (std::exception& e) {
        // sqlite will take care of transaction consistency
        md.unset(TYPE_ASSIGNEDDATASET);
        md.add_note("Failed to store in dataset '"+m_name+"': " + e.what());
        return ACQ_ERROR;
    }

	// After appending, keep updated info in-memory, and update manifest on
	// flush when the Datafile structures are deallocated
}

void Writer::remove(Metadata& md)
{
    // Nothing to do
    throw std::runtime_error("cannot remove data from simple dataset: dataset does not support removing items");
}

void Writer::flush()
{
    SegmentedWriter::flush();
    m_mft->flush();
}

Pending Writer::test_writelock()
{
    return m_mft->test_writelock();
}

Writer::AcquireResult Writer::testAcquire(const ConfigFile& cfg, const Metadata& md, std::ostream& out)
{
	// TODO
#if 0
	wibble::sys::fs::Lockfile lockfile(wibble::str::joinpath(cfg.value("path"), "lock"));

	string name = cfg.value("name");
	try {
		if (ConfigFile::boolValue(cfg.value("replace")))
		{
			if (cfg.value("index") != string())
				ondisk::writer::IndexedRootDirectory::testReplace(cfg, md, out);
			else
				ondisk::writer::RootDirectory::testReplace(cfg, md, out);
			return ACQ_OK;
		} else {
			try {
				if (cfg.value("index") != string())
					ondisk::writer::IndexedRootDirectory::testAcquire(cfg, md, out);
				else
					ondisk::writer::RootDirectory::testAcquire(cfg, md, out);
				return ACQ_OK;
			} catch (Index::DuplicateInsert& di) {
				out << "Source information restored to original value" << endl;
				out << "Failed to store in dataset '"+name+"' because the dataset already has the data: " + di.what() << endl;
				return ACQ_ERROR_DUPLICATE;
			}
		}
	} catch (std::exception& e) {
		out << "Source information restored to original value" << endl;
		out << "Failed to store in dataset '"+name+"': " + e.what() << endl;
		return ACQ_ERROR;
	}
#endif
}



Checker::Checker(const ConfigFile& cfg)
    : IndexedChecker(cfg), m_mft(0)
{
    // If the index is missing, take note not to perform a repack until a
    // check is made
    if (!index::Manifest::exists(m_path))
        files::createDontpackFlagfile(m_path);

    unique_ptr<index::Manifest> mft = index::Manifest::create(m_path, &cfg);
    m_mft = mft.release();
    m_mft->openRW();
    m_idx = m_mft;
}

Checker::~Checker()
{
    m_mft->flush();
}

namespace {
struct Deleter
{
    Checker& checker;
    Reporter& reporter;
    bool writable;

    Deleter(Checker& checker, Reporter& reporter, bool writable)
        : checker(checker), reporter(reporter), writable(writable) {}

    void operator()(const std::string& relpath, segment::State state)
    {
        if (writable)
        {
            sys::unlink_ifexists(relpath);
            // TODO: should also delete .metadata and .summary files?
            reporter.segment_delete(checker, relpath, "deleted");
        } else
            reporter.segment_delete(checker, relpath, "would be deleted");
    }
};
}

void Checker::maintenance(segment::state_func v, bool quick)
{
    // TODO Detect if data is not in reftime order
    maintenance::CheckAge ca(v, *m_step, m_archive_age, m_delete_age);
    m_mft->check(*m_segment_manager, ca, quick);
    SegmentedChecker::maintenance(v, quick);
}

void Checker::removeAll(Reporter& reporter, bool writable)
{
    Deleter deleter(*this, reporter, writable);
    m_mft->check(*m_segment_manager, deleter, true);
    // TODO: empty manifest
    SegmentedChecker::removeAll(reporter, writable);
}

void Checker::indexFile(const std::string& relname, metadata::Collection&& mds)
{
    string pathname = str::joinpath(m_path, relname);
    time_t mtime = scan::timestamp(pathname);
    if (mtime == 0)
        throw std::runtime_error("cannot acquire " + pathname + ": file does not exist");

    // Iterate the metadata, computing the summary and making the data
    // paths relative
    mds.strip_source_paths();
    Summary sum;
    mds.add_to_summary(sum);

    // Regenerate .metadata
    mds.writeAtomically(pathname + ".metadata");

    // Regenerate .summary
    sum.writeAtomically(pathname + ".summary");

    // Add to manifest
    m_mft->acquire(relname, mtime, sum);
    m_mft->flush();
}

void Checker::rescanFile(const std::string& relpath)
{
    // Delete cached info to force a full rescan
    string pathname = str::joinpath(m_path, relpath);
    sys::unlink_ifexists(pathname + ".metadata");
    sys::unlink_ifexists(pathname + ".summary");

    m_mft->rescanFile(m_path, relpath);
}


size_t Checker::repackFile(const std::string& relpath)
{
    string pathname = str::joinpath(m_path, relpath);

    // Read the metadata
    metadata::Collection mdc;
    scan::scan(pathname, mdc.inserter_func());

    // Sort by reference time
    mdc.sort();

    // Write out the data with the new order
    Pending p_repack = m_segment_manager->repack(relpath, mdc);

    // Strip paths from mds sources
    mdc.strip_source_paths();

    // Prevent reading the still open old file using the new offsets
    Metadata::flushDataReaders();

    // Remove existing cached metadata, since we scramble their order
    sys::unlink_ifexists(pathname + ".metadata");
    sys::unlink_ifexists(pathname + ".summary");

    size_t size_pre = sys::size(pathname);

    p_repack.commit();

    size_t size_post = sys::size(pathname);

	// Write out the new metadata
	mdc.writeAtomically(pathname + ".metadata");

    // Regenerate the summary. It is unchanged, really, but its timestamp
    // has become obsolete by now
    Summary sum;
    mdc.add_to_summary(sum);
    sum.writeAtomically(pathname + ".summary");

    // Reindex with the new file information
    time_t mtime = sys::timestamp(pathname);
    m_mft->acquire(relpath, mtime, sum);

	return size_pre - size_post;
}

size_t Checker::removeFile(const std::string& relpath, bool withData)
{
    m_mft->remove(relpath);
    return SegmentedChecker::removeFile(relpath, withData);
}

void Checker::archiveFile(const std::string& relpath)
{
    // Remove from index
    m_mft->remove(relpath);

    // Delegate the rest to SegmentedChecker
    SegmentedChecker::archiveFile(relpath);
}

size_t Checker::vacuum()
{
    return m_mft->vacuum();
}


}
}
}
