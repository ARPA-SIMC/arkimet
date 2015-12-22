#include "arki/dataset/ondisk2/writer.h"
#include "arki/dataset/maintenance.h"
#include "arki/dataset/archive.h"
#include "arki/dataset/data.h"
#include "arki/types/assigneddataset.h"
#include "arki/types/source/blob.h"
#include "arki/configfile.h"
#include "arki/metadata.h"
#include "arki/metadata/collection.h"
#include "arki/matcher.h"
#include "arki/scan/dir.h"
#include "arki/scan/any.h"
#include "arki/utils.h"
#include "arki/utils/files.h"
#include "arki/utils/compress.h"
#include "arki/summary.h"
#include "arki/nag.h"
#include "arki/utils/string.h"
#include "arki/utils/sys.h"
#include "arki/wibble/exception.h"
#include <fstream>
#include <sstream>
#include <sys/types.h>
#include <sys/stat.h>
#include <fcntl.h>
#include <unistd.h>
#include <cerrno>
#include <cassert>

using namespace std;
using namespace arki::utils;
using namespace arki::types;

namespace arki {
namespace dataset {
namespace ondisk2 {

Writer::Writer(const ConfigFile& cfg)
    : IndexedWriter(cfg), m_cfg(cfg), idx(new index::WContents(cfg))
{
    m_idx = idx;

    // Create the directory if it does not exist
    sys::makedirs(m_path);

    // If the index is missing, take note not to perform a repack until a
    // check is made
    if (!sys::exists(str::joinpath(m_path, "index.sqlite")))
        files::createDontpackFlagfile(m_path);

    idx->open();
}

Writer::~Writer()
{
    flush();
}

Writer::AcquireResult Writer::acquire_replace_never(Metadata& md)
{
    data::Segment* w = file(md, md.source().format);
    off_t ofs;

    Pending p_idx = idx->beginTransaction();
    Pending p_df = w->append(md, &ofs);
    auto assigned_dataset = types::AssignedDataset::create(m_name, w->relname + ":" + to_string(ofs));
    auto source = types::source::Blob::create(md.source().format, m_path, w->relname, ofs, md.data_size());

    try {
        int id;
        idx->index(md, w->relname, ofs, &id);
        p_df.commit();
        p_idx.commit();
        md.set(move(assigned_dataset));
        md.set_source(move(source));
        return ACQ_OK;
    } catch (utils::sqlite::DuplicateInsert& di) {
        md.add_note("Failed to store in dataset '" + m_name + "' because the dataset already has the data: " + di.what());
        return ACQ_ERROR_DUPLICATE;
    } catch (std::exception& e) {
        // sqlite will take care of transaction consistency
        md.add_note("Failed to store in dataset '" + m_name + "': " + e.what());
        return ACQ_ERROR;
    }
}

Writer::AcquireResult Writer::acquire_replace_always(Metadata& md)
{
    data::Segment* w = file(md, md.source().format);
    off_t ofs;

    Pending p_idx = idx->beginTransaction();
    Pending p_df = w->append(md, &ofs);
    auto assigned_dataset = types::AssignedDataset::create(m_name, w->relname + ":" + to_string(ofs));
    auto source = types::source::Blob::create(md.source().format, m_path, w->relname, ofs, md.data_size());

    try {
        int id;
        idx->replace(md, w->relname, ofs, &id);
        // In a replace, we necessarily replace inside the same file,
        // as it depends on the metadata reftime
        //createPackFlagfile(df->pathname);
        p_df.commit();
        p_idx.commit();
        md.set(move(assigned_dataset));
        md.set_source(move(source));
        return ACQ_OK;
    } catch (std::exception& e) {
        // sqlite will take care of transaction consistency
        md.add_note("Failed to store in dataset '" + m_name + "': " + e.what());
        return ACQ_ERROR;
    }
}

Writer::AcquireResult Writer::acquire_replace_higher_usn(Metadata& md)
{
    // Try to acquire without replacing
    data::Segment* w = file(md, md.source().format);
    off_t ofs;

    Pending p_idx = idx->beginTransaction();
    Pending p_df = w->append(md, &ofs);
    auto assigned_dataset = types::AssignedDataset::create(m_name, w->relname + ":" + to_string(ofs));
    auto source = types::source::Blob::create(md.source().format, m_path, w->relname, ofs, md.data_size());

    try {
        int id;
        idx->index(md, w->relname, ofs, &id);
        p_df.commit();
        p_idx.commit();
        md.set(move(assigned_dataset));
        md.set_source(move(source));
        return ACQ_OK;
    } catch (utils::sqlite::DuplicateInsert& di) {
        // It already exists, so we keep p_df uncommitted and check Update Sequence Numbers
    } catch (std::exception& e) {
        // sqlite will take care of transaction consistency
        md.add_note("Failed to store in dataset '"+m_name+"': " + e.what());
        return ACQ_ERROR;
    }

    // Read the update sequence number of the new BUFR
    int new_usn;
    if (!scan::update_sequence_number(md, new_usn))
        return ACQ_ERROR_DUPLICATE;

    // Read the metadata of the existing BUFR
    Metadata old_md;
    if (!idx->get_current(md, old_md))
    {
        stringstream ss;
        ss << "cannot acquire into dataset " << m_name << ": insert reported a conflict, the index failed to find the original version";
        throw runtime_error(ss.str());
    }

    // Read the update sequence number of the old BUFR
    int old_usn;
    if (!scan::update_sequence_number(old_md, old_usn))
    {
        stringstream ss;
        ss << "cannot acquire into dataset " << m_name << ": insert reported a conflict, the new element has an Update Sequence Number but the old one does not, so they cannot be compared";
        throw runtime_error(ss.str());
    }

    // If there is no new Update Sequence Number, report a duplicate
    if (old_usn > new_usn)
        return ACQ_ERROR_DUPLICATE;

    // Replace, reusing the pending datafile transaction from earlier
    try {
        int id;
        idx->replace(md, w->relname, ofs, &id);
        // In a replace, we necessarily replace inside the same file,
        // as it depends on the metadata reftime
        //createPackFlagfile(df->pathname);
        p_df.commit();
        p_idx.commit();
        md.set(move(assigned_dataset));
        md.set_source(move(source));
        return ACQ_OK;
    } catch (std::exception& e) {
        // sqlite will take care of transaction consistency
        md.add_note("Failed to store in dataset '"+m_name+"': " + e.what());
        return ACQ_ERROR;
    }
}

Writer::AcquireResult Writer::acquire(Metadata& md, ReplaceStrategy replace)
{
    if (replace == REPLACE_DEFAULT) replace = m_default_replace_strategy;

    // TODO: refuse if md is before "archive age"

    switch (replace)
    {
        case REPLACE_NEVER: return acquire_replace_never(md);
        case REPLACE_ALWAYS: return acquire_replace_always(md);
        case REPLACE_HIGHER_USN: return acquire_replace_higher_usn(md);
        default:
        {
            stringstream ss;
            ss << "cannot acquire into dataset " << m_name << ": replace strategy " << (int)replace << " is not supported";
            throw runtime_error(ss.str());
        }
    }
}

void Writer::remove(Metadata& md)
{
    const types::source::Blob* source = md.has_source_blob();
    if (!source)
        throw std::runtime_error("cannot remove metadata from dataset, because it has no Blob source");

    if (source->basedir != m_path)
        throw std::runtime_error("cannot remove metadata from dataset: its basedir is " + source->basedir + " but this dataset is at " + m_path);

    // TODO: refuse if md is in the archive

    // Delete from DB, and get file name
    Pending p_del = idx->beginTransaction();
    idx->remove(source->filename, source->offset);

    // Create flagfile
    //createPackFlagfile(str::joinpath(m_path, file));

    // Commit delete from DB
    p_del.commit();

    // reset source and dataset in the metadata
    md.unset_source();
    md.unset(TYPE_ASSIGNEDDATASET);
}

void Writer::flush()
{
    SegmentedWriter::flush();
    idx->flush();
}

Pending Writer::test_writelock()
{
    return idx->beginExclusiveTransaction();
}

void Writer::sanityChecks(std::ostream& log, bool writable)
{
    SegmentedWriter::sanityChecks(log, writable);

    if (!idx->checkSummaryCache(log) && writable)
    {
        log << name() << ": rebuilding summary cache." << endl;
        idx->rebuildSummaryCache();
    }
}

static time_t override_now = 0;

TestOverrideCurrentDateForMaintenance::TestOverrideCurrentDateForMaintenance(time_t ts)
{
    old_ts = override_now;
    override_now = ts;
}
TestOverrideCurrentDateForMaintenance::~TestOverrideCurrentDateForMaintenance()
{
    override_now = old_ts;
}

namespace {
struct Deleter : public maintenance::IndexFileVisitor
{
	std::string name;
	std::ostream& log;
	bool writable;

	Deleter(const std::string& name, std::ostream& log, bool writable)
		: name(name), log(log), writable(writable) {}

    void operator()(const std::string& file, const metadata::Collection& mdc)
    {
        if (writable)
        {
            log << name << ": deleting file " << file << endl;
            sys::unlink_ifexists(file);
        } else
            log << name << ": would delete file " << file << endl;
    }
};

struct CheckAge : public maintenance::MaintFileVisitor
{
	maintenance::MaintFileVisitor& next;
	const index::Contents& idx;
	std::string archive_threshold;
	std::string delete_threshold;

    CheckAge(MaintFileVisitor& next, const index::Contents& idx, int archive_age=-1, int delete_age=-1);

    std::string format_ts(time_t ts)
    {
        struct tm t;
        gmtime_r(&ts, &t);
        char buf[25];
        snprintf(buf, 25, "%04d-%02d-%02d %02d:%02d:%02d",
                t.tm_year + 1900, t.tm_mon+1, t.tm_mday,
                t.tm_hour, t.tm_min, t.tm_sec);
        return buf;
    }

    void operator()(const std::string& file, data::FileState state);
};

CheckAge::CheckAge(MaintFileVisitor& next, const index::Contents& idx, int archive_age, int delete_age)
    : next(next), idx(idx)
{
    time_t now = override_now ? override_now : time(NULL);

    // Go to the beginning of the day
    now -= (now % (3600*24));

    if (archive_age != -1)
        archive_threshold = format_ts(now - archive_age * 3600 * 24);
    if (delete_age != -1)
        delete_threshold = format_ts(now - delete_age * 3600 * 24);
}

void CheckAge::operator()(const std::string& file, data::FileState state)
{
    if (archive_threshold.empty() and delete_threshold.empty())
        next(file, state);
    else
    {
        string maxdate = idx.max_file_reftime(file);
        //cerr << "TEST " << maxdate << " WITH " << delete_threshold << " AND " << archive_threshold << endl;
        if (not delete_threshold.empty() && delete_threshold >= maxdate)
        {
            nag::verbose("CheckAge: %s is old enough to be deleted", file.c_str());
            next(file, state + FILE_TO_DELETE);
        }
        else if (not archive_threshold.empty() && archive_threshold >= maxdate)
        {
            nag::verbose("CheckAge: %s is old enough to be archived", file.c_str());
            next(file, state + FILE_TO_ARCHIVE);
        }
        else
            next(file, state);
    }
}

struct FileChecker : public maintenance::IndexFileVisitor
{
    data::SegmentManager& sm;
    bool quick;
    maintenance::MaintFileVisitor& next;

    FileChecker(data::SegmentManager& sm, bool quick, maintenance::MaintFileVisitor& next)
        : sm(sm), quick(quick), next(next)
    {
    }

    virtual void operator()(const std::string& relname, const metadata::Collection& mds)
    {
        next(relname, sm.check(relname, mds, quick));
    }
};
}

void Writer::maintenance(maintenance::MaintFileVisitor& v, bool quick)
{
    // TODO: run file:///usr/share/doc/sqlite3-doc/pragma.html#debug
    // and delete the index if it fails

    // Iterate subdirs in sorted order
    // Also iterate files on index in sorted order
    // Check each file for need to reindex or repack
    CheckAge ca(v, *idx, m_archive_age, m_delete_age);
    vector<string> files = scan::dir(m_path);
    maintenance::FindMissing fm(ca, files);
    FileChecker checker(*m_segment_manager, quick, fm);
    idx->scan_files(checker);
    fm.end();
    SegmentedWriter::maintenance(v, quick);
}

void Writer::removeAll(std::ostream& log, bool writable)
{
    Deleter deleter(m_name, log, writable);
    idx->scan_files(deleter);
    if (writable)
    {
        log << m_name << ": clearing index" << endl;
        idx->reset();
    } else
        log << m_name << ": would clear index" << endl;

    // TODO: empty the index
    SegmentedWriter::removeAll(log, writable);
}


void Writer::rescanFile(const std::string& relpath)
{
    string pathname = str::joinpath(m_path, relpath);

    // Temporarily uncompress the file for scanning
    unique_ptr<utils::compress::TempUnzip> tu;
    if (scan::isCompressed(pathname))
        tu.reset(new utils::compress::TempUnzip(pathname));

    // Collect the scan results in a metadata::Collector
    metadata::Collection mds;
    if (!scan::scan(pathname, mds.inserter_func()))
        throw wibble::exception::Consistency("rescanning " + pathname, "file format unknown");
    // cerr << " SCANNED " << pathname << ": " << mds.size() << endl;

    // Lock away writes and reads
    Pending p = idx->beginExclusiveTransaction();
    // cerr << "LOCK" << endl;

    // Remove from the index all data about the file
    idx->reset(relpath);
    // cerr << " RESET " << file << endl;

    // Scan the list of metadata, looking for duplicates and marking all
    // the duplicates except the last one as deleted
    index::IDMaker id_maker(idx->unique_codes());

    map<string, const Metadata*> finddupes;
    for (metadata::Collection::const_iterator i = mds.begin(); i != mds.end(); ++i)
    {
        string id = id_maker.id(**i);
        if (id.empty())
            continue;
        map<string, const Metadata*>::iterator dup = finddupes.find(id);
        if (dup == finddupes.end())
            finddupes.insert(make_pair(id, *i));
        else
            dup->second = *i;
    }
    // cerr << " DUPECHECKED " << pathname << ": " << finddupes.size() << endl;

    // Send the remaining metadata to the reindexer
    std::string basename = str::basename(relpath);
    for (map<string, const Metadata*>::const_iterator i = finddupes.begin();
            i != finddupes.end(); ++i)
    {
        const Metadata& md = *i->second;
        const source::Blob& blob = md.sourceBlob();
        try {
            int id;
            if (str::basename(blob.filename) != basename)
                throw std::runtime_error("cannot rescan " + relpath + ": metadata points to the wrong file: " + blob.filename);
            idx->index(md, relpath, blob.offset, &id);
        } catch (utils::sqlite::DuplicateInsert& di) {
            stringstream ss;
            ss << "cannot reindex " << basename << ": data item at offset " << blob.offset << " has a duplicate elsewhere in the dataset: manual fix is required";
            throw runtime_error(ss.str());
        } catch (std::exception& e) {
            stringstream ss;
            ss << "cannot reindex " << basename << ": failed to reindex data item at offset " << blob.offset << ": " << e.what();
            throw runtime_error(ss.str());
            // sqlite will take care of transaction consistency
        }
    }
    // cerr << " REINDEXED " << pathname << endl;

	// TODO: if scan fails, remove all info from the index and rename the
	// file to something like .broken

	// Commit the changes on the database
	p.commit();
	// cerr << " COMMITTED" << endl;

	// TODO: remove relevant summary
}


size_t Writer::repackFile(const std::string& relpath)
{
    // Lock away writes and reads
    Pending p = idx->beginExclusiveTransaction();

    // Make a copy of the file with the right data in it, sorted by
    // reftime, and update the offsets in the index
    string pathname = str::joinpath(m_path, relpath);

    metadata::Collection mds;
    idx->scan_file(relpath, mds.inserter_func(), "reftime, offset");
    Pending p_repack = m_segment_manager->repack(relpath, mds);

    // Reindex mds
    idx->reset(relpath);
    for (metadata::Collection::const_iterator i = mds.begin(); i != mds.end(); ++i)
    {
        const source::Blob& source = (*i)->sourceBlob();
        idx->index(**i, source.filename, source.offset);
    }

    size_t size_pre = sys::size(pathname);

    // Remove the .metadata file if present, because we are shuffling the
    // data file and it will not be valid anymore
    string mdpathname = pathname + ".metadata";
    if (sys::exists(mdpathname))
        if (unlink(mdpathname.c_str()) < 0)
        {
            stringstream ss;
            ss << "cannot remove obsolete metadata file " << mdpathname;
            throw std::system_error(errno, std::system_category(), ss.str());
        }

    // Prevent reading the still open old file using the new offsets
    Metadata::flushDataReaders();

    // Commit the changes in the file system
    p_repack.commit();

    // Commit the changes in the database
    p.commit();

    size_t size_post = sys::size(pathname);

    return size_pre - size_post;
}

size_t Writer::removeFile(const std::string& relpath, bool withData)
{
    idx->reset(relpath);
    // TODO: also remove .metadata and .summary files
    return SegmentedWriter::removeFile(relpath, withData);
}

void Writer::archiveFile(const std::string& relpath)
{
    // Create the target directory in the archive
    string pathname = str::joinpath(m_path, relpath);

    // Rebuild the metadata
    metadata::Collection mds;
    idx->scan_file(relpath, mds.inserter_func());
    mds.writeAtomically(pathname + ".metadata");

    // Remove from index
    idx->reset(relpath);

    // Delegate the rest to SegmentedWriter
    SegmentedWriter::archiveFile(relpath);
}

size_t Writer::vacuum()
{
    size_t size_pre = 0, size_post = 0;
    if (sys::size(idx->pathname(), 0) > 0)
    {
        size_pre = sys::size(idx->pathname(), 0)
                 + sys::size(idx->pathname() + "-journal", 0);
        idx->vacuum();
        size_post = sys::size(idx->pathname(), 0)
                  + sys::size(idx->pathname() + "-journal", 0);
    }

    // Rebuild the cached summaries, if needed
    if (!sys::exists(str::joinpath(m_path, ".summaries/all.summary")))
    {
        Summary s;
        idx->summaryForAll(s);
    }

    return size_pre > size_post ? size_pre - size_post : 0;
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
    throw std::runtime_error("testAcquire not implemented for ondisk2 datasets");
}

}
}
}
