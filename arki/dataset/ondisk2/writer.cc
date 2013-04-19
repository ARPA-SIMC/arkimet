/*
 * dataset/ondisk2/writer - Local on disk dataset writer
 *
 * Copyright (C) 2007--2012  ARPA-SIM <urpsim@smr.arpa.emr.it>
 *
 * This program is free software; you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation; either version 2 of the License, or
 * (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License along
 * with this program; if not, write to the Free Software Foundation, Inc.,
 * 51 Franklin Street, Fifth Floor, Boston, MA 02110-1301 USA.
 *
 * Author: Enrico Zini <enrico@enricozini.com>
 */

#include "config.h"
#include <arki/dataset/ondisk2/writer.h>
#include <arki/dataset/maintenance.h>
#include <arki/dataset/archive.h>
#include <arki/dataset/targetfile.h>
#include <arki/types/assigneddataset.h>
#include <arki/configfile.h>
#include <arki/metadata.h>
#include <arki/metadata/collection.h>
#include <arki/matcher.h>
#include <arki/scan/dir.h>
#include <arki/scan/any.h>
#include <arki/utils.h>
#include <arki/utils/files.h>
#include <arki/utils/compress.h>
#include <arki/summary.h>
#include <arki/nag.h>

#include <wibble/exception.h>
#include <wibble/string.h>
#include <wibble/sys/fs.h>
#include <wibble/sys/lockfile.h>

#include <fstream>
#include <sstream>

#include <sys/types.h>
#include <sys/stat.h>
#include <fcntl.h>
#include <unistd.h>
#include <cerrno>

using namespace std;
using namespace wibble;
using namespace arki::utils;

namespace arki {
namespace dataset {
namespace ondisk2 {

Writer::Writer(const ConfigFile& cfg)
	: WritableLocal(cfg), m_cfg(cfg),
	  m_idx(cfg), m_tf(0)
{
	// Create the directory if it does not exist
	wibble::sys::fs::mkpath(m_path);

	// If the index is missing, take note not to perform a repack until a
	// check is made
	if (!sys::fs::exists(str::joinpath(m_path, "index.sqlite")))
		files::createDontpackFlagfile(m_path);

	m_tf = TargetFile::create(cfg);
	m_idx.open();
}

Writer::~Writer()
{
	flush();
	if (m_tf) delete m_tf;
}

data::Writer Writer::file(const std::string& pathname, const std::string& format)
{
    std::map<std::string, data::Writer>::iterator i = m_df_cache.find(pathname);
    if (i != m_df_cache.end())
        return i->second;

	// Ensure that the directory for 'pathname' exists
	string pn = str::joinpath(m_path, pathname);
	size_t pos = pn.rfind('/');
	if (pos != string::npos)
		wibble::sys::fs::mkpath(pn.substr(0, pos));

	if (scan::isCompressed(pn))
		throw wibble::exception::Consistency("accessing data file " + pathname,
				"cannot update compressed data files: please manually uncompress it first");

    data::Writer res = data::Writer::get(format, pn);
    m_df_cache.insert(make_pair(pathname, res));
    return res;
}

WritableDataset::AcquireResult Writer::acquire_replace_never(Metadata& md)
{
    string reldest = (*m_tf)(md) + "." + md.source->format;
    data::Writer w = file(reldest, md.source->format);
    off64_t ofs;

    Pending p_idx = m_idx.beginTransaction();
    Pending p_df = w.append(md, &ofs);

    try {
        int id;
        m_idx.index(md, reldest, ofs, &id);
        p_df.commit();
        p_idx.commit();
        md.set(types::AssignedDataset::create(m_name, str::fmt(id)));
        return ACQ_OK;
    } catch (utils::sqlite::DuplicateInsert& di) {
        md.add_note(types::Note::create("Failed to store in dataset '"+m_name+"' because the dataset already has the data: " + di.what()));
        return ACQ_ERROR_DUPLICATE;
    } catch (std::exception& e) {
        // sqlite will take care of transaction consistency
        md.add_note(types::Note::create("Failed to store in dataset '"+m_name+"': " + e.what()));
        return ACQ_ERROR;
    }
}

WritableDataset::AcquireResult Writer::acquire_replace_always(Metadata& md)
{
    string reldest = (*m_tf)(md) + "." + md.source->format;
    data::Writer w = file(reldest, md.source->format);
    off64_t ofs;

    Pending p_idx = m_idx.beginTransaction();
    Pending p_df = w.append(md, &ofs);

    try {
        int id;
        m_idx.replace(md, reldest, ofs, &id);
        // In a replace, we necessarily replace inside the same file,
        // as it depends on the metadata reftime
        //createPackFlagfile(df->pathname);
        p_df.commit();
        p_idx.commit();
        md.set(types::AssignedDataset::create(m_name, str::fmt(id)));
        return ACQ_OK;
    } catch (std::exception& e) {
        // sqlite will take care of transaction consistency
        md.add_note(types::Note::create("Failed to store in dataset '"+m_name+"': " + e.what()));
        return ACQ_ERROR;
    }
}

WritableDataset::AcquireResult Writer::acquire_replace_higher_usn(Metadata& md)
{
    // Try to acquire without replacing
    string reldest = (*m_tf)(md) + "." + md.source->format;
    data::Writer w = file(reldest, md.source->format);
    off64_t ofs;

    Pending p_idx = m_idx.beginTransaction();
    Pending p_df = w.append(md, &ofs);

    try {
        int id;
        m_idx.index(md, reldest, ofs, &id);
        p_df.commit();
        p_idx.commit();
        md.set(types::AssignedDataset::create(m_name, str::fmt(id)));
        return ACQ_OK;
    } catch (utils::sqlite::DuplicateInsert& di) {
        // It already exists, so we keep p_df uncommitted and check Update Sequence Numbers
    } catch (std::exception& e) {
        // sqlite will take care of transaction consistency
        md.add_note(types::Note::create("Failed to store in dataset '"+m_name+"': " + e.what()));
        return ACQ_ERROR;
    }

    // Read the update sequence number of the new BUFR
    int new_usn;
    if (!scan::update_sequence_number(md, new_usn))
        return ACQ_ERROR_DUPLICATE;

    // Read the metadata of the existing BUFR
    Metadata old_md;
    if (!m_idx.get_current(md, old_md))
        throw wibble::exception::Consistency("acquiring into dataset " + m_name, "Insert reported a conflict, the index failed to find the original version");

    // Read the update sequence number of the old BUFR
    int old_usn;
    if (!scan::update_sequence_number(old_md, old_usn))
        throw wibble::exception::Consistency("acquiring into dataset " + m_name, "Insert reported a conflict, the new element has an Update Sequence Number but the old one does not, so they cannot be compared");

    // If there is no new Update Sequence Number, report a duplicate
    if (old_usn > new_usn)
        return ACQ_ERROR_DUPLICATE;

    // Replace, reusing the pending datafile transaction from earlier
    try {
        int id;
        m_idx.replace(md, reldest, ofs, &id);
        // In a replace, we necessarily replace inside the same file,
        // as it depends on the metadata reftime
        //createPackFlagfile(df->pathname);
        p_df.commit();
        p_idx.commit();
        md.set(types::AssignedDataset::create(m_name, str::fmt(id)));
        return ACQ_OK;
    } catch (std::exception& e) {
        // sqlite will take care of transaction consistency
        md.add_note(types::Note::create("Failed to store in dataset '"+m_name+"': " + e.what()));
        return ACQ_ERROR;
    }
}

WritableDataset::AcquireResult Writer::acquire(Metadata& md, ReplaceStrategy replace)
{
    if (replace == REPLACE_DEFAULT) replace = m_default_replace_strategy;

    // TODO: refuse if md is before "archive age"

    switch (replace)
    {
        case REPLACE_NEVER: return acquire_replace_never(md);
        case REPLACE_ALWAYS: return acquire_replace_always(md);
        case REPLACE_HIGHER_USN: return acquire_replace_higher_usn(md);
        default:
            throw wibble::exception::Consistency("acquiring into dataset " + m_name, "unsupported replace strategy " + str::fmt((int)replace));
    }
}

void Writer::remove(Metadata& md)
{
	Item<types::AssignedDataset> ds = md.get(types::TYPE_ASSIGNEDDATASET).upcast<types::AssignedDataset>();
	if (!ds.defined())
		throw wibble::exception::Consistency("removing metadata from dataset", "the metadata is not assigned to this dataset");

	remove(ds->id);

	// reset source and dataset in the metadata
	md.source.clear();
	md.unset(types::TYPE_ASSIGNEDDATASET);
}

void Writer::remove(const std::string& str_id)
{
	// TODO: refuse if md is in the archive

	if (str_id.empty()) return;
	int id = strtoul(str_id.c_str(), 0, 10);

	// Delete from DB, and get file name
	Pending p_del = m_idx.beginTransaction();
	string file;
	m_idx.remove(id, file);

	// Create flagfile
	//createPackFlagfile(str::joinpath(m_path, file));

	// Commit delete from DB
	p_del.commit();
}

void Writer::flush()
{
    // Clearing will also call the destructors of all data::Writers
    m_df_cache.clear();
}

Pending Writer::test_writelock()
{
    return m_idx.beginExclusiveTransaction();
}

void Writer::sanityChecks(std::ostream& log, bool writable)
{
	WritableLocal::sanityChecks(log, writable);

	if (!m_idx.checkSummaryCache(log) && writable)
	{
		log << name() << ": rebuilding summary cache." << endl;
		m_idx.rebuildSummaryCache();
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
	string last_fname;

	Deleter(const std::string& name, std::ostream& log, bool writable)
		: name(name), log(log), writable(writable) {}

	void operator()(const std::string& file, int id, off64_t offset, size_t size)
	{
		if (last_fname == file) return;
		if (writable)
		{
			log << name << ": deleting file " << file << endl;
			sys::fs::deleteIfExists(file);
		} else
			log << name << ": would delete file " << file << endl;
		last_fname = file;
	}
};

struct CheckAge : public maintenance::MaintFileVisitor
{
	maintenance::MaintFileVisitor& next;
	const Index& idx;
	std::string archive_threshold;
	std::string delete_threshold;

	CheckAge(MaintFileVisitor& next, const Index& idx, int archive_age=-1, int delete_age=-1);

	void operator()(const std::string& file, State state);
};

CheckAge::CheckAge(MaintFileVisitor& next, const Index& idx, int archive_age, int delete_age)
    : next(next), idx(idx)
{
    time_t now = override_now ? override_now : time(NULL);
    struct tm t;

    // Go to the beginning of the day
    now -= (now % (3600*24));

	if (archive_age != -1)
	{
		time_t arc_thr = now - archive_age * 3600 * 24;
		gmtime_r(&arc_thr, &t);
		archive_threshold = str::fmtf("%04d-%02d-%02d %02d:%02d:%02d",
				t.tm_year + 1900, t.tm_mon+1, t.tm_mday,
				t.tm_hour, t.tm_min, t.tm_sec);
	}
	if (delete_age != -1)
	{
		time_t del_thr = now - delete_age * 3600 * 24;
		gmtime_r(&del_thr, &t);
		delete_threshold = str::fmtf("%04d-%02d-%02d %02d:%02d:%02d",
				t.tm_year + 1900, t.tm_mon+1, t.tm_mday,
				t.tm_hour, t.tm_min, t.tm_sec);
	}
}

void CheckAge::operator()(const std::string& file, State state)
{
	if (state != OK or (archive_threshold.empty() and delete_threshold.empty()))
		next(file, state);
	else
	{
		string maxdate = idx.max_file_reftime(file);
		//cerr << "TEST " << maxdate << " WITH " << delete_threshold << " AND " << archive_threshold << endl;
		if (delete_threshold >= maxdate)
		{
			nag::verbose("CheckAge: %s is old enough to be deleted", file.c_str());
			next(file, TO_DELETE);
		}
		else if (archive_threshold >= maxdate)
		{
			nag::verbose("CheckAge: %s is old enough to be archived", file.c_str());
			next(file, TO_ARCHIVE);
		}
		else
			next(file, state);
	}
}
}

void Writer::maintenance(maintenance::MaintFileVisitor& v, bool quick)
{
	// TODO: run file:///usr/share/doc/sqlite3-doc/pragma.html#debug
	// and delete the index if it fails

	// Iterate subdirs in sorted order
	// Also iterate files on index in sorted order
	// Check each file for need to reindex or repack
	CheckAge ca(v, m_idx, m_archive_age, m_delete_age);
	vector<string> files = scan::dir(m_path);
	maintenance::FindMissing fm(ca, files);
	maintenance::HoleFinder hf(fm, m_path, quick);
	m_idx.scan_files(hf);
	hf.end();
	fm.end();
	WritableLocal::maintenance(v, quick);
}

void Writer::removeAll(std::ostream& log, bool writable)
{
	if (writable)
	{
		log << m_name << ": clearing summary cache" << endl;
		m_idx.invalidateSummaryCache();
	} else
		log << m_name << ": would clear summary cache" << endl;

	Deleter deleter(m_name, log, writable);
	m_idx.scan_files(deleter);

	// TODO: empty the index
	WritableLocal::removeAll(log, writable);
}


namespace {
struct FileCopier : maintenance::IndexFileVisitor
{
	WIndex& m_idx;
	const scan::Validator& m_val;
	wibble::sys::Buffer buf;
	std::string src;
	std::string dst;
	int fd_src;
	int fd_dst;
	off_t w_off;

	FileCopier(WIndex& idx, const std::string& src, const std::string& dst);
	virtual ~FileCopier();

	void operator()(const std::string& file, int id, off64_t offset, size_t size);

	void flush();
};

FileCopier::FileCopier(WIndex& idx, const std::string& src, const std::string& dst)
	: m_idx(idx), m_val(scan::Validator::by_filename(src)), src(src), dst(dst), fd_src(-1), fd_dst(-1), w_off(0)
{
	fd_src = open(src.c_str(), O_RDONLY
#ifdef linux
			| O_NOATIME
#endif
	);
	if (fd_src < 0)
		throw wibble::exception::File(src, "opening file");

	fd_dst = open(dst.c_str(), O_WRONLY | O_CREAT | O_TRUNC, 0666);
	if (fd_dst < 0)
		throw wibble::exception::File(dst, "opening file");
}

FileCopier::~FileCopier()
{
	flush();
}

void FileCopier::operator()(const std::string& file, int id, off64_t offset, size_t size)
{
    // Resize the buffer at the exact size
    buf.resize(size);
	ssize_t res = pread(fd_src, buf.data(), size, offset);
	if (res < 0 || (unsigned)res != size)
		throw wibble::exception::File(src, "reading " + str::fmt(size) + " bytes");
	m_val.validate(buf.data(), size);

    // Get the file extension
    std::string fmt;
    size_t pos;
    if ((pos = file.rfind('.')) != std::string::npos) {
        fmt = file.substr(pos+1);
    }
    // Append the buffer
    data::Writer writer = data::Writer::get(fmt, dst);
    writer.append(buf);

	// Reindex file from offset to w_off
	m_idx.relocate_data(id, w_off);

    // The offset is given by the writer
	w_off = writer.wrpos();
}

void FileCopier::flush()
{
	if (fd_src != -1 and close(fd_src) != 0)
		throw wibble::exception::File(src, "closing file");
	fd_src = -1;
	if (fd_dst != -1 and close(fd_dst) != 0)
		throw wibble::exception::File(dst, "closing file");
	fd_dst = -1;
}

struct Reindexer : public metadata::Consumer
{
	WIndex& idx;
	std::string relfile;
	std::string basename;

	Reindexer(WIndex& idx,
		  const std::string& relfile)
	       	: idx(idx), relfile(relfile), basename(str::basename(relfile)) {}

	virtual bool operator()(Metadata& md)
	{
		Item<types::source::Blob> blob = md.source.upcast<types::source::Blob>();
		try {
			int id;
			if (str::basename(blob->filename) != basename)
				throw wibble::exception::Consistency(
					"rescanning " + relfile,
				       	"metadata points to the wrong file: " + blob->filename);
			idx.index(md, relfile, blob->offset, &id);
		} catch (utils::sqlite::DuplicateInsert& di) {
			throw wibble::exception::Consistency("reindexing " + basename,
					"data item at offset " + str::fmt(blob->offset) + " has a duplicate elsewhere in the dataset: manual fix is required");
		} catch (std::exception& e) {
			// sqlite will take care of transaction consistency
			throw wibble::exception::Consistency("reindexing " + basename,
					"failed to reindex data item at offset " + str::fmt(blob->offset) + ": " + e.what());
		}
		return true;
	}
};

}

void Writer::rescanFile(const std::string& relpath)
{
	// Lock away writes and reads
	Pending p = m_idx.beginExclusiveTransaction();
	// cerr << "LOCK" << endl;

	// Remove from the index all data about the file
	m_idx.reset(relpath);
	// cerr << " RESET " << file << endl;

	string pathname = str::joinpath(m_path, relpath);

	// Temporarily uncompress the file for scanning
	auto_ptr<utils::compress::TempUnzip> tu;
	if (scan::isCompressed(pathname))
		tu.reset(new utils::compress::TempUnzip(pathname));

	// Collect the scan results in a metadata::Collector
	metadata::Collection mds;
	if (!scan::scan(pathname, mds))
		throw wibble::exception::Consistency("rescanning " + pathname, "file format unknown");
	// cerr << " SCANNED " << pathname << ": " << mds.size() << endl;

	// Scan the list of metadata, looking for duplicates and marking all
	// the duplicates except the last one as deleted
	index::IDMaker id_maker(m_idx.unique_codes());

	map<string, Metadata*> finddupes;
	for (metadata::Collection::iterator i = mds.begin(); i != mds.end(); ++i)
	{
		string id = id_maker.id(*i);
		if (id.empty())
			continue;
		map<string, Metadata*>::iterator dup = finddupes.find(id);
		if (dup == finddupes.end())
			finddupes.insert(make_pair(id, &(*i)));
		else
			dup->second = &(*i);
	}
	// cerr << " DUPECHECKED " << pathname << ": " << finddupes.size() << endl;

	// Send the remaining metadata to the reindexer
	Reindexer fixer(m_idx, relpath);
	for (map<string, Metadata*>::const_iterator i = finddupes.begin();
			i != finddupes.end(); ++i)
	{
		bool res = fixer(*i->second);
		assert(res);
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
	Pending p = m_idx.beginExclusiveTransaction();

	// Make a copy of the file with the right data in it, sorted by
	// reftime, and update the offsets in the index
	string pathname = str::joinpath(m_path, relpath);
	string pntmp = pathname + ".repack";
	FileCopier copier(m_idx, pathname, pntmp);
	m_idx.scan_file(relpath, copier, "reftime, offset");
	copier.flush();

	size_t size_pre = files::size(pathname);
	size_t size_post = files::size(pntmp);

	// Remove the .metadata file if present, because we are shuffling the
	// data file and it will not be valid anymore
	string mdpathname = pathname + ".metadata";
	if (sys::fs::exists(mdpathname))
		if (unlink(mdpathname.c_str()) < 0)
			throw wibble::exception::System("removing obsolete metadata file " + mdpathname);

	// Prevent reading the still open old file using the new offsets
	Metadata::flushDataReaders();

	// Rename the file with to final name
	if (rename(pntmp.c_str(), pathname.c_str()) < 0)
		throw wibble::exception::System("renaming " + pntmp + " to " + pathname);

	// Commit the changes on the database
	p.commit();

	return size_pre - size_post;
}

size_t Writer::removeFile(const std::string& relpath, bool withData)
{
	m_idx.reset(relpath);

	if (withData)
	{
		string pathname = str::joinpath(m_path, relpath);
		size_t size = files::size(pathname);
		if (unlink(pathname.c_str()) < 0)
			throw wibble::exception::System("removing " + pathname);
		return size;
	} else
		return 0;
}

void Writer::archiveFile(const std::string& relpath)
{
	// Create the target directory in the archive
	string pathname = str::joinpath(m_path, relpath);

	// Rebuild the metadata
	metadata::Collection mds;
	m_idx.scan_file(relpath, mds);
	mds.writeAtomically(pathname + ".metadata");

	// Remove from index
	m_idx.reset(relpath);

	// Delegate the rest to WritableLocal
	WritableLocal::archiveFile(relpath);
}

size_t Writer::vacuum()
{
	size_t size_pre = 0, size_post = 0;
	if (files::size(m_idx.pathname()) > 0)
	{
		size_pre = files::size(m_idx.pathname())
				+ files::size(m_idx.pathname() + "-journal");
		m_idx.vacuum();
		size_post = files::size(m_idx.pathname())
				+ files::size(m_idx.pathname() + "-journal");
	}

	// Rebuild the cached summaries, if needed
	if (!sys::fs::exists(str::joinpath(m_path, ".summaries/all.summary")))
		m_idx.summaryForAll();

	return size_pre > size_post ? size_pre - size_post : 0;
}

WritableDataset::AcquireResult Writer::testAcquire(const ConfigFile& cfg, const Metadata& md, std::ostream& out)
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

}
}
}
// vim:set ts=4 sw=4:
