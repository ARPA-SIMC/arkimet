/*
 * dataset/ondisk2/writer - Local on disk dataset writer
 *
 * Copyright (C) 2007,2008,2009  ARPA-SIM <urpsim@smr.arpa.emr.it>
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

#include <arki/dataset/ondisk2/writer.h>
#include <arki/dataset/ondisk2/writer/datafile.h>
#include <arki/dataset/ondisk2/writer/utils.h>
//#include <arki/dataset/ondisk2/maintenance.h>
//#include <arki/dataset/ondisk2/maint/datafile.h>
//#include <arki/dataset/ondisk2/maint/directory.h>
#include <arki/dataset/targetfile.h>
#include <arki/types/assigneddataset.h>
#include <arki/configfile.h>
#include <arki/metadata.h>
#include <arki/matcher.h>
#include <arki/utils.h>
#include <arki/utils/files.h>
#include <arki/summary.h>

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
using namespace arki::utils::files;

namespace arki {
namespace dataset {
namespace ondisk2 {

Writer::Writer(const ConfigFile& cfg)
	: m_cfg(cfg), m_path(cfg.value("path")), m_name(cfg.value("name")),
	  m_idx(cfg), m_tf(0), m_replace(false)
{
	// Create the directory if it does not exist
	wibble::sys::fs::mkpath(m_path);

	m_tf = TargetFile::create(cfg);
	m_replace = ConfigFile::boolValue(cfg.value("replace"), false);
	m_idx.open();
}

Writer::~Writer()
{
	flush();
	if (m_tf) delete m_tf;
}

std::string Writer::path() const
{
	return m_path;
}

std::string Writer::id(const Metadata& md) const
{
	int id = m_idx.id(md);
	if (id == -1) return "";
	return str::fmt(id);
}

writer::Datafile* Writer::file(const std::string& pathname)
{
	std::map<std::string, writer::Datafile*>::iterator i = m_df_cache.find(pathname);
	if (i != m_df_cache.end())
		return i->second;

	// Ensure that the directory for 'pathname' exists
	string pn = str::joinpath(m_path, pathname);
	size_t pos = pn.rfind('/');
	if (pos != string::npos)
		wibble::sys::fs::mkpath(pn.substr(0, pos));

	writer::Datafile* res = new writer::Datafile(pn);
	m_df_cache.insert(make_pair(pathname, res));
	return res;
}


WritableDataset::AcquireResult Writer::acquire(Metadata& md)
{
	// If replace is on in the configuration file, do a replace instead
	if (m_replace)
		return replace(md) ? ACQ_OK : ACQ_ERROR;

	string reldest = (*m_tf)(md) + "." + md.source->format;
	writer::Datafile* df = file(reldest);
	off_t ofs;

	Pending p_idx = m_idx.beginTransaction();
	Pending p_df = df->append(md, &ofs);

	try {
		int id;
		m_idx.index(md, reldest, ofs, &id);
		p_df.commit();
		p_idx.commit();
		md.set(types::AssignedDataset::create(m_name, str::fmt(id)));
		return ACQ_OK;
	} catch (index::DuplicateInsert& di) {
		md.notes.push_back(types::Note::create("Failed to store in dataset '"+m_name+"' because the dataset already has the data: " + di.what()));
		return ACQ_ERROR_DUPLICATE;
	} catch (std::exception& e) {
		// sqlite will take care of transaction consistency
		md.notes.push_back(types::Note::create("Failed to store in dataset '"+m_name+"': " + e.what()));
		return ACQ_ERROR;
	}
}

bool Writer::replace(Metadata& md)
{
	string reldest = (*m_tf)(md) + "." + md.source->format;
	writer::Datafile* df = file(reldest);
	off_t ofs;

	Pending p_idx = m_idx.beginTransaction();
	Pending p_df = df->append(md, &ofs);

	try {
		int id;
		m_idx.replace(md, reldest, ofs, &id);
		// In a replace, we necessarily replace inside the same file,
		// as it depends on the metadata reftime
		createPackFlagfile(df->pathname);
		p_df.commit();
		p_idx.commit();
		md.set(types::AssignedDataset::create(m_name, str::fmt(id)));
		return true;
	} catch (std::exception& e) {
		// sqlite will take care of transaction consistency
		md.notes.push_back(types::Note::create("Failed to store in dataset '"+m_name+"': " + e.what()));
		return false;
	}
}

void Writer::remove(const std::string& str_id)
{
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
	for (std::map<std::string, writer::Datafile*>::iterator i = m_df_cache.begin();
			i != m_df_cache.end(); ++i)
		if (i->second) delete i->second;
	m_df_cache.clear();
}

struct HoleFinder : FileVisitor
{
	writer::MaintFileVisitor& next;

	const std::string& m_root;

	string last_file;
	off_t last_file_size;
	bool has_hole;

	HoleFinder(writer::MaintFileVisitor& next, const std::string& root)
	       	: next(next), m_root(root), has_hole(false) {}

	void finaliseFile()
	{
		if (!last_file.empty())
		{
			// Check if last_file_size matches the file size
			if (!has_hole)
			{
				off_t size = utils::files::size(str::joinpath(m_root, last_file));
				if (size > last_file_size)
					has_hole = true;
				else if (size < last_file_size)
				{
					// throw wibble::exception::Consistency("checking size of "+last_file, "file is shorter than what the index believes: please run a dataset check");
					next(last_file, writer::MaintFileVisitor::CORRUPTED);
					return;
				}
			}

			// Take note of files with holes
			if (has_hole)
			{
				next(last_file, writer::MaintFileVisitor::HOLES);
			} else {
				next(last_file, writer::MaintFileVisitor::OK);
			}
		}
	}

	void operator()(const std::string& file, off_t offset, size_t size)
	{
		if (last_file != file)
		{
			finaliseFile();
			last_file = file;
			last_file_size = 0;
			has_hole = false;
		}
		if (offset != last_file_size)
			has_hole = true;
		last_file_size += size;
	}

	void end()
	{
		finaliseFile();
	}
};

struct FindMissing : public writer::MaintFileVisitor
{
	writer::MaintFileVisitor& next;
	writer::DirScanner disk;

	FindMissing(MaintFileVisitor& next, const std::string& root) : next(next), disk(root) {}

	void operator()(const std::string& file, State state)
	{
		while (not disk.cur().empty() and disk.cur() < file)
		{
			next(disk.cur(), OUT_OF_INDEX);
			disk.next();
		}
		if (disk.cur() == file)
			disk.next();
		// TODO: if requested, check for internal consistency
		// TODO: it requires to have an infrastructure for quick
		// TODO:   consistency checkers (like, "GRIB starts with GRIB
		// TODO:   and ends with 7777")
		next(file, state);
	}

	void end()
	{
		while (not disk.cur().empty())
		{
			next(disk.cur(), OUT_OF_INDEX);
			disk.next();
		}
	}
};

struct MaintPrinter : public writer::MaintFileVisitor
{
	void operator()(const std::string& file, State state)
	{
		switch (state)
		{
			case OK: cerr << file << " OK" << endl;
			case HOLES: cerr << file << " HOLES" << endl;
			case OUT_OF_INDEX: cerr << file << " OUT_OF_INDEX" << endl;
			case CORRUPTED: cerr << file << " CORRUPTED" << endl;
		}
	}
};

void Writer::maintenance(MaintenanceAgent& a)
{
	// Iterate subdirs in sorted order
	// Also iterate files on index in sorted order
	// Check each file for need to reindex or repack
	MaintPrinter printer;
	FindMissing fm(printer, m_path);
	HoleFinder hf(fm, m_path);	
	m_idx.scan_files(hf, "file, reftime");
	hf.end();
	fm.end();


	// TODO: rebuild of index
	// TODO:  (empty the index, scan all files)
	// TODO: also file:///usr/share/doc/sqlite3-doc/pragma.html#debug

	// TODO: repack (vacuum of database and rebuild of data files with repack flagfiles)
	// TODO:  (select all files, offsets, sizes in order, and look for gaps)
	// TODO:  (also look for gaps at end of files and truncate)
	// TODO:  (also look for files not in the database)
	// TODO:  (maybe do all this only for files for which there is a
	//         flagfile, and just warn about other files)

#if 0
	auto_ptr<maint::RootDirectory> maint_root(maint::RootDirectory::create(m_cfg));

	a.start(*this);

	// See if the main index is ok, eventually changing the indexing behaviour
	// in the next steps
	bool full_reindex = maint_root->checkMainIndex(a);
	// See that all data files are ok or if they need rebuild
	maint_root->checkDataFiles(a, full_reindex);
	// See that all the index info are ok (summaries and index)
	maint_root->checkDirectories(a);
	// Commit database changes
	maint_root->commit();

	a.end();
#endif
}

struct HoledFilesCollector : FileVisitor
{
	const std::string& m_root;
	vector<string> files;
	string last_file;
	off_t last_file_size;
	bool has_hole;

	HoledFilesCollector(const std::string& m_root) : m_root(m_root), has_hole(false) {}

	void finaliseFile()
	{
		if (!last_file.empty())
		{
			// Check if last_file_size matches the file size
			if (!has_hole)
			{
				off_t size = utils::files::size(str::joinpath(m_root, last_file));
				if (size > last_file_size)
					has_hole = true;
				else if (size < last_file_size)
					throw wibble::exception::Consistency("checking size of "+last_file, "file is shorter than what the index believes: please run a dataset check");
			}

			// Take note of files with holes
			if (has_hole)
				files.push_back(last_file);
		}
	}

	void operator()(const std::string& file, off_t offset, size_t size)
	{
		if (last_file != file)
		{
			finaliseFile();
			last_file = file;
			last_file_size = 0;
			has_hole = false;
		}
		if (offset != last_file_size)
			has_hole = true;
		last_file_size += size;
	}

	void end()
	{
		finaliseFile();
	}
};

struct FileCopier : FileVisitor
{
	WIndex& m_idx;
	sys::Buffer buf;
	std::string src;
	std::string dst;
	int fd_src;
	int fd_dst;
	off_t w_off;

	FileCopier(WIndex& idx, const std::string& src, const std::string& dst)
		: m_idx(idx), src(src), dst(dst), fd_src(-1), fd_dst(-1), w_off(0)
	{
		fd_src = open(src.c_str(), O_RDONLY | O_NOATIME);
		if (fd_src < 0)
			throw wibble::exception::File(src, "opening file");

		fd_dst = open(dst.c_str(), O_WRONLY | O_CREAT | O_TRUNC, 0666);
		if (fd_dst < 0)
			throw wibble::exception::File(dst, "opening file");
	}

	virtual ~FileCopier()
	{
		flush();
	}

	void operator()(const std::string& file, off_t offset, size_t size)
	{
		if (buf.size() < size)
			buf.resize(size);
		ssize_t res = pread(fd_src, buf.data(), size, offset);
		if (res < 0 || (unsigned)res != size)
			throw wibble::exception::File(src, "reading " + str::fmt(size) + " bytes");
		res = write(fd_dst, buf.data(), size);
		if (res < 0 || (unsigned)res != size)
			throw wibble::exception::File(dst, "writing " + str::fmt(size) + " bytes");

		// Reindex file from offset to w_off
		m_idx.relocate_data(file, offset, w_off);

		w_off += size;
	}

	void flush()
	{
		if (fd_src != -1 and close(fd_src) != 0)
			throw wibble::exception::File(src, "closing file");
		fd_src = -1;
		if (fd_dst != -1 and close(fd_dst) != 0)
			throw wibble::exception::File(dst, "closing file");
		fd_dst = -1;
	}
};

void Writer::repack(RepackAgent& a)
{
	// TODO: send info to RepackAgent

	// TODO: lock away writes, allow reads
	// TODO: delete all files not indexed
	// TODO: unlock writes

	// Get the sorted list of files with holes
	HoledFilesCollector hf(m_path);	
	m_idx.scan_files(hf, "file, reftime");
	hf.end();

	for (vector<string>::const_iterator f = hf.files.begin();
			f != hf.files.end(); ++f)
	{
		// TODO: lock away writes, allow reads

		Pending p = m_idx.beginTransaction();

		// Make a copy of the file with the right data in it, sorted by
		// reftime, and update the offsets in the index
		string pathname = str::joinpath(m_path, *f);
		string pntmp = pathname + ".repack";
		FileCopier copier(m_idx, pathname, pntmp);
		m_idx.scan_file(*f, copier, "reftime");
		copier.flush();

		// Rename the file with to final name
		if (rename(pntmp.c_str(), pathname.c_str()) < 0)
			throw wibble::exception::System("renaming " + pntmp + " to " + pathname);

		// Commit the changes on the database
		p.commit();

		// TODO: unlock writes
	}
}

void Writer::depthFirstVisit(Visitor& v)
{
	// TODO
#if 0
	auto_ptr<maint::RootDirectory> maint_root(maint::RootDirectory::create(m_cfg));
	v.enterDataset(*this);
	maint_root->depthFirstVisit(v);
	v.leaveDataset(*this);
#endif
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
