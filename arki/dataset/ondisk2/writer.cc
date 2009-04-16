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
	createPackFlagfile(str::joinpath(m_path, file));

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

void Writer::maintenance(MaintenanceAgent& a)
{
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

void Writer::repack(RepackAgent& a)
{
	// TODO
#if 0
	auto_ptr<maint::RootDirectory> maint_root(maint::RootDirectory::create(m_cfg));

	maint_root->checkForRepack(a);
#endif
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
