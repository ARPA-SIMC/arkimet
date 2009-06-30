/*
 * dataset/ondisk/maint/directory - Local on disk dataset directory
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

#include <arki/dataset/ondisk/maint/directory.h>
#include <arki/dataset/ondisk/maint/datafile.h>
#include <arki/dataset/ondisk/maintenance.h>
#include <arki/dataset/targetfile.h>
#include <arki/metadata.h>
#include <arki/types/assigneddataset.h>
#include <arki/matcher.h>
#include <arki/utils.h>
#include <arki/utils/files.h>
#include <arki/summary.h>
#include <arki/configfile.h>

#include <wibble/exception.h>
#include <wibble/string.h>
#include <wibble/sys/fs.h>

#include <fstream>
#include <sstream>

#include <sys/types.h>
#include <sys/stat.h>
#include <fcntl.h>
#include <unistd.h>
#include <cerrno>

//#define DO_DEBUG_THIS
#ifdef DO_DEBUG_THIS
#include <iostream>
#endif

using namespace std;
using namespace wibble;
using namespace arki::utils::files;

namespace arki {
namespace dataset {
namespace ondisk {
namespace maint {

Directory::~Directory()
{
}

void Directory::rebuildSummary()
{
	// Rebuild the summary

	// Restart from scratch with the summary
	Summary summary;

	// Merge all the summaries from files and subdirectories
	string rootDir = fullpath();
	wibble::sys::fs::Directory dir(rootDir);
	for (wibble::sys::fs::Directory::const_iterator i = dir.begin();
			i != dir.end(); ++i)
	{
		// Skip '.', '..' and hidden files
		if ((*i)[0] == '.') continue;

		string path;
		if (utils::isdir(rootDir, i))
		{
			path = str::joinpath(str::joinpath(rootDir, *i), "summary");
		} else if (str::endsWith(*i, ".metadata")) {
			path = str::joinpath(rootDir, (*i).substr(0, (*i).size() - 9) + ".summary");
		} else
			continue;
		Summary sub;
		Directory::readSummary(sub, path);
		//cerr << "Read " << path << ": count " << sub.count() << " size: " << sub.size() << endl;
		summary.add(sub);
	}

	Directory::writeSummary(summary, str::joinpath(rootDir, "summary"));
	//cerr << "Writing " << str::joinpath(rootDir, "summary") << ": count " << summary.count() << " size: " << summary.size() << endl;
}

void Directory::invalidateAll()
{
	string rootDir = fullpath();
	wibble::sys::fs::Directory dir(rootDir);

	utils::removeFlagfile(str::joinpath(rootDir, "summary"));

	for (wibble::sys::fs::Directory::const_iterator i = dir.begin();
			i != dir.end(); ++i)
	{
		// Skip '.', '..' and hidden files
		if ((*i)[0] == '.') continue;

		if (utils::isdir(rootDir, i))
		{
			SubDirectory sub(this, *i);
			sub.invalidateAll();
		}
		else if (str::endsWith(*i, FLAGFILE_REBUILD) || str::endsWith(*i, ".metadata"))
		{
			string name = (*i).substr(0, (*i).rfind('.'));
			string pathname = wibble::str::joinpath(rootDir, name);
			createRebuildFlagfile(pathname);
			utils::removeFlagfile(pathname + ".summary");
			utils::removeFlagfile(pathname + ".metadata");
		}
	}
}

#if 0
struct DataFileChecker : public Visitor
{
	MaintenanceAgent& a;
	stack<time_t> maxts;

	DataFileChecker(MaintenanceAgent& a) : a(a) {}

	virtual void enterDirectory(Directory& dir)
	{
		maxts.push(0);
	}
	virtual void leaveDirectory(Directory& dir)
	{
	}
	virtual void visitFile(Datafile& df)
	{
		if (hasRebuildFlagfile(df.pathname))
			a.needsDatafileRebuild(df);
	}
};
#endif

void Directory::checkDirectories(MaintenanceAgent& a)
{
	string rootDir = fullpath();
	wibble::sys::fs::Directory dir(rootDir);

	// Separate dirs from files, so we can go depth-first
	set<string> files;

	time_t maxSummaryTS = 0;
	for (wibble::sys::fs::Directory::const_iterator i = dir.begin();
			i != dir.end(); ++i)
	{
		// Skip '.', '..' and hidden files
		if ((*i)[0] == '.') continue;

		if (utils::isdir(rootDir, i))
		{
			// Process the dirs right away
			SubDirectory sub(this, *i);
			sub.checkDirectories(a);

			// Take note of the maximum mtime of all subdir's summaries
			time_t subdirSummaryTS = timestamp(wibble::str::joinpath(sub.fullpath(), "summary"));
			if (subdirSummaryTS > maxSummaryTS)
				maxSummaryTS = subdirSummaryTS;
		}
		else if (str::endsWith(*i, FLAGFILE_REBUILD) || str::endsWith(*i, ".metadata"))
		{
			// Process the files later
			string name = (*i).substr(0, (*i).rfind('.'));
			files.insert(name);
		}
	}

	// Then files
	for (set<string>::const_iterator i = files.begin();
			i != files.end(); ++i)
	{
		Datafile df(this, *i);

		//if (timestamp(df.pathname + ".metadata") > timestamp(df.pathname + ".summary"))
		//	a.needsSummaryRebuild(df);

		time_t fileSummaryTS = timestamp(df.pathname + ".summary");
		if (fileSummaryTS > maxSummaryTS)
			maxSummaryTS = fileSummaryTS;
	}

	// If the any depending summary is newer than this dir's summary, this
	// dir's summary needs to be rebuilt
	if (maxSummaryTS > timestamp(wibble::str::joinpath(rootDir, "summary")))
		a.needsSummaryRebuild(*this);
}

struct DeleteSummary : public Visitor
{
	virtual void enterDirectory(Directory& dir)
	{
	}

	virtual void leaveDirectory(Directory& dir)
	{
		utils::removeFlagfile(wibble::str::joinpath(dir.fullpath(), "summary"));
	}

	virtual void visitFile(Datafile& file)
	{
		utils::removeFlagfile(file.pathname + ".summary");
	}
};

void Directory::deleteSummaries()
{
	DeleteSummary ds;
	depthFirstVisit(ds);
}

void Directory::depthFirstVisit(Visitor& v)
{
	v.enterDirectory(*this);

	string rootDir = fullpath();
	wibble::sys::fs::Directory dir(rootDir);

	// Separate dirs from files, so we can go depth-first
	set<string> files;

	// Directories first
	for (wibble::sys::fs::Directory::const_iterator i = dir.begin();
			i != dir.end(); ++i)
	{
		// Skip '.', '..' and hidden files
		if ((*i)[0] == '.') continue;

		if (utils::isdir(rootDir, i))
		{
			// Process the dirs right away
			SubDirectory sub(this, *i);
			sub.depthFirstVisit(v);
		}
		else if (str::endsWith(*i, FLAGFILE_REBUILD) || str::endsWith(*i, ".metadata"))
			// Process the files later
			files.insert((*i).substr(0, (*i).rfind('.')));
	}

	// Then files
	for (set<string>::const_iterator i = files.begin();
			i != files.end(); ++i)
	{
		Datafile df(this, *i);
		v.visitFile(df);
	}

	v.leaveDirectory(*this);
}

void Directory::reindex(MetadataConsumer& salvage)
{
	string rootDir = fullpath();
	wibble::sys::fs::Directory dir(rootDir);
	for (wibble::sys::fs::Directory::const_iterator i = dir.begin();
			i != dir.end(); ++i)
	{
		// Skip '.', '..' and hidden files
		if ((*i)[0] == '.') continue;

		if (utils::isdir(rootDir, i))
		{
			SubDirectory sub(this, *i);
			sub.reindex(salvage);
		} else if (str::endsWith(*i, ".metadata")) {
			string name = (*i).substr(0, (*i).size() - 9);
			if (hasRebuildFlagfile(name))
				continue;
			Datafile df(this, name);
			df.reindex(salvage);
		}
	}
}

void Directory::readSummary(Summary& s, const std::string& fname)
{
	ifstream in;
	in.open(fname.c_str(), ios::in);
	if (!in.is_open() || in.fail())
		throw wibble::exception::File(fname, "opening file for reading");

	s.read(in, fname);
}

void Directory::writeSummary(const Summary& s, const std::string& fname)
{
	string enc = s.encode();
	string tmpfile = fname + ".tmp" + str::fmt(getpid());
	int fd = open(tmpfile.c_str(), O_WRONLY | O_CREAT | O_EXCL, 0666);
	if (fd == -1)
		throw wibble::exception::File(tmpfile, "creating temporary file for the summary");
	try {
		int res = write(fd, enc.data(), enc.size());
		if (res < 0 || (unsigned)res != enc.size())
			throw wibble::exception::File(tmpfile, "writing " + str::fmt(enc.size()) + " bytes to the file");
		if (close(fd) == -1)
		{
			fd = -1;
			throw wibble::exception::File(tmpfile, "closing file");
		}
		fd = -1;
		if (rename(tmpfile.c_str(), fname.c_str()) == -1)
			throw wibble::exception::System("Renaming " + tmpfile + " into " + fname);
	} catch (...) {
		if (fd != -1)
			close(fd);
		throw;
	}
}


RootDirectory::RootDirectory(const ConfigFile& cfg)
	: m_path(cfg.value("path")), m_name(cfg.value("name"))
{
	// Create the directory if it does not exist
	wibble::sys::fs::mkpath(fullpath());
}

RootDirectory::~RootDirectory()
{
}

IndexedRootDirectory::IndexedRootDirectory(const ConfigFile& cfg)
	: RootDirectory(cfg), m_idx(cfg)
{
	if (!utils::hasFlagfile(str::joinpath(m_path, "index.sqlite")))
		createIndexFlagfile(m_path);
	m_idx.open();
}

IndexedRootDirectory::~IndexedRootDirectory()
{
	if (m_trans) m_trans.rollback();
}

void IndexedRootDirectory::beginTransactionIfNeeded()
{
	if (m_trans)
		return;
	//cerr << "Begin transaction" << endl;
	m_trans = m_idx.beginTransaction();
	createIndexFlagfile(m_path);
}

void RootDirectory::addToIndex(Metadata& md, const std::string& file, size_t ofs)
{
	// Nothing to do: we don't have an index
	//cerr << "RootDirectory::addToIndex doing nothing" << endl;
}
void RootDirectory::commit()
{
	//cerr << "RootDirectory::commit doing nothing" << endl;
}

void RootDirectory::resetIndex(const std::string& file)
{
	// Nothing to do: we don't have an index
}

void RootDirectory::invalidateAll()
{
	Directory::invalidateAll();
}

void IndexedRootDirectory::commit()
{
	/*
	if (m_trans)
		cerr << "IndexedRootDirectory::commit committing" << endl;
	else
		cerr << "IndexedRootDirectory::nothing to commit" << endl;
		*/
	RootDirectory::commit();
	m_trans.commit();
}

void IndexedRootDirectory::addToIndex(Metadata& md, const std::string& file, size_t ofs)
{
	beginTransactionIfNeeded();

	//cerr << "IRD addTOIndex " << file << " ofs " << ofs << endl;

	m_idx.index(md, file, ofs);
}

std::string IndexedRootDirectory::id(const Metadata& md) const
{
	return m_idx.id(md);
}

void IndexedRootDirectory::resetIndex(const std::string& file)
{
	beginTransactionIfNeeded();

	m_idx.reset(file);
}

void IndexedRootDirectory::invalidateAll()
{
	recreateIndex();

	RootDirectory::invalidateAll();
}

struct RepackChecker : public Visitor
{
	RepackAgent& a;

	RepackChecker(RepackAgent& a) : a(a) {}

	virtual void enterDirectory(Directory& dir) {}
	virtual void leaveDirectory(Directory& dir) {}
	virtual void visitFile(Datafile& df)
	{
		// If a data file needs rebuild, don't mess with it
		if (hasRebuildFlagfile(df.pathname))
			return;

		if (hasPackFlagfile(df.pathname))
			a.needsRepack(df);
	}
};

void RootDirectory::checkForRepack(RepackAgent& a)
{
	RepackChecker rc(a);
	depthFirstVisit(rc);
}

void IndexedRootDirectory::checkForRepack(RepackAgent& a)
{
	if (m_trans)
		throw wibble::exception::Consistency("starting repack run", "there are unflushed transactions: please finish updating the dataset before running repack");
	RootDirectory::checkForRepack(a);
}

bool RootDirectory::checkMainIndex(MaintenanceAgent& a)
{
	// We have no main index, so nothing needs doing
	return false;
}
bool IndexedRootDirectory::checkMainIndex(MaintenanceAgent& a)
{
	if (m_trans)
		throw wibble::exception::Consistency("starting main index check", "there are unflushed transactions: please finish updating the dataset before running maintenance");

	if (RootDirectory::checkMainIndex(a))
		return true;

	if (hasIndexFlagfile(m_path) || !utils::hasFlagfile(str::joinpath(m_path, "index.sqlite")))
	{
		a.needsFullIndexRebuild(*this);
		return true;
	}
	return false;
}

struct DataFileChecker : public Visitor
{
	MaintenanceAgent& a;
	bool full_reindex;

	DataFileChecker(MaintenanceAgent& a, bool full_reindex) : a(a), full_reindex(full_reindex) {}

	virtual void enterDirectory(Directory& dir) {}
	virtual void leaveDirectory(Directory& dir) {}
	virtual void visitFile(Datafile& df)
	{
		if (hasRebuildFlagfile(df.pathname))
			a.needsDatafileRebuild(df);
		else if (timestamp(df.pathname + ".metadata") > timestamp(df.pathname + ".summary"))
			a.needsSummaryRebuild(df);
		else if (full_reindex)
			a.needsReindex(df);
	}
};
void RootDirectory::checkDataFiles(MaintenanceAgent& a, bool full_reindex)
{
	DataFileChecker dfc(a, full_reindex);
	depthFirstVisit(dfc);
}
void IndexedRootDirectory::checkDataFiles(MaintenanceAgent& a, bool full_reindex)
{
	if (m_trans)
		throw wibble::exception::Consistency("starting data file check", "there are unflushed transactions: please finish updating the dataset before running maintenance");
	RootDirectory::checkDataFiles(a, full_reindex);
}

void RootDirectory::checkDirectories(MaintenanceAgent& a)
{
	Directory::checkDirectories(a);
}
void IndexedRootDirectory::checkDirectories(MaintenanceAgent& a)
{
	RootDirectory::checkDirectories(a);
}

void IndexedRootDirectory::recreateIndex()
{
	// Reset the whole index
	createIndexFlagfile(m_path);
	m_idx.reset();
}


SubDirectory::SubDirectory(Directory* parent, const std::string& path)
	: parent(parent), path(path)
{
	// Create the directory if it does not exist
	wibble::sys::fs::mkpath(fullpath());
}

void SubDirectory::addToIndex(Metadata& md, const std::string& file, size_t ofs)
{
	//cerr << "Add to index: " << file << " ofs " << ofs << endl;
	parent->addToIndex(md, file, ofs);
}

std::string SubDirectory::id(const Metadata& md) const
{
	return parent->id(md);
}

void SubDirectory::resetIndex(const std::string& file)
{
	parent->resetIndex(file);
}

void SubDirectory::commit()
{
	parent->commit();
}

std::string RootDirectory::fullpath() const
{
	return m_path;
}

std::string RootDirectory::relpath() const
{
	return string();
}

std::string SubDirectory::fullpath() const
{
	return wibble::str::joinpath(parent->fullpath(), path);
}

std::string SubDirectory::relpath() const
{
	return wibble::str::joinpath(parent->relpath(), path);
}

void RootDirectory::recreateIndex()
{
}

auto_ptr<RootDirectory> RootDirectory::create(const ConfigFile& cfg)
{
	// If there is no 'index' in the config file, don't index anything
	if (cfg.value("index") != string())
	{
		//cerr << "MAKE INDEXED MAINT" << endl;
		return auto_ptr<RootDirectory>(new IndexedRootDirectory(cfg));
	}
	else
	{
		//cerr << "MAKE NONINDEXED MAINT" << endl;
		return auto_ptr<RootDirectory>(new RootDirectory(cfg));
	}
}

}
}
}
}
// vim:set ts=4 sw=4:
