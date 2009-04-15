/*
 * dataset/ondisk/writer/directory - Local on disk dataset directory
 *
 * Copyright (C) 2007,2008  ARPA-SIM <urpsim@smr.arpa.emr.it>
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

#include <arki/dataset/ondisk/writer/directory.h>
#include <arki/dataset/ondisk/writer/datafile.h>
#include <arki/dataset/ondisk/common.h>
#include <arki/dataset/ondisk/maintenance.h>
#include <arki/dataset/targetfile.h>
#include <arki/metadata.h>
#include <arki/types/assigneddataset.h>
#include <arki/matcher.h>
#include <arki/utils.h>
#include <arki/summary.h>
#include <arki/configfile.h>

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

namespace arki {
namespace dataset {
namespace ondisk {
namespace writer {

Directory::Directory()
	: m_summary(0), summaryIsInvalid(false)
{
};

RootDirectory::RootDirectory(const ConfigFile& cfg)
	: m_path(cfg.value("path")), m_name(cfg.value("name")), m_tf(0), m_lockfile(0)
{
	m_tf = TargetFile::create(cfg);

	// Create the directory if it does not exist
	wibble::sys::fs::mkpath(fullpath());

	m_lockfile = new sys::fs::Lockfile(fullpath() + "/lock");
}

RootDirectory::~RootDirectory()
{
	if (m_tf) delete m_tf;
	if (m_lockfile) delete m_lockfile;
}

IndexedRootDirectory::IndexedRootDirectory(const ConfigFile& cfg)
	: RootDirectory(cfg), m_idx(cfg)
{
	m_idx.open();
}

IndexedRootDirectory::~IndexedRootDirectory()
{
	if (m_trans) m_trans.rollback();
}

std::string RootDirectory::id(const Metadata& md) const
{
	return string();
}

std::string IndexedRootDirectory::id(const Metadata& md) const
{
	return m_idx.id(md);
}

void IndexedRootDirectory::beginTransactionIfNeeded()
{
	if (m_trans)
		return;
	m_trans = m_idx.beginTransaction();
	createIndexFlagfile(m_path);
}

void RootDirectory::addToIndex(Metadata& md, const std::string& file, size_t ofs)
{
	// Nothing to do: we don't have an index
}

void IndexedRootDirectory::addToIndex(Metadata& md, const std::string& file, size_t ofs)
{
	beginTransactionIfNeeded();

	m_idx.index(md, file, ofs);
}

void RootDirectory::acquire(Metadata& md)
{
	string reldest = (*m_tf)(md) + "." + md.source->format;
	Datafile* dest = file(reldest);
	string newid = id(md);

	md.set(types::AssignedDataset::create(m_name, newid));

	dest->append(md);
}

void IndexedRootDirectory::acquire(Metadata& md)
{
	string reldest = (*m_tf)(md) + "." + md.source->format;
	Datafile* dest = file(reldest);
	string newid = id(md);

	md.set(types::AssignedDataset::create(m_name, newid));

	addToIndex(md, reldest, dest->nextOffset());
	dest->append(md);
}

void RootDirectory::replace(Metadata& md)
{
	// If we have no index, replace is the same as acquire
	acquire(md);
}

void IndexedRootDirectory::replace(Metadata& md)
{
	// Get the old position
	string oldfilename;
	size_t oldofs;
	if (!m_idx.fetch(md, oldfilename, oldofs))
	{
		// If the metadata is not in the dataset, just call acquire()
		acquire(md);
		return;
	}
	Datafile* old = file(oldfilename);

	string reldest = (*m_tf)(md) + "." + md.source->format;
	Datafile* dest = file(reldest);
	string newid = id(md);

	md.set(types::AssignedDataset::create(m_name, newid));

	// Note that Datafile instances are always shared, so old and dest can be
	// the same (and usually are)

	beginTransactionIfNeeded();

	m_idx.replace(md, reldest, dest->nextOffset());
	dest->append(md);
	old->remove(oldofs);

	// TODO: do something so that a following flush won't remove the
	// flagfile of the Datafile that failed here
}

void RootDirectory::testAcquire(const ConfigFile& cfg, const Metadata& md, std::ostream& out)
{
	auto_ptr<TargetFile> tf(TargetFile::create(cfg));
	string dest = wibble::str::joinpath(cfg.value("path"), (*tf)(md) + "." + md.source->format);
	if (hasRebuildFlagfile(dest))
		throw wibble::exception::Consistency("acquiring metadata to " + dest, "destination file needs rebuilding");
	out << "Appended to file " << dest << endl;
}
void RootDirectory::testReplace(const ConfigFile& cfg, const Metadata& md, std::ostream& out)
{
	// If we have no index, replace is the same as acquire
	testAcquire(cfg, md, out);
}
void IndexedRootDirectory::testAcquire(const ConfigFile& cfg, const Metadata& md, std::ostream& out)
{
	auto_ptr<TargetFile> tf(TargetFile::create(cfg));
	string dest = wibble::str::joinpath(cfg.value("path"), (*tf)(md) + "." + md.source->format);
	if (hasRebuildFlagfile(dest))
		throw wibble::exception::Consistency("acquiring metadata to " + dest, "destination file needs rebuilding");
	RIndex idx(cfg);
	idx.open();
	string file; size_t ofs;
	if (idx.fetch(md, file, ofs))
		throw index::DuplicateInsert("Data is already in the dataset at " + file + ":" + str::fmt(ofs));
	out << "Appended to file " << dest << " with id " << idx.id(md) << endl;
}
void IndexedRootDirectory::testReplace(const ConfigFile& cfg, const Metadata& md, std::ostream& out)
{
	auto_ptr<TargetFile> tf(TargetFile::create(cfg));
	string dest = wibble::str::joinpath(cfg.value("path"), (*tf)(md) + "." + md.source->format);
	if (hasRebuildFlagfile(dest))
		throw wibble::exception::Consistency("acquiring metadata to " + dest, "destination file needs rebuilding");
	RIndex idx(cfg);
	idx.open();
	string file; size_t ofs;
	if (idx.fetch(md, file, ofs))
		out << "Deleting old data in file " << file << ":" << ofs << endl;

	out << "Appended to file " << dest << " with id " << idx.id(md) << endl;
}

void RootDirectory::remove(const std::string& id)
{
	// Nothing to do if we don't have an index
}

void IndexedRootDirectory::remove(const std::string& id)
{
	beginTransactionIfNeeded();

	// Start the remove transaction in the database
	string oldfilename;
	size_t oldofs;
	m_idx.remove(id, oldfilename, oldofs);

	// Remove from the data file
	Datafile* old = file(oldfilename);
	old->remove(oldofs);
}

void RootDirectory::flush()
{
	Directory::flush();
}

void IndexedRootDirectory::flush()
{
	RootDirectory::flush();

	m_trans.commit();
	removeIndexFlagfile(m_path);
}

SubDirectory::SubDirectory(Directory* parent, const std::string& path)
	: parent(parent), path(path)
{
	// Create the directory if it does not exist
	wibble::sys::fs::mkpath(fullpath());
}

Directory::~Directory()
{
	//fprintf(stderr, "Deleting %s\n", fullpath().c_str());
	for (map<string, Directory*>::const_iterator i = m_subdir_cache.begin();
			i != m_subdir_cache.end(); ++i)
	{
		//fprintf(stderr, "  dir %s\n", i->second->fullpath().c_str());
		if (i->second)
			delete i->second;
	}
	for (map<string, Datafile*>::const_iterator i = m_files_cache.begin();
			i != m_files_cache.end(); ++i)
	{
		//fprintf(stderr, "  file %s\n", i->second->pathname.c_str());
		if (i->second)
			delete i->second;
	}

	if (m_summary)
		delete m_summary;
}

Directory* Directory::subdir(const std::string& path)
{
	size_t pos = path.find('/');
	if (pos != string::npos)
	{
		// If it's not the final path, go on recursively
		Directory* first = subdir(path.substr(0, pos));
		return first->subdir(path.substr(pos+1));
	} else {
		// If we have it in cache, return it from the cache
		map<string, Directory*>::iterator i = m_subdir_cache.find(path);
		if (i != m_subdir_cache.end())
			return i->second;

		// Else, create it and add it to the cache
		SubDirectory* res = new SubDirectory(this, path);
		m_subdir_cache.insert(make_pair(path, res));
		return res;
	}
}

Datafile* Directory::file(const std::string& path)
{
	// Split the path in dir and name
	size_t pos = path.rfind('/');
	// If there is a directory, let that directory instantiate the Datafile
	if (pos != string::npos)
	{
		Directory* parent = subdir(path.substr(0, pos));
		return parent->file(path.substr(pos+1));
	}

	// We instantiate the datafile

	// Split the name in name and ext
	pos = path.rfind('.');
	if (pos == string::npos)
		// We need an extension to represent the type of data within
		throw wibble::exception::Consistency("accessing file " + path, "file name has no extension");
	
	// If we have it in cache, return it from the cache
	map<string, Datafile*>::iterator i = m_files_cache.find(path);
	if (i != m_files_cache.end())
		return i->second;

	// Else, create it and add it to the cache
	Datafile* res = new Datafile(this, path.substr(0, pos), path.substr(pos+1));
	m_files_cache.insert(make_pair(path, res));
	return res;
}

void Directory::addToSummary(Metadata& md)
{
	// If the summary has been invalidated, do nothing: we have to rebuild
	// it anyway
	if (summaryIsInvalid)
		return;

	// Load the summary if it hasn't been loaded yet
	if (!m_summary)
		summary();

	// Add the metadata to the summary
	m_summary->add(md);
}

void SubDirectory::addToSummary(Metadata& md)
{
	Directory::addToSummary(md);

	// Propagate to parent
	parent->addToSummary(md);
}

void SubDirectory::addToIndex(Metadata& md, const std::string& file, size_t ofs)
{
	parent->addToIndex(md, file, ofs);
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

void Directory::flush()
{
	//fprintf(stderr, "Flushing %s\n", fullpath().c_str());

	// Call flush() on all directories and empty the cache
	for (map<string, Directory*>::iterator i = m_subdir_cache.begin();
			i != m_subdir_cache.end(); ++i)
	{
		//fprintf(stderr, "  dir %s\n", i->second->fullpath().c_str());
		i->second->flush();
	}

	// Call done() on all datafiles and empty the cache
	for (map<string, Datafile*>::iterator i = m_files_cache.begin();
			i != m_files_cache.end(); ++i)
	{
		//fprintf(stderr, "  file %s\n", i->second->pathname.c_str());
		delete i->second;
		i->second = 0;
	}
	m_files_cache.clear();

	Directory::writeSummary(summary(), wibble::str::joinpath(fullpath(), "summary"));
}

void Directory::releaseUnused()
{
	for (map<string, Datafile*>::iterator i = m_files_cache.begin();
			i != m_files_cache.end(); ++i)
	{
		//fprintf(stderr, "  file %s\n", i->second->pathname.c_str());
		if (i->second)
		{
			delete i->second;
			i->second = 0;
		}
	}
	m_files_cache.clear();
}

const Summary& Directory::summary()
{
	if (summaryIsInvalid)
	{
		// Rebuild the summary
		
		// Restart from scratch with the summary
		if (m_summary) delete m_summary;
		m_summary = new Summary;

		// Scan the directory tree, instantiating Directory and Datafile
		// objects and getting the summary out of them, then merging them in

		string rootDir = fullpath();
		wibble::sys::fs::Directory dir(rootDir);
		for (wibble::sys::fs::Directory::const_iterator i = dir.begin();
				i != dir.end(); ++i)
		{
			// Skip '.', '..' and hidden files
			if ((*i)[0] == '.') continue;

			if (utils::isdir(rootDir, i))
			{
				Directory* d = subdir(*i);
				m_summary->add(d->summary());
			} else if (str::endsWith(*i, ".metadata")) {
				Datafile* f = file((*i).substr(0, (*i).size() - 9));
				m_summary->add(f->summary());
			}
		}
		summaryIsInvalid = false;
	} else if (!m_summary) {
		m_summary = new Summary;
		Directory::readSummary(*m_summary, wibble::str::joinpath(fullpath(), "summary"));
	}
	return *m_summary;
}

void Directory::readSummary(Summary& s, const std::string& fname)
{
	ifstream in;
	in.open(fname.c_str(), ios::in);
	if (!in.is_open() || in.fail())
	{
		if (errno == ENOENT)
			// If the file is missing, we just leave s empty
			return;
		else
			throw wibble::exception::File(fname, "opening file for reading");
	}

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

void Directory::invalidateSummary()
{
	summaryIsInvalid = true;
	if (m_summary)
	{
		delete m_summary;
		m_summary = 0;
	}
}

void SubDirectory::invalidateSummary()
{
	Directory::invalidateSummary();
	parent->invalidateSummary();
}


}
}
}
}
// vim:set ts=4 sw=4:
