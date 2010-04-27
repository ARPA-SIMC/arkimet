/*
 * dataset/simple/writer - Writer for simple datasets with no duplicate checks
 *
 * Copyright (C) 2009--2010  ARPA-SIM <urpsim@smr.arpa.emr.it>
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

#include <arki/dataset/simple/writer.h>
#include <arki/dataset/simple/index.h>
#include <arki/dataset/simple/datafile.h>
#include <arki/dataset/targetfile.h>
#include <arki/dataset/maintenance.h>
#include <arki/types/assigneddataset.h>
#include <arki/summary.h>
#include <arki/types/reftime.h>
#include <arki/matcher.h>
#include <arki/utils/metadata.h>
#include <arki/utils/files.h>
#include <arki/utils/dataset.h>
#include <arki/utils/compress.h>
#include <arki/scan/any.h>
#include <arki/postprocess.h>
#include <arki/sort.h>
#include <arki/nag.h>

#include <wibble/exception.h>
#include <wibble/sys/fs.h>
#include <wibble/string.h>

#include <fstream>
#include <ctime>
#include <cstdio>

#include "config.h"

#ifdef HAVE_LUA
#include <arki/report.h>
#endif

using namespace std;
using namespace wibble;
using namespace arki;
using namespace arki::types;
using namespace arki::utils;

namespace arki {
namespace dataset {
namespace simple {

Writer::Writer(const ConfigFile& cfg)
	: WritableLocal(cfg), m_mft(0), m_tf(0)
{
	m_name = cfg.value("name");

	// Create the directory if it does not exist
	wibble::sys::fs::mkpath(m_path);

	m_tf = TargetFile::create(cfg);

	// If the index is missing, take note not to perform a repack until a
	// check is made
	if (!sys::fs::access(str::joinpath(m_path, "index.sqlite"), F_OK))
		files::createDontpackFlagfile(m_path);

	auto_ptr<simple::Manifest> mft = simple::Manifest::create(m_path);
	m_mft = mft.release();
	m_mft->openRW();
}

Writer::~Writer()
{
	flush();
	if (m_tf) delete m_tf;
	if (m_mft) delete m_mft;
}

std::string Writer::id(const Metadata& md) const
{
	return std::string();
}

Datafile* Writer::file(const std::string& pathname)
{
	std::map<std::string, Datafile*>::iterator i = m_df_cache.find(pathname);
	if (i != m_df_cache.end())
		return i->second;

	// Ensure that the directory for 'pathname' exists
	string pn = str::joinpath(m_path, pathname);
	size_t pos = pn.rfind('/');
	if (pos != string::npos)
		wibble::sys::fs::mkpath(pn.substr(0, pos));

	if (!sys::fs::access(pn, F_OK) && sys::fs::access(pn + ".gz", F_OK))
		throw wibble::exception::Consistency("accessing data file " + pathname,
				"cannot update compressed data files: please manually uncompress it first");

	Datafile* res = new Datafile(pn);
	m_df_cache.insert(make_pair(pathname, res));
	return res;
}

WritableDataset::AcquireResult Writer::acquire(Metadata& md)
{
	// TODO: refuse if md is before "archive age"

	string reldest = (*m_tf)(md) + "." + md.source->format;
	Datafile* df = file(reldest);

	// Try appending
	UItem<types::AssignedDataset> oldads = md.get<types::AssignedDataset>();
	md.set(types::AssignedDataset::create(m_name, ""));

	try {
		df->append(md);
		return ACQ_OK;
	} catch (std::exception& e) {
		// sqlite will take care of transaction consistency
		if (oldads.defined())
			md.set(oldads);
		else
			md.unset(types::TYPE_ASSIGNEDDATASET);
		md.add_note(types::Note::create("Failed to store in dataset '"+m_name+"': " + e.what()));
		return ACQ_ERROR;
	}

	// After appending, keep updated info in-memory, and update manifest on
	// flush when the Datafile structures are deallocated
}

bool Writer::replace(Metadata& md)
{
	// Same as acquire
	return acquire(md) == ACQ_OK;
}

void Writer::remove(const std::string& id)
{
	// Nothing to do
}

void Writer::maintenance(maintenance::MaintFileVisitor& v, bool quick)
{
	m_mft->check(v, quick);
}

void Writer::flush()
{
	for (std::map<std::string, Datafile*>::iterator i = m_df_cache.begin();
			i != m_df_cache.end(); ++i)
	{
		m_mft->acquire(i->first, utils::files::timestamp(i->second->pathname), i->second->sum);
		delete i->second;
	}
	m_df_cache.clear();
	m_mft->flush();
}

void Writer::repack(std::ostream& log, bool writable)
{
	// TODO Sort data in reftime order
	// TODO Regenerate summary or metadata if timestamp show it's needed
	// TODO Move files to archive
	auto_ptr<maintenance::Agent> repacker;

	if (writable)
		// No safeguard against a deleted index: we catch that in the
		// constructor and create the don't pack flagfile
		repacker.reset(new maintenance::RealRepacker(log, *this));
	else
		repacker.reset(new maintenance::MockRepacker(log, *this));

	maintenance(*repacker);
	repacker->end();
}

void Writer::check(std::ostream& log, bool fix, bool quick)
{
	// TODO Regenerate summary or metadata if timestamp show it's needed
	if (fix)
	{
		maintenance::RealFixer fixer(log, *this);
		maintenance(fixer, quick);
		fixer.end();
	} else {
		maintenance::MockFixer fixer(log, *this);
		maintenance(fixer, quick);
		fixer.end();
	}
}

void Writer::rescanFile(const std::string& relpath)
{
	string pathname = str::joinpath(m_path, relpath);

	// Temporarily uncompress the file for scanning
	auto_ptr<utils::compress::TempUnzip> tu;
	if (!sys::fs::access(pathname, F_OK) && sys::fs::access(pathname + ".gz", F_OK))
		tu.reset(new utils::compress::TempUnzip(pathname));

	// Read the timestamp
	time_t mtime = files::timestamp(pathname);
	if (mtime == 0)
		throw wibble::exception::Consistency("acquiring " + pathname, "file does not exist");

	// Scan the file
	utils::metadata::Collector mds;
	if (!scan::scan(pathname, mds))
		throw wibble::exception::Consistency("rescanning " + pathname, "it does not look like a file we can scan");

	// Iterate the metadata, computing the summary and making the data
	// paths relative
	Summary sum;
	for (utils::metadata::Collector::const_iterator i = mds.begin();
			i != mds.end(); ++i)
	{
		Item<source::Blob> s = i->source.upcast<source::Blob>();
		s->filename = str::basename(s->filename);
		sum.add(*i);
	}

	// Regenerate .metadata
	mds.writeAtomically(pathname + ".metadata");

	// Regenerate .summary
	sum.writeAtomically(pathname + ".summary");

	// Add to manifest
	m_mft->acquire(relpath, mtime, sum);
}

size_t Writer::repackFile(const std::string& relpath)
{
	// TODO
	throw wibble::exception::Consistency("repacking " + relpath, "function to be implemented");
}

size_t Writer::removeFile(const std::string& relpath, bool withData)
{
	m_mft->remove(relpath);
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
	// TODO
	throw wibble::exception::Consistency("archiving " + relpath, "function to be implemented");
}

size_t Writer::vacuum()
{
	// Nothing to do here really
	return 0;
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
