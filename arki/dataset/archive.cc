/*
 * dataset/archive - Handle archived data
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

#include <arki/dataset/archive.h>
#include <arki/dataset/simple/index.h>
#include <arki/dataset/simple/reader.h>
#include <arki/dataset/maintenance.h>
#include <arki/summary.h>
#include <arki/types/reftime.h>
#include <arki/matcher.h>
#include <arki/utils/metadata.h>
#include <arki/utils/files.h>
#include <arki/utils/dataset.h>
#include <arki/utils/compress.h>
#include <arki/scan/any.h>
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

Archive::Archive(const std::string& dir)
	: m_dir(dir), m_mft(0)
{
	// Create the directory if it does not exist
	wibble::sys::fs::mkpath(m_dir);
}

Archive::~Archive()
{
	if (m_mft) delete m_mft;
}

bool Archive::is_archive(const std::string& dir)
{
	return simple::Manifest::exists(dir);
}

void Archive::openRO()
{
	auto_ptr<simple::Manifest> mft = simple::Manifest::create(m_dir);
	m_mft = mft.release();
	m_mft->openRO();
}

void Archive::openRW()
{
	auto_ptr<simple::Manifest> mft = simple::Manifest::create(m_dir);
	m_mft = mft.release();
	m_mft->openRW();
}

void Archive::queryData(const dataset::DataQuery& q, MetadataConsumer& consumer)
{
	m_mft->queryData(q, consumer);
}

void Archive::queryBytes(const dataset::ByteQuery& q, std::ostream& out)
{
	m_mft->queryBytes(q, out);
}

void Archive::querySummary(const Matcher& matcher, Summary& summary)
{
	m_mft->querySummary(matcher, summary);
}

void Archive::acquire(const std::string& relname)
{
	if (!m_mft) throw wibble::exception::Consistency("acquiring into archive " + m_dir, "archive opened in read only mode");
	// Scan file, reusing .metadata if still valid
	utils::metadata::Collector mdc;
	string pathname = str::joinpath(m_dir, relname);
	if (!scan::scan(pathname, mdc))
		throw wibble::exception::Consistency("acquiring " + pathname, "it does not look like a file we can acquire");
	acquire(relname, mdc);
}

void Archive::acquire(const std::string& relname, const utils::metadata::Collector& mds)
{
	if (!m_mft) throw wibble::exception::Consistency("acquiring into archive " + m_dir, "archive opened in read only mode");
	string pathname = str::joinpath(m_dir, relname);
	time_t mtime = files::timestamp(pathname);
	if (mtime == 0)
		throw wibble::exception::Consistency("acquiring " + pathname, "file does not exist");

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
	m_mft->acquire(relname, mtime, sum);
}

void Archive::flush()
{
	if (m_mft) m_mft->flush();
}

namespace {
struct ToArchiveState : public maintenance::MaintFileVisitor
{
	maintenance::MaintFileVisitor& next;

	ToArchiveState(maintenance::MaintFileVisitor& next) : next(next) {}

	virtual void operator()(const std::string& file, State state)
	{
		switch (state)
		{
			case OK:		next(file, ARC_OK); break;
			case TO_ARCHIVE:	next(file, ARC_OK); break;
			case TO_DELETE:		next(file, ARC_OK); break;
			case TO_PACK:		next(file, ARC_OK); break;
			case TO_INDEX:		next(file, ARC_TO_INDEX); break;
			case TO_RESCAN:		next(file, ARC_TO_RESCAN); break;
			case DELETED:		next(file, ARC_DELETED); break;
			case ARC_OK:
			case ARC_TO_INDEX:
			case ARC_TO_RESCAN:
			case ARC_DELETED:	next(file, state); break;
			default:
				throw wibble::exception::Consistency("checking archive", "programming error: found unsupported file state");
		}
	}
};
}

void Archive::maintenance(maintenance::MaintFileVisitor& v)
{
	ToArchiveState tas(v);
	m_mft->check(tas);
	m_mft->flush();
}

/*
void Archive::repack(std::ostream& log, bool writable)
{
}

void Archive::check(std::ostream& log)
{
}
*/

void Archive::remove(const std::string& relname)
{
	if (!m_mft) throw wibble::exception::Consistency("removing file from " + m_dir, "archive opened in read only mode");
	// Remove from index and from file system, including attached .metadata
	// and .summary, if they exist
	string pathname = str::joinpath(m_dir, relname);

	sys::fs::deleteIfExists(pathname + ".summary");
	sys::fs::deleteIfExists(pathname + ".metadata");
	//sys::fs::deleteIfExists(pathname);

	deindex(relname);
}

void Archive::deindex(const std::string& relname)
{
	if (!m_mft) throw wibble::exception::Consistency("deindexing file from " + m_dir, "archive opened in read only mode");
	m_mft->remove(relname);
}

void Archive::rescan(const std::string& relname)
{
	if (!m_mft) throw wibble::exception::Consistency("rescanning file in " + m_dir, "archive opened in read only mode");
	string pathname = str::joinpath(m_dir, relname);
	time_t ts_data = files::timestamp(pathname);
	if (ts_data == 0)
		ts_data = files::timestamp(pathname + ".gz");
	time_t ts_md = files::timestamp(pathname + ".metadata");

	// Invalidate summary
	sys::fs::deleteIfExists(pathname + ".summary");

	// Invalidate metadata if older than data
	if (ts_md < ts_data)
		sys::fs::deleteIfExists(pathname + ".metadata");

	// Deindex the file
	deindex(relname);

	// Temporarily uncompress the file for scanning
	auto_ptr<utils::compress::TempUnzip> tu;
	if (!sys::fs::access(pathname, F_OK) && sys::fs::access(pathname + ".gz", F_OK))
		tu.reset(new utils::compress::TempUnzip(pathname));

	// Reindex the file
	acquire(relname);
}

void Archive::vacuum()
{
	if (!m_mft) throw wibble::exception::Consistency("vacuuming " + m_dir, "archive opened in read only mode");
	// If archive dir is not writable, skip this section
	if (!sys::fs::access(m_dir, W_OK)) return;

	m_mft->vacuum();

	// Regenerate summary cache if needed
	Summary s;
	m_mft->querySummary(Matcher(), s);
}

Archives::Archives(const std::string& dir, bool read_only)
	: m_dir(dir), m_read_only(read_only), m_last(0)
{
	// Create the directory if it does not exist
	wibble::sys::fs::mkpath(m_dir);

	// Look for subdirectories: they are archives
	sys::fs::Directory d(m_dir);
	for (sys::fs::Directory::const_iterator i = d.begin();
			i != d.end(); ++i)
	{
		// Skip '.', '..' and hidden files
		if ((*i)[0] == '.')
			continue;
		string pathname = str::joinpath(m_dir, *i);
		if (!sys::fs::isDirectory(pathname))
			continue;
		if (read_only && !Archive::is_archive(pathname))
			continue;

		Archive* a = 0;
		if (*i == "last")
			m_last = a = new Archive(pathname);
		else
			m_archives.insert(make_pair(*i, a = new Archive(pathname)));

		if (a)
		{
			if (read_only)
				a->openRO();
			else
				a->openRW();
		}
	}

	// Instantiate the 'last' archive even if the directory does not exist,
	// if not read only
	if (!read_only && !m_last)
	{
		m_last = new Archive(str::joinpath(m_dir, "last"));
		m_last->openRW();
	}
}

Archives::~Archives()
{
	for (map<string, Archive*>::iterator i = m_archives.begin();
			i != m_archives.end(); ++i)
		delete i->second;
	if (m_last)
		delete m_last;
}

void Archives::flush()
{
	for (map<string, Archive*>::iterator i = m_archives.begin();
			i != m_archives.end(); ++i)
		i->second->flush();
	if (m_last)
		m_last->flush();
}

Archive* Archives::lookup(const std::string& name)
{
	if (name == "last")
		return m_last;

	std::map<std::string, Archive*>::iterator i = m_archives.find(name);
	if (i == m_archives.end())
		return 0;
	return i->second;
}

void Archives::queryData(const dataset::DataQuery& q, MetadataConsumer& consumer)
{
	for (map<string, Archive*>::iterator i = m_archives.begin();
			i != m_archives.end(); ++i)
		i->second->queryData(q, consumer);
	if (m_last)
		m_last->queryData(q, consumer);
}

void Archives::queryBytes(const dataset::ByteQuery& q, std::ostream& out)
{
	for (map<string, Archive*>::iterator i = m_archives.begin();
			i != m_archives.end(); ++i)
		i->second->queryBytes(q, out);
	if (m_last)
		m_last->queryBytes(q, out);
}

void Archives::querySummary(const Matcher& matcher, Summary& summary)
{
	for (map<string, Archive*>::iterator i = m_archives.begin();
			i != m_archives.end(); ++i)
		i->second->querySummary(matcher, summary);
	if (m_last)
		m_last->querySummary(matcher, summary);
}

static std::string poppath(std::string& path)
{
	size_t start = 0;
	while (start < path.size() && path[start] == '/')
		++start;
	size_t end = start;
	while (end < path.size() && path[end] != '/')
		++end;
	size_t nstart = end;
	while (nstart < path.size() && path[nstart] == '/')
		++nstart;
	string res = path.substr(start, end-start);
	path = path.substr(nstart);
	return res;
}

void Archives::acquire(const std::string& relname)
{
	string path = relname;
	string name = poppath(path);
	if (Archive* a = lookup(name))
		a->acquire(path);
	else
		throw wibble::exception::Consistency("acquiring " + relname,
				"archive " + name + " does not exist in " + m_dir);
}

void Archives::acquire(const std::string& relname, const utils::metadata::Collector& mds)
{
	string path = relname;
	string name = poppath(path);
	if (Archive* a = lookup(name))
		a->acquire(path, mds);
	else
		throw wibble::exception::Consistency("acquiring " + relname,
				"archive " + name + " does not exist in " + m_dir);
}

void Archives::remove(const std::string& relname)
{
	string path = relname;
	string name = poppath(path);
	if (Archive* a = lookup(name))
		a->remove(path);
	else
		throw wibble::exception::Consistency("removing " + relname,
				"archive " + name + " does not exist in " + m_dir);
}

void Archives::rescan(const std::string& relname)
{
	string path = relname;
	string name = poppath(path);
	if (Archive* a = lookup(name))
		a->rescan(path);
	else
		throw wibble::exception::Consistency("rescanning " + relname,
				"archive " + name + " does not exist in " + m_dir);
}

namespace {
struct MaintPathPrepender : public maintenance::MaintFileVisitor
{
	MaintFileVisitor& next;
	std::string prefix;

	MaintPathPrepender(MaintFileVisitor& next, const std::string& prefix)
		: next(next), prefix(prefix) {}
	
	virtual void operator()(const std::string& file, State state)
	{
		next(str::joinpath(prefix, file), state);
	}
};
}

void Archives::maintenance(maintenance::MaintFileVisitor& v)
{
	for (map<string, Archive*>::iterator i = m_archives.begin();
			i != m_archives.end(); ++i)
	{
		MaintPathPrepender p(v, i->first);
		i->second->maintenance(p);
	}
	if (m_last)
	{
		MaintPathPrepender p(v, "last");
		m_last->maintenance(p);
	}
}

void Archives::vacuum()
{
	for (map<string, Archive*>::iterator i = m_archives.begin();
			i != m_archives.end(); ++i)
		i->second->vacuum();
	if (m_last)
		m_last->vacuum();
}

}
}
