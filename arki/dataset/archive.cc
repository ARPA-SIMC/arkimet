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
#include <arki/metadata/collection.h>
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
#include <unistd.h>

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

Archive::~Archive() {}

bool Archive::is_archive(const std::string& dir)
{
    return simple::Manifest::exists(dir);
}

Archive* Archive::create(const std::string& dir, bool writable)
{
    if (sys::fs::exists(dir + ".summary"))
    {
        // Writable is not allowed on archives that have been archived offline
        if (writable) return 0;

        if (simple::Manifest::exists(dir))
        {
            auto_ptr<OnlineArchive> res(new OnlineArchive(dir));
            res->openRO();
            return res.release();
        } else
            return new OfflineArchive(dir + ".summary");
    } else {
        auto_ptr<OnlineArchive> res(new OnlineArchive(dir));
        if (writable)
            res->openRW();
        else
            res->openRO();
        return res.release();
    }
}


OnlineArchive::OnlineArchive(const std::string& dir)
: m_dir(dir), m_mft(0)
{
    // Create the directory if it does not exist
    wibble::sys::fs::mkpath(m_dir);
}

OnlineArchive::~OnlineArchive()
{
    if (m_mft) delete m_mft;
}

void OnlineArchive::openRO()
{
	auto_ptr<simple::Manifest> mft = simple::Manifest::create(m_dir);
	m_mft = mft.release();
	m_mft->openRO();
}

void OnlineArchive::openRW()
{
	auto_ptr<simple::Manifest> mft = simple::Manifest::create(m_dir);
	m_mft = mft.release();
	m_mft->openRW();
}

void OnlineArchive::queryData(const dataset::DataQuery& q, metadata::Consumer& consumer)
{
	m_mft->queryData(q, consumer);
}

void OnlineArchive::queryBytes(const dataset::ByteQuery& q, std::ostream& out)
{
	m_mft->queryBytes(q, out);
}

void OnlineArchive::querySummary(const Matcher& matcher, Summary& summary)
{
	m_mft->querySummary(matcher, summary);
}

size_t OnlineArchive::produce_nth(metadata::Consumer& cons, size_t idx)
{
    return m_mft->produce_nth(cons, idx);
}

void OnlineArchive::acquire(const std::string& relname)
{
	if (!m_mft) throw wibble::exception::Consistency("acquiring into archive " + m_dir, "archive opened in read only mode");
	// Scan file, reusing .metadata if still valid
	metadata::Collection mdc;
	string pathname = str::joinpath(m_dir, relname);
	if (!scan::scan(pathname, mdc))
		throw wibble::exception::Consistency("acquiring " + pathname, "it does not look like a file we can acquire");
	acquire(relname, mdc);
}

void OnlineArchive::acquire(const std::string& relname, metadata::Collection& mds)
{
	if (!m_mft) throw wibble::exception::Consistency("acquiring into archive " + m_dir, "archive opened in read only mode");
	string pathname = str::joinpath(m_dir, relname);
	time_t mtime = scan::timestamp(pathname);
	if (mtime == 0)
		throw wibble::exception::Consistency("acquiring " + pathname, "file does not exist");

	// Iterate the metadata, computing the summary and making the data
	// paths relative
	Summary sum;
	for (metadata::Collection::iterator i = mds.begin();
			i != mds.end(); ++i)
	{
		Item<source::Blob> s = i->source.upcast<source::Blob>();
		i->source = s->fileOnly();
		sum.add(*i);
	}

	// Regenerate .metadata
	mds.writeAtomically(pathname + ".metadata");

	// Regenerate .summary
	sum.writeAtomically(pathname + ".summary");

	// Add to manifest
	m_mft->acquire(relname, mtime, sum);
}

void OnlineArchive::flush()
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

void OnlineArchive::maintenance(maintenance::MaintFileVisitor& v)
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

void OnlineArchive::remove(const std::string& relname)
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

void OnlineArchive::deindex(const std::string& relname)
{
	if (!m_mft) throw wibble::exception::Consistency("deindexing file from " + m_dir, "archive opened in read only mode");
	m_mft->remove(relname);
}

void OnlineArchive::rescan(const std::string& relname)
{
	if (!m_mft) throw wibble::exception::Consistency("rescanning file in " + m_dir, "archive opened in read only mode");
	m_mft->rescanFile(m_dir, relname);
}

void OnlineArchive::vacuum()
{
	if (!m_mft) throw wibble::exception::Consistency("vacuuming " + m_dir, "archive opened in read only mode");
	// If archive dir is not writable, skip this section
	if (!sys::fs::exists(m_dir)) return;

	m_mft->vacuum();

	// Regenerate summary cache if needed
	Summary s;
	m_mft->querySummary(Matcher(), s);
}


OfflineArchive::OfflineArchive(const std::string& fname)
    : fname(fname)
{
    sum.readFile(fname);
}

OfflineArchive::~OfflineArchive()
{
}

void OfflineArchive::queryData(const dataset::DataQuery& q, metadata::Consumer& consumer)
{
    // If the matcher would match the summary, output some kind of note about it
}

void OfflineArchive::queryBytes(const dataset::ByteQuery& q, std::ostream& out)
{
    // We can't do anything here, as we cannot post a note to a raw stream
}

void OfflineArchive::querySummary(const Matcher& matcher, Summary& summary)
{
    sum.filter(matcher, summary);
}

size_t OfflineArchive::produce_nth(metadata::Consumer& cons, size_t idx)
{
    // All files are offline, so there is nothing we can produce
    return 0;
}

void OfflineArchive::acquire(const std::string& relname)
{
    throw wibble::exception::Consistency("running acquire on offline archive", "operation does not make sense");
}
void OfflineArchive::acquire(const std::string& relname, metadata::Collection& mds)
{
    throw wibble::exception::Consistency("running acquire on offline archive", "operation does not make sense");
}
void OfflineArchive::remove(const std::string& relname)
{
    throw wibble::exception::Consistency("running remove on offline archive", "operation does not make sense");
}
void OfflineArchive::rescan(const std::string& relname)
{
    throw wibble::exception::Consistency("running rescan on offline archive", "operation does not make sense");
}
void OfflineArchive::deindex(const std::string& relname)
{
    throw wibble::exception::Consistency("running deindex on offline archive", "operation does not make sense");
}
void OfflineArchive::flush()
{
    // Nothing to flush
}
void OfflineArchive::maintenance(maintenance::MaintFileVisitor& v)
{
    // No files, nothing to do
}
void OfflineArchive::vacuum()
{
    // Nothing to vacuum
}


Archives::Archives(const std::string& dir, bool read_only)
    : m_dir(dir), m_read_only(read_only), m_last(0)
{
    // Create the directory if it does not exist
    wibble::sys::fs::mkpath(m_dir);

    // Look for subdirectories: they are archives
    sys::fs::Directory d(m_dir);
    set<string> names;
    for (sys::fs::Directory::const_iterator i = d.begin();
            i != d.end(); ++i)
    {
        // Skip '.', '..' and hidden files
        if ((*i)[0] == '.') continue;
        if (!d.isdir(i))
        {
            // Add .summary files
            string name = *i;
            if (str::endsWith(name, ".summary"))
                names.insert(name.substr(0, name.size() - 8));
        } else {
            // Add directory with a manifest inside
            string pathname = str::joinpath(m_dir, *i);
            if (!read_only || Archive::is_archive(pathname))
                names.insert(*i);
        }
    }

    for (set<string>::const_iterator i = names.begin();
            i != names.end(); ++i)
    {
        auto_ptr<Archive> a(Archive::create(str::joinpath(m_dir, *i), !read_only));
        if (a.get())
        {
            if (*i == "last")
                m_last = a.release();
            else
                m_archives.insert(make_pair(*i, a.release()));
        }
    }

    // Instantiate the 'last' archive even if the directory does not exist,
    // if not read only
    if (!read_only && !m_last)
    {
        OnlineArchive* o;
        m_last = o = new OnlineArchive(str::joinpath(m_dir, "last"));
        o->openRW();
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

void Archives::queryData(const dataset::DataQuery& q, metadata::Consumer& consumer)
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
    {
        i->second->querySummary(matcher, summary);
    }
    if (m_last)
        m_last->querySummary(matcher, summary);
}

size_t Archives::produce_nth(metadata::Consumer& cons, size_t idx)
{
    size_t res = 0;
    for (map<string, Archive*>::iterator i = m_archives.begin();
            i != m_archives.end(); ++i)
        res += i->second->produce_nth(cons, idx);
    if (m_last)
        res += m_last->produce_nth(cons, idx);
    return res;
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

void Archives::acquire(const std::string& relname, metadata::Collection& mds)
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
