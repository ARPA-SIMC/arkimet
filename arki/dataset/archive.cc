#include "config.h"
#include "arki/dataset/archive.h"
#include "arki/dataset/index/manifest.h"
#include "arki/dataset/simple/reader.h"
#include "arki/dataset/maintenance.h"
#include "arki/summary.h"
#include "arki/types/reftime.h"
#include "arki/types/source/blob.h"
#include "arki/matcher.h"
#include "arki/metadata/collection.h"
#include "arki/utils/fd.h"
#include "arki/utils/files.h"
#include "arki/utils/compress.h"
#include "arki/scan/any.h"
#include "arki/nag.h"
#include "arki/utils/sys.h"
#include "arki/utils/string.h"
#include "arki/wibble/exception.h"
#include <fstream>
#include <ctime>
#include <cstdio>
#include <cerrno>
#include <unistd.h>
#include <sys/types.h>
#include <sys/stat.h>
#include <fcntl.h>
#ifdef HAVE_LUA
#include "arki/report.h"
#endif

using namespace std;
using namespace arki;
using namespace arki::types;
using namespace arki::utils;

namespace arki {
namespace dataset {

Archive::~Archive() {}

bool Archive::is_archive(const std::string& dir)
{
    return index::Manifest::exists(dir);
}

Archive* Archive::create(const std::string& dir, bool writable)
{
    if (sys::exists(dir + ".summary"))
    {
        // Writable is not allowed on archives that have been archived offline
        if (writable) return 0;

        if (index::Manifest::exists(dir))
        {
            unique_ptr<OnlineArchive> res(new OnlineArchive(dir));
            res->openRO();
            return res.release();
        } else
            return new OfflineArchive(dir + ".summary");
    } else {
        unique_ptr<OnlineArchive> res(new OnlineArchive(dir));
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
    sys::makedirs(m_dir);
}

OnlineArchive::~OnlineArchive()
{
    if (m_mft) delete m_mft;
}

void OnlineArchive::openRO()
{
    unique_ptr<index::Manifest> mft = index::Manifest::create(m_dir);
    m_mft = mft.release();
    m_mft->openRO();
}

void OnlineArchive::openRW()
{
    unique_ptr<index::Manifest> mft = index::Manifest::create(m_dir);
    m_mft = mft.release();
    m_mft->openRW();
}

void OnlineArchive::query_data(const dataset::DataQuery& q, metadata_dest_func dest)
{
    m_mft->query_data(q, dest);
}

void OnlineArchive::query_summary(const Matcher& matcher, Summary& summary)
{
    m_mft->query_summary(matcher, summary);
}

size_t OnlineArchive::produce_nth(metadata_dest_func cons, size_t idx)
{
    return m_mft->produce_nth(cons, idx);
}

void OnlineArchive::expand_date_range(unique_ptr<Time>& begin, unique_ptr<Time>& end) const
{
    m_mft->expand_date_range(begin, end);
}

void OnlineArchive::acquire(const std::string& relname)
{
    if (!m_mft) throw wibble::exception::Consistency("acquiring into archive " + m_dir, "archive opened in read only mode");
    // Scan file, reusing .metadata if still valid
    metadata::Collection mdc;
    string pathname = str::joinpath(m_dir, relname);
    if (!scan::scan(pathname, mdc.inserter_func()))
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
    mds.strip_source_paths();
    Summary sum;
    mds.add_to_summary(sum);

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

void OnlineArchive::maintenance(segment::state_func v)
{
    unique_ptr<segment::SegmentManager> segment_manager(segment::SegmentManager::get(m_dir));
    m_mft->check(*segment_manager, [&](const std::string& relpath, segment::FileState state) {
        // Add the archived bit
        // Remove the TO_PACK bit, since once a file is archived it's not
        //   touched anymore, so there's no point packing it
        // Remove the TO_ARCHIVE bit, since we're already in the archive
        // Remove the TO_DELETE bit, since delete age doesn't affect the
        //   archive
        v(relpath, state - FILE_TO_PACK - FILE_TO_ARCHIVE - FILE_TO_DELETE + FILE_ARCHIVED);
    });
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

    sys::unlink_ifexists(pathname + ".summary");
    sys::unlink_ifexists(pathname + ".metadata");
    //sys::fs::deleteIfExists(pathname);
    deindex(relname);

    m_mft->invalidate_summary();
}

void OnlineArchive::deindex(const std::string& relname)
{
    if (!m_mft) throw wibble::exception::Consistency("deindexing file from " + m_dir, "archive opened in read only mode");
    m_mft->remove(relname);
    m_mft->invalidate_summary();
}

void OnlineArchive::rescan(const std::string& relname)
{
    if (!m_mft) throw wibble::exception::Consistency("rescanning file in " + m_dir, "archive opened in read only mode");
    m_mft->rescanFile(m_dir, relname);
    m_mft->invalidate_summary();
}

void OnlineArchive::vacuum()
{
    if (!m_mft) throw wibble::exception::Consistency("vacuuming " + m_dir, "archive opened in read only mode");
    // If archive dir is not writable, skip this section
    if (!sys::exists(m_dir)) return;

    m_mft->vacuum();

    // Regenerate summary cache if needed
    Summary s;
    m_mft->query_summary(Matcher(), s);
}


OfflineArchive::OfflineArchive(const std::string& fname)
    : fname(fname)
{
    sum.readFile(fname);
}

OfflineArchive::~OfflineArchive()
{
}

void OfflineArchive::query_data(const dataset::DataQuery& q, metadata_dest_func dest)
{
    // If the matcher would match the summary, output some kind of note about it
}

void OfflineArchive::query_summary(const Matcher& matcher, Summary& summary)
{
    sum.filter(matcher, summary);
}

size_t OfflineArchive::produce_nth(metadata_dest_func cons, size_t idx)
{
    // All files are offline, so there is nothing we can produce
    return 0;
}

void OfflineArchive::expand_date_range(unique_ptr<Time>& begin, unique_ptr<Time>& end) const
{
    sum.expand_date_range(begin, end);
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
void OfflineArchive::maintenance(segment::state_func v)
{
    // No files, nothing to do
}
void OfflineArchive::vacuum()
{
    // Nothing to vacuum
}


Archives::Archives(const std::string& root, const std::string& dir, bool read_only)
    : m_scache_root(str::joinpath(root, ".summaries")), m_dir(dir), m_read_only(read_only), m_last(0)
{
    // Create the directory if it does not exist
    sys::makedirs(m_dir);

    // Look for other archives other than 'last'
    rescan_archives();
}

Archives::~Archives()
{
	for (map<string, Archive*>::iterator i = m_archives.begin();
			i != m_archives.end(); ++i)
		delete i->second;
	if (m_last)
		delete m_last;
}

void Archives::rescan_archives()
{
    // Clean up existing archives and restart from scratch
    if (m_last)
    {
        delete m_last;
        m_last = 0;
    }
    for (std::map<std::string, Archive*>::const_iterator i = m_archives.begin();
            i != m_archives.end(); ++i)
        delete i->second;
    m_archives.clear();

    // Look for subdirectories: they are archives
    sys::Path d(m_dir);
    set<string> names;
    for (sys::Path::iterator i = d.begin(); i != d.end(); ++i)
    {
        // Skip '.', '..' and hidden files
        if (i->d_name[0] == '.') continue;
        if (!i.isdir())
        {
            // Add .summary files
            string name = i->d_name;
            if (str::endswith(name, ".summary"))
                names.insert(name.substr(0, name.size() - 8));
        } else {
            // Add directory with a manifest inside
            string pathname = str::joinpath(m_dir, i->d_name);
            if (!m_read_only || Archive::is_archive(pathname))
                names.insert(i->d_name);
        }
    }

    // Look for existing archives
    for (set<string>::const_iterator i = names.begin();
            i != names.end(); ++i)
    {
        unique_ptr<Archive> a(Archive::create(str::joinpath(m_dir, *i), !m_read_only));
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
    if (!m_read_only && !m_last)
    {

        OnlineArchive* o;
        m_last = o = new OnlineArchive(str::joinpath(m_dir, "last"));
        o->openRW();
    }
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

void Archives::query_data(const dataset::DataQuery& q, metadata_dest_func dest)
{
    for (map<string, Archive*>::iterator i = m_archives.begin();
            i != m_archives.end(); ++i)
        i->second->query_data(q, dest);
    if (m_last)
        m_last->query_data(q, dest);
}

void Archives::query_bytes(const dataset::ByteQuery& q, int out)
{
    for (map<string, Archive*>::iterator i = m_archives.begin();
            i != m_archives.end(); ++i)
        i->second->query_bytes(q, out);
    if (m_last)
        m_last->query_bytes(q, out);
}

void Archives::summary_for_all(Summary& out)
{
    string sum_file = str::joinpath(m_scache_root, "archives.summary");
    bool has_cache = true;
    int fd = open(sum_file.c_str(), O_RDONLY);
    if (fd < 0)
    {
        if (errno == ENOENT)
            has_cache = false;
        else
            throw wibble::exception::System("opening file " + sum_file);
    }
    utils::fd::HandleWatch hw(sum_file, fd);

    if (has_cache)
        out.read(fd, sum_file);
    else
    {
        Matcher m;
        // Query the summaries of all archives
        for (map<string, Archive*>::iterator i = m_archives.begin();
                i != m_archives.end(); ++i)
            i->second->query_summary(m, out);
        if (m_last)
            m_last->query_summary(m, out);
    }
}

void Archives::rebuild_summary_cache()
{
    // Only when writable
    if (m_read_only)
        throw wibble::exception::Consistency(
                "rebuilding summary cache for archives in " + m_dir,
                "archive opened in read only mode");

    string sum_file = str::joinpath(m_scache_root, "archives.summary");
    Summary s;
    Matcher m;

    // Query the summaries of all archives
    for (map<string, Archive*>::iterator i = m_archives.begin();
            i != m_archives.end(); ++i)
        i->second->query_summary(m, s);
    if (m_last)
        m_last->query_summary(m, s);

    // Add all summaries in toplevel dirs
    sys::Path d(m_dir);
    for (sys::Path::iterator i = d.begin(); i != d.end(); ++i)
    {
        // Skip '.', '..' and hidden files
        if (i->d_name[0] == '.') continue;
        if (!str::endswith(i->d_name, ".summary")) continue;
        s.readFile(str::joinpath(m_dir, i->d_name));
    }

    // Write back to the cache directory, if allowed
    if (!m_read_only)
        if (sys::access(m_scache_root, W_OK))
            s.writeAtomically(sum_file);
}

void Archives::query_summary(const Matcher& matcher, Summary& summary)
{
    unique_ptr<Time> matcher_begin;
    unique_ptr<Time> matcher_end;
    if (!matcher.restrict_date_range(matcher_begin, matcher_end))
        // If the matcher is inconsistent, return now
        return;

    // Extract date range from matcher
    if (!matcher_begin.get() && !matcher_end.get())
    {
        // Use archive summary cache
        Summary s;
        summary_for_all(s);
        s.filter(matcher, summary);
        return;
    }

    // Query only archives that fit that date range
    for (map<string, Archive*>::iterator i = m_archives.begin();
            i != m_archives.end(); ++i)
    {
        unique_ptr<Time> arc_begin;
        unique_ptr<Time> arc_end;
        i->second->expand_date_range(arc_begin, arc_end);
        if (Time::range_overlaps(matcher_begin.get(), matcher_end.get(), arc_begin.get(), arc_end.get()))
            i->second->query_summary(matcher, summary);
    }
    if (m_last)
    {
        unique_ptr<Time> arc_begin;
        unique_ptr<Time> arc_end;
        m_last->expand_date_range(arc_begin, arc_end);
        if (Time::range_overlaps(matcher_begin.get(), matcher_end.get(), arc_begin.get(), arc_end.get()))
            m_last->query_summary(matcher, summary);
    }
}

void Archives::expand_date_range(unique_ptr<Time>& begin, unique_ptr<Time>& end) const
{
    for (map<string, Archive*>::const_iterator i = m_archives.begin();
            i != m_archives.end(); ++i)
        i->second->expand_date_range(begin, end);
}

size_t Archives::produce_nth(metadata_dest_func cons, size_t idx)
{
    size_t res = 0;
    for (map<string, Archive*>::iterator i = m_archives.begin();
            i != m_archives.end(); ++i)
        res += i->second->produce_nth(cons, idx);
    if (m_last)
        res += m_last->produce_nth(cons, idx);
    return res;
}

void Archives::invalidate_summary_cache()
{
    sys::unlink_ifexists(str::joinpath(m_scache_root, "archives.summary"));
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
    invalidate_summary_cache();
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
    invalidate_summary_cache();
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
    invalidate_summary_cache();
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
    invalidate_summary_cache();
}

void Archives::maintenance(segment::state_func v)
{
    for (map<string, Archive*>::iterator i = m_archives.begin();
            i != m_archives.end(); ++i)
    {
        i->second->maintenance([&](const std::string& file, segment::FileState state) {
            v(str::joinpath(i->first, file), state);
        });
    }
    if (m_last)
    {
        m_last->maintenance([&](const std::string& file, segment::FileState state) {
            v(str::joinpath("last", file), state);
        });
    }
}

void Archives::vacuum()
{
	for (map<string, Archive*>::iterator i = m_archives.begin();
			i != m_archives.end(); ++i)
		i->second->vacuum();
	if (m_last)
		m_last->vacuum();
    if (!m_read_only)
        rebuild_summary_cache();
}

}
}
