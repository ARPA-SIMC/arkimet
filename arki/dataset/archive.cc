#include "config.h"
#include "arki/configfile.h"
#include "arki/dataset/archive.h"
#include "arki/dataset/index/manifest.h"
#include "arki/dataset/simple/reader.h"
#include "arki/dataset/simple/writer.h"
#include "arki/dataset/offline.h"
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

namespace archive {

std::unique_ptr<Reader> create_reader(const std::string& dir);
std::unique_ptr<SegmentedChecker> create_checker(const std::string& dir);


bool is_archive(const std::string& dir)
{
    return index::Manifest::exists(dir);
}

static ConfigFile make_config(const std::string& dir)
{
    ConfigFile cfg;
    cfg.setValue("name", str::basename(dir));
    cfg.setValue("path", dir);
    return cfg;
}

static ConfigFile make_config(const std::string& name, const std::string& dir)
{
    ConfigFile cfg;
    cfg.setValue("name", name);
    cfg.setValue("path", dir);
    return cfg;
}

}

#if 0
class OnlineArchiveChecker : public simple::Checker
{
public:
    OnlineArchiveChecker(const std::string& dir)
    {
    }

    //const std::string& path() const { return m_dir; }

#if 0
    void indexFile(const std::string& relname)
    {
        if (!m_mft) throw wibble::exception::Consistency("acquiring into archive " + m_dir, "archive opened in read only mode");
        // Scan file, reusing .metadata if still valid
        metadata::Collection mdc;
        string pathname = str::joinpath(m_dir, relname);
        if (!scan::scan(pathname, mdc.inserter_func()))
            throw wibble::exception::Consistency("acquiring " + pathname, "it does not look like a file we can acquire");
        acquire(relname, mdc);
    }
#endif


#if 0
    void flush() override
    {
        if (m_mft) m_mft->flush();
    }
#endif
};
#endif


#if 0
#warning TODO: move to LocalChecker, wrapping all checks of local archives with this
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
#endif

#if 0
size_t OfflineArchiveReader::produce_nth(metadata_dest_func cons, size_t idx)
{
    // All files are offline, so there is nothing we can produce
    return 0;
}
#endif

namespace archive {

template<typename Archive>
struct ArchivesRoot
{
    std::string dataset_root;
    std::string archive_root;

    std::map<std::string, Archive*> archives;
    Archive* last = nullptr;

    ArchivesRoot(const std::string& dataset_root)
        : dataset_root(dataset_root), archive_root(str::joinpath(dataset_root, ".archive"))
          // m_scache_root(str::joinpath(root, ".summaries"))
    {
        // Create the directory if it does not exist
        sys::makedirs(archive_root);

        // Look for other archives other than 'last'
        rescan();
    }

    virtual ~ArchivesRoot()
    {
        for (auto& i: archives)
            delete i.second;
        if (last)
            delete last;
    }

    void rescan()
    {
        // Clean up existing archives and restart from scratch
        if (last)
        {
            delete last;
            last = 0;
        }
        for (auto& i: archives)
            delete i.second;
        archives.clear();

        // Look for subdirectories: they are archives
        sys::Path d(archive_root);
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
                string pathname = str::joinpath(archive_root, i->d_name);
                if (archive::is_archive(pathname))
                    names.insert(i->d_name);
            }
        }

        // Look for existing archives
        for (const auto& i: names)
        {
            unique_ptr<Archive> a(this->instantiate(i));
            if (a.get())
            {
                if (i == "last")
                    last = a.release();
                else
                    archives.insert(make_pair(i, a.release()));
            }
        }

        // Instantiate the 'last' archive even if the directory does not exist,
        // if not read only
        if (!last)
            last = this->instantiate("last").release();
    }

    void iter(std::function<void(Archive&)> f)
    {
        for (auto& i: archives)
            f(*i.second);
        if (last)
            f(*last);
    }

    // Look up an archive, returns 0 if not found
    Archive* lookup(const std::string& name)
    {
        if (name == "last")
            return last;

        auto i = archives.find(name);
        if (i == archives.end())
            return nullptr;
        return i->second;
    }

    void invalidate_summary_cache()
    {
        sys::unlink_ifexists(str::joinpath(dataset_root, ".summaries/archives.summary"));
    }

    void rebuild_summary_cache()
    {
        string sum_file = str::joinpath(dataset_root, ".summaries/archives.summary");
        Summary s;
        Matcher m;

        // Query the summaries of all archives
        iter([&](Archive& a) {
            unique_ptr<Reader> r(archive::create_reader(str::joinpath(archive_root, a.name())));
            r->query_summary(m, s);
        });

        // Write back to the cache directory, if allowed
        if (sys::access(str::joinpath(dataset_root, ".summaries"), W_OK))
            s.writeAtomically(sum_file);
    }

    virtual std::unique_ptr<Archive> instantiate(const std::string& name) = 0;
};

struct ArchivesReaderRoot: public ArchivesRoot<Reader>
{
    using ArchivesRoot::ArchivesRoot;

    std::unique_ptr<Reader> instantiate(const std::string& name) override
    {
        return create_reader(str::joinpath(archive_root, name));
    }
};
struct ArchivesCheckerRoot: public ArchivesRoot<SegmentedChecker>
{
    using ArchivesRoot::ArchivesRoot;

    std::unique_ptr<SegmentedChecker> instantiate(const std::string& name) override
    {
        return create_checker(str::joinpath(archive_root, name));
    }
};

}


ArchivesReader::ArchivesReader(const std::string& root)
    : Reader("archives"), archives(new archive::ArchivesReaderRoot(root))
{
}

ArchivesReader::~ArchivesReader()
{
    delete archives;
}

void ArchivesReader::query_data(const dataset::DataQuery& q, metadata_dest_func dest)
{
    archives->iter([&](Reader& r) {
        r.query_data(q, dest);
    });
}

void ArchivesReader::query_bytes(const dataset::ByteQuery& q, int out)
{
    archives->iter([&](Reader& r) {
        r.query_bytes(q, out);
    });
}

void ArchivesReader::summary_for_all(Summary& out)
{
    string sum_file = str::joinpath(archives->dataset_root, ".summaries/archives.summary");
    sys::File fd(sum_file);
    if (fd.open_ifexists(O_RDONLY))
        out.read(fd, sum_file);
    else
    {
        // Query the summaries of all archives
        Matcher m;
        archives->iter([&](Reader& a) {
            a.query_summary(m, out);
        });
    }
}

void ArchivesReader::query_summary(const Matcher& matcher, Summary& summary)
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
    archives->iter([&](Reader& a) {
        unique_ptr<Time> arc_begin;
        unique_ptr<Time> arc_end;
        a.expand_date_range(arc_begin, arc_end);
        if (Time::range_overlaps(matcher_begin.get(), matcher_end.get(), arc_begin.get(), arc_end.get()))
            a.query_summary(matcher, summary);
    });
}



ArchivesChecker::ArchivesChecker(const std::string& root)
    : SegmentedChecker(archive::make_config("archives", str::joinpath(root, ".archive"))), archives(new archive::ArchivesCheckerRoot(root))
{
}

ArchivesChecker::~ArchivesChecker()
{
    delete archives;
}

void ArchivesChecker::removeAll(std::ostream& log, bool writable)
{
    archives->iter([&](SegmentedChecker& a) {
        a.removeAll(log, writable);
    });
}

void ArchivesChecker::archiveFile(const std::string& relpath)
{
    throw std::runtime_error("cannot archive " + relpath + ": file is already in the archive");
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

size_t ArchivesChecker::repackFile(const std::string& relname)
{
    string path = relname;
    string name = poppath(path);
    if (SegmentedChecker* a = archives->lookup(name))
        a->repackFile(path);
    else
        throw std::runtime_error("cannot repack " + relname + ": archive " + name + " does not exist in " + archives->archive_root);
    archives->invalidate_summary_cache();
}

void ArchivesChecker::indexFile(const std::string& relname, metadata::Collection&& mds)
{
    string path = relname;
    string name = poppath(path);
    if (SegmentedChecker* a = archives->lookup(name))
        a->indexFile(path, move(mds));
    else
        throw std::runtime_error("cannot acquire " + relname + ":archive " + name + " does not exist in " + archives->archive_root);
    archives->invalidate_summary_cache();
}

size_t ArchivesChecker::removeFile(const std::string& relname, bool with_data)
{
    size_t res;
    string path = relname;
    string name = poppath(path);
    if (SegmentedChecker* a = archives->lookup(name))
        res = a->removeFile(path, with_data);
    else
        throw std::runtime_error("cannot remove " + relname + ": archive " + name + " does not exist in " + archives->archive_root);
    archives->invalidate_summary_cache();
    return res;
}

void ArchivesChecker::rescanFile(const std::string& relname)
{
    string path = relname;
    string name = poppath(path);
    if (SegmentedChecker* a = archives->lookup(name))
        a->rescanFile(path);
    else
        throw std::runtime_error("cannot rescan " + relname + ": archive " + name + " does not exist in " + archives->archive_root);
    archives->invalidate_summary_cache();
}

void ArchivesChecker::maintenance(segment::state_func v, bool quick)
{
    archives->iter([&](SegmentedChecker& a) {
        a.maintenance([&](const std::string& file, segment::FileState state) {
            v(str::joinpath(a.name(), file), state);
        }, quick);
    });
}

size_t ArchivesChecker::vacuum()
{
    size_t res = 0;
    archives->iter([&](SegmentedChecker& a) { res += a.vacuum(); });
    archives->rebuild_summary_cache();
    return res;
}

namespace archive {

unique_ptr<Reader> create_reader(const std::string& dir)
{
    if (sys::exists(dir + ".summary"))
    {
        if (index::Manifest::exists(dir))
            return unique_ptr<Reader>(new simple::Reader(make_config(dir)));
        else
            return unique_ptr<Reader>(new OfflineReader(dir + ".summary"));
    } else
        return unique_ptr<Reader>(new simple::Reader(make_config(dir)));
}

unique_ptr<SegmentedChecker> create_checker(const std::string& dir)
{
    if (sys::exists(dir + ".summary"))
    {
        if (index::Manifest::exists(dir))
            return unique_ptr<SegmentedChecker>(new simple::Checker(make_config(dir)));
        else
            return unique_ptr<SegmentedChecker>(new NullSegmentedChecker(make_config(dir)));
    } else
        return unique_ptr<SegmentedChecker>(new simple::Checker(make_config(dir)));
}
}

}
}
