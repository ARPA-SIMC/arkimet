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
#include <ctime>
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

namespace archive {

template<typename Archive>
struct ArchivesRoot
{
    std::string dataset_root;
    std::string archive_root;
    dataset::Base& parent;

    std::map<std::string, Archive*> archives;
    Archive* last = nullptr;

    ArchivesRoot(const std::string& dataset_root, dataset::Base& parent)
        : dataset_root(dataset_root), archive_root(str::joinpath(dataset_root, ".archive")), parent(parent)
          // m_scache_root(str::joinpath(root, ".summaries"))
    {
        // Create the directory if it does not exist
        sys::makedirs(archive_root);
    }

    virtual ~ArchivesRoot()
    {
        clear();
    }

    void clear()
    {
        for (auto& i: archives)
            delete i.second;
        archives.clear();
        if (last)
            delete last;
        last = nullptr;
    }

    void rescan()
    {
        // Clean up existing archives and restart from scratch
        clear();

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
            unique_ptr<Reader> r(instantiate_reader(a.name()));
            r->query_summary(m, s);
        });

        // Write back to the cache directory, if allowed
        if (sys::access(str::joinpath(dataset_root, ".summaries"), W_OK))
            s.writeAtomically(sum_file);
    }

    std::unique_ptr<Reader> instantiate_reader(const std::string& name)
    {
        string pathname = str::joinpath(archive_root, name);
        unique_ptr<Reader> res;
        if (sys::exists(pathname + ".summary"))
        {
            if (index::Manifest::exists(pathname))
                res.reset(new simple::Reader(make_config(pathname)));
            else
                res.reset(new OfflineReader(pathname + ".summary"));
        } else
            res.reset(new simple::Reader(make_config(pathname)));
        res->set_parent(parent);
        return res;
    }

    virtual std::unique_ptr<Archive> instantiate(const std::string& name) = 0;
};

struct ArchivesReaderRoot: public ArchivesRoot<Reader>
{
    using ArchivesRoot::ArchivesRoot;

    std::unique_ptr<Reader> instantiate(const std::string& name) override
    {
        return instantiate_reader(name);
    }
};

struct ArchivesCheckerRoot: public ArchivesRoot<Checker>
{
    using ArchivesRoot::ArchivesRoot;

    void rescan()
    {
        ArchivesRoot<Checker>::rescan();

        // Instantiate the 'last' archive even if the directory does not exist
        if (!last)
        {
            last = instantiate("last").release();

            // FIXME: this fails if a file has already been placed there.
            // Use a new Checker::create function instead, that just makes sure
            // a dataset is there without needs-check-do-not-pack (but then it
            // should fail if the directory already exists).
            // Ok, instead, when archiving a file, ensure that 'last' exists
            // before starting moving files into it.

            // Run a check to remove needs-check-do-not-pack files
            NullReporter r;
            last->check(r, true, true);
        }
    }

    std::unique_ptr<Checker> instantiate(const std::string& name) override
    {
        string pathname = str::joinpath(archive_root, name);
        unique_ptr<Checker> res;
        if (sys::exists(pathname + ".summary"))
        {
            if (index::Manifest::exists(pathname))
                res.reset(new simple::Checker(make_config(pathname)));
            else
                res.reset(new NullChecker(make_config(pathname)));
        } else
            res.reset(new simple::Checker(make_config(pathname)));
        res->set_parent(parent);
        return res;
    }
};

}


ArchivesReader::ArchivesReader(const std::string& root)
    : Reader("archives"), archives(new archive::ArchivesReaderRoot(root, *this))
{
    archives->rescan();
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
    : Checker(archive::make_config("archives", root)), archives(new archive::ArchivesCheckerRoot(root, *this))
{
    archives->rescan();
}

ArchivesChecker::~ArchivesChecker()
{
    delete archives;
}

void ArchivesChecker::removeAll(Reporter& reporter, bool writable)
{
    archives->iter([&](Checker& a) {
        a.removeAll(reporter, writable);
    });
}

void ArchivesChecker::repack(dataset::Reporter& reporter, bool writable)
{
    archives->iter([&](Checker& a) {
        a.repack(reporter, writable);
    });
}

void ArchivesChecker::check(dataset::Reporter& reporter, bool fix, bool quick)
{
    archives->iter([&](Checker& a) {
        a.check(reporter, fix, quick);
    });
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

void ArchivesChecker::indexFile(const std::string& relname, metadata::Collection&& mds)
{
    string path = relname;
    string name = poppath(path);
    if (Checker* a = archives->lookup(name))
    {
        if (SegmentedChecker* sc = dynamic_cast<SegmentedChecker*>(a))
            sc->indexFile(path, move(mds));
        else
            throw std::runtime_error(this->name() + ": cannot acquire " + relname + ": archive " + name + " is not writable");
    }
    else
        throw std::runtime_error(this->name() + ": cannot acquire " + relname + ": archive " + name + " does not exist in " + archives->archive_root);
    archives->invalidate_summary_cache();
}

}
}
