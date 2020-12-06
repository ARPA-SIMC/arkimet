#include "archive.h"
#include "reporter.h"
#include "index/manifest.h"
#include "simple/reader.h"
#include "simple/writer.h"
#include "simple/checker.h"
#include "offline.h"
#include "maintenance.h"
#include "empty.h"
#include "arki/libconfig.h"
#include "arki/matcher.h"
#include "arki/metadata/collection.h"
#include "arki/nag.h"
#include "arki/summary.h"
#include "arki/types/reftime.h"
#include "arki/types/source/blob.h"
#include "arki/utils/files.h"
#include "arki/utils/string.h"
#include "arki/utils/sys.h"
#include <ctime>
#include <fcntl.h>

using namespace std;
using namespace arki;
using namespace arki::core;
using namespace arki::types;
using namespace arki::utils;

namespace arki {
namespace dataset {

namespace archive {

bool is_archive(const std::string& dir)
{
    return index::Manifest::exists(dir);
}


static core::cfg::Section make_config(const std::string& dir)
{
    core::cfg::Section cfg;
    cfg.set("name", str::basename(dir));
    cfg.set("path", dir);
    cfg.set("step", "monthly");
    cfg.set("offline", "true");
    cfg.set("smallfiles", "true");
    return cfg;
}


template<typename Archive>
struct ArchivesRoot
{
    std::string dataset_root;
    std::string archive_root;
    std::shared_ptr<dataset::Dataset> parent;

    std::map<std::string, std::shared_ptr<Archive>> archives;
    std::shared_ptr<Archive> last;

    ArchivesRoot(const std::string& dataset_root, std::shared_ptr<dataset::Dataset> parent)
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
        archives.clear();
        last.reset();
    }

    void rescan(bool include_invalid=false)
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
                if (include_invalid || archive::is_archive(pathname))
                {
                    names.insert(i->d_name);
                }
            }
        }

        // Look for existing archives
        for (const auto& i: names)
        {
            auto a(this->instantiate(i));
            if (a.get())
            {
                if (i == "last")
                    last = a;
                else
                    archives.insert(make_pair(i, a));
            }
        }
    }

    bool iter(std::function<bool(Archive&)> f)
    {
        for (auto& i: archives)
            if (!f(*i.second))
                return false;
        if (last)
            return f(*last);
        return true;
    }

    // Look up an archive, returns 0 if not found
    std::shared_ptr<Archive> lookup(const std::string& name)
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

    std::shared_ptr<dataset::Reader> instantiate_reader(const std::string& name)
    {
        string pathname = str::joinpath(archive_root, name);
        std::shared_ptr<dataset::Dataset> ds;
        if (sys::exists(pathname + ".summary"))
        {
            if (index::Manifest::exists(pathname))
                ds = std::make_shared<simple::Dataset>(parent->session, make_config(pathname));
            else
                ds = std::make_shared<offline::Dataset>(parent->session, pathname);
        } else
            ds = std::make_shared<simple::Dataset>(parent->session, make_config(pathname));
        ds->set_parent(parent);
        return ds->create_reader();
    }

    virtual std::shared_ptr<Archive> instantiate(const std::string& name) = 0;
};

class ArchivesReaderRoot: public ArchivesRoot<dataset::Reader>
{
public:
    using ArchivesRoot::ArchivesRoot;

    std::shared_ptr<dataset::Reader> instantiate(const std::string& name) override
    {
        return instantiate_reader(name);
    }
};

class ArchivesCheckerRoot: public ArchivesRoot<dataset::Checker>
{
public:
    using ArchivesRoot::ArchivesRoot;

    void rescan()
    {
        ArchivesRoot<dataset::Checker>::rescan(true);

        // Instantiate the 'last' archive even if the directory does not exist
        if (!last)
        {
            last = instantiate("last");

            // FIXME: this fails if a file has already been placed there.
            // Use a new Checker::create function instead, that just makes sure
            // a dataset is there without needs-check-do-not-pack (but then it
            // should fail if the directory already exists).
            // Ok, instead, when archiving a file, ensure that 'last' exists
            // before starting moving files into it.

            // Run a check to remove needs-check-do-not-pack files
            CheckerConfig conf;
            conf.readonly = false;
            last->check(conf);
        }
    }

    std::shared_ptr<dataset::Checker> instantiate(const std::string& name) override
    {
        string pathname = str::joinpath(archive_root, name);
        if (sys::exists(pathname + ".summary"))
            return std::shared_ptr<dataset::Checker>();

        auto ds = std::make_shared<simple::Dataset>(parent->session, make_config(pathname));
        ds->set_parent(parent);
        return ds->create_checker();
    }
};

Dataset::Dataset(std::shared_ptr<Session> session, const std::string& root)
    : dataset::Dataset(session, "archives"), root(root)
{
}

std::shared_ptr<dataset::Reader> Dataset::create_reader()
{
    return std::make_shared<archive::Reader>(static_pointer_cast<Dataset>(shared_from_this()));
}

std::shared_ptr<dataset::Checker> Dataset::create_checker()
{
    return std::make_shared<archive::Checker>(static_pointer_cast<Dataset>(shared_from_this()));
}


Reader::Reader(std::shared_ptr<Dataset> dataset)
    : DatasetAccess(dataset), archives(new archive::ArchivesReaderRoot(dataset->root, dataset))
{
    archives->rescan();
}

Reader::~Reader()
{
    delete archives;
}

std::string Reader::type() const { return "archives"; }

bool Reader::impl_query_data(const dataset::DataQuery& q, metadata_dest_func dest)
{
    return archives->iter([&](dataset::Reader& r) {
        return r.query_data(q, dest);
    });
}

void Reader::impl_fd_query_bytes(const dataset::ByteQuery& q, NamedFileDescriptor& out)
{
    archives->iter([&](dataset::Reader& r) {
        r.query_bytes(q, out);
        return true;
    });
}

void Reader::impl_abstract_query_bytes(const dataset::ByteQuery& q, AbstractOutputFile& out)
{
    archives->iter([&](dataset::Reader& r) {
        r.query_bytes(q, out);
        return true;
    });
}

void Reader::summary_for_all(Summary& out)
{
    string sum_file = str::joinpath(archives->dataset_root, ".summaries/archives.summary");
    sys::File fd(sum_file);
    if (fd.open_ifexists(O_RDONLY))
        out.read(fd);
    else
    {
        // Query the summaries of all archives
        Matcher m;
        archives->iter([&](dataset::Reader& a) {
            a.query_summary(m, out);
            return true;
        });
    }
}

void Reader::impl_query_summary(const Matcher& matcher, Summary& summary)
{
    core::Interval interval;
    if (!matcher.intersect_interval(interval))
        // If the matcher is inconsistent, return now
        return;

    // Extract date range from matcher
    if (!interval.begin.is_set() && !interval.end.is_set())
    {
        // Use archive summary cache
        Summary s;
        summary_for_all(s);
        s.filter(matcher, summary);
        return;
    }

    // Query only archives that fit that date range
    archives->iter([&](dataset::Reader& a) {
        core::Interval i = a.get_stored_time_interval();
        if (i.is_unbounded())
            return true;
        if (interval.intersects(i))
            a.query_summary(matcher, summary);
        return true;
    });
}

core::Interval Reader::get_stored_time_interval()
{
    core::Interval res;
    // Query only archives that fit that date range
    archives->iter([&](dataset::Reader& a) {
        if (res.is_unbounded())
            res = a.get_stored_time_interval();
        else
            res.extend(a.get_stored_time_interval());
        return true;
    });
    return res;
}

unsigned Reader::test_count_archives() const
{
    return archives->archives.size();
}



Checker::Checker(std::shared_ptr<Dataset> dataset)
    : DatasetAccess(dataset), archives(new ArchivesCheckerRoot(dataset->root, dataset))
{
    archives->rescan();
}

Checker::~Checker()
{
    delete archives;
}

std::string Checker::type() const { return "archives"; }

void Checker::segments_recursive(CheckerConfig& opts, std::function<void(segmented::Checker&, segmented::CheckerSegment&)> dest)
{
    archives->iter([&](dataset::Checker& a) {
        if (segmented::Checker* sc = dynamic_cast<segmented::Checker*>(&a))
        {
            sc->segments(opts, [&](segmented::CheckerSegment& segment) {
                dest(*sc, segment);
            });
        }
        return true;
    });
}

void Checker::remove_old(CheckerConfig& opts)
{
    archives->iter([&](dataset::Checker& a) {
        a.remove_all(opts);
        return true;
    });
}

void Checker::remove_all(CheckerConfig& opts)
{
    archives->iter([&](dataset::Checker& a) {
        a.remove_all(opts);
        return true;
    });
}

void Checker::tar(CheckerConfig& opts)
{
    if (!opts.offline) return;
    archives->iter([&](dataset::Checker& a) {
        a.tar(opts);
        return true;
    });
}

void Checker::zip(CheckerConfig& opts)
{
    if (!opts.offline) return;
    archives->iter([&](dataset::Checker& a) {
        a.zip(opts);
        return true;
    });
}

void Checker::compress(CheckerConfig& opts, unsigned groupsize)
{
    if (!opts.offline) return;
    archives->iter([&](dataset::Checker& a) {
        a.compress(opts, groupsize);
        return true;
    });
}

void Checker::repack(CheckerConfig& opts, unsigned test_flags)
{
    archives->iter([&](dataset::Checker& a) {
        a.repack(opts, test_flags);
        return true;
    });
}

void Checker::check(CheckerConfig& opts)
{
    archives->iter([&](dataset::Checker& a) {
        a.check(opts);
        return true;
    });
}

void Checker::check_issue51(CheckerConfig& opts)
{
    if (!opts.offline) return;
    archives->iter([&](dataset::Checker& a) {
        a.check_issue51(opts);
        return true;
    });
}

void Checker::state(CheckerConfig& opts)
{
    if (!opts.offline) return;
    archives->iter([&](dataset::Checker& a) {
        a.state(opts);
        return true;
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

void Checker::index_segment(const std::string& relpath, metadata::Collection&& mds)
{
    string path = relpath;
    string name = poppath(path);
    if (auto a = archives->lookup(name))
    {
        if (auto sc = dynamic_pointer_cast<segmented::Checker>(a))
            sc->segment(path)->index(move(mds));
        else
            throw std::runtime_error(this->name() + ": cannot acquire " + relpath + ": archive " + name + " is not writable");
    }
    else
        throw std::runtime_error(this->name() + ": cannot acquire " + relpath + ": archive " + name + " does not exist in " + archives->archive_root);
    archives->invalidate_summary_cache();
}

void Checker::release_segment(const std::string& relpath, const std::string& new_root, const std::string& new_relpath, const std::string& new_abspath)
{
    string path = utils::str::normpath(relpath);
    string name = poppath(path);
    if (name != "last") throw std::runtime_error(this->name() + ": cannot release segment " + relpath + ": segment is not in last/ archive");

    if (auto a = archives->lookup(name))
    {
        if (auto sc = dynamic_pointer_cast<segmented::Checker>(a))
            sc->segment(path)->release(new_root, new_relpath, new_abspath);
        else
            throw std::runtime_error(this->name() + ": cannot acquire " + relpath + ": archive " + name + " is not writable");
    }
    else
        throw std::runtime_error(this->name() + ": cannot acquire " + relpath + ": archive " + name + " does not exist in " + archives->archive_root);
    archives->invalidate_summary_cache();
}

unsigned Checker::test_count_archives() const
{
    return archives->archives.size();
}

}
}
}
