#include "archive.h"
#include "arki/matcher.h"
#include "arki/metadata.h"
#include "arki/metadata/collection.h"
#include "arki/summary.h"
#include "arki/types.h"
#include "arki/utils/string.h"
#include "arki/utils/sys.h"
#include "offline.h"
#include "simple.h"
#include "simple/manifest.h"
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

bool is_archive(const std::filesystem::path& dir)
{
    return simple::manifest::exists(dir);
}

static core::cfg::Section make_config(const std::filesystem::path& dir,
                                      const std::string& step_name)
{
    core::cfg::Section cfg;
    cfg.set("name", dir.filename());
    cfg.set("path", dir);
    cfg.set("step", step_name);
    cfg.set("offline", "true");
    cfg.set("smallfiles", "true");
    return cfg;
}

template <typename Archive> struct ArchivesRoot
{
    std::filesystem::path dataset_root;
    std::filesystem::path archive_root;
    std::shared_ptr<archive::Dataset> parent;

    std::map<std::string, std::shared_ptr<Archive>> archives;
    std::shared_ptr<Archive> last;

    ArchivesRoot(const std::filesystem::path& dataset_root,
                 std::shared_ptr<archive::Dataset> parent)
        : dataset_root(dataset_root), archive_root(dataset_root), parent(parent)
    // m_scache_root(str::joinpath(root, ".summaries"))
    {
        // Create the directory if it does not exist
        std::filesystem::create_directories(archive_root);
    }

    virtual ~ArchivesRoot() { clear(); }

    void clear()
    {
        archives.clear();
        last.reset();
    }

    void rescan(bool include_invalid = false)
    {
        // Clean up existing archives and restart from scratch
        clear();

        // Look for subdirectories: they are archives
        sys::Path d(archive_root);
        set<string> names;
        for (sys::Path::iterator i = d.begin(); i != d.end(); ++i)
        {
            // Skip '.', '..' and hidden files
            if (i->d_name[0] == '.')
                continue;
            if (!i.isdir())
            {
                // Add .summary files
                string name = i->d_name;
                if (str::endswith(name, ".summary"))
                    names.insert(name.substr(0, name.size() - 8));
            }
            else
            {
                // Add directory with a manifest inside
                auto pathname = archive_root / i->d_name;
                if (include_invalid || archive::is_archive(pathname))
                {
                    names.insert(i->d_name);
                }
            }
        }

        // Look for existing archives
        for (const auto& i : names)
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
        for (auto& i : archives)
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
        std::filesystem::remove(dataset_root / ".summaries" /
                                "archives.summary");
    }

    void rebuild_summary_cache()
    {
        auto sum_file = dataset_root / ".summaries" / "archives.summary";
        Summary s;
        Matcher m;

        // Query the summaries of all archives
        iter([&](Archive& a) {
            unique_ptr<Reader> r(instantiate_reader(a.name()));
            r->query_summary(m, s);
        });

        // Write back to the cache directory, if allowed
        if (sys::access(dataset_root / ".summaries", W_OK))
            s.writeAtomically(sum_file);
    }

    std::shared_ptr<dataset::Reader> instantiate_reader(const std::string& name)
    {
        auto pathname = archive_root / name;
        std::shared_ptr<dataset::Dataset> ds;
        if (std::filesystem::exists(sys::with_suffix(pathname, ".summary")))
        {
            if (simple::manifest::exists(pathname))
                ds = std::make_shared<simple::Dataset>(
                    parent->session, make_config(pathname, parent->step_name));
            else
                ds = std::make_shared<offline::Dataset>(parent->session,
                                                        pathname);
        }
        else
            ds = std::make_shared<simple::Dataset>(
                parent->session, make_config(pathname, parent->step_name));
        ds->set_parent(parent.get());
        return ds->create_reader();
    }

    virtual std::shared_ptr<Archive> instantiate(const std::string& name) = 0;
};

class ArchivesReaderRoot : public ArchivesRoot<dataset::Reader>
{
public:
    using ArchivesRoot::ArchivesRoot;

    std::shared_ptr<dataset::Reader>
    instantiate(const std::string& name) override
    {
        return instantiate_reader(name);
    }
};

class ArchivesCheckerRoot : public ArchivesRoot<dataset::Checker>
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

    std::shared_ptr<dataset::Checker>
    instantiate(const std::string& name) override
    {
        auto pathname = archive_root / name;
        if (std::filesystem::exists(sys::with_suffix(pathname, ".summary")))
            return std::shared_ptr<dataset::Checker>();

        auto ds = std::make_shared<simple::Dataset>(
            parent->session, make_config(pathname, parent->step_name));
        ds->set_parent(parent.get());
        return ds->create_checker();
    }
};

Dataset::Dataset(std::shared_ptr<Session> session,
                 const std::filesystem::path& root,
                 const std::string& step_name)
    : dataset::Dataset(session, "archives"), root(root),
      segment_session(std::make_shared<segment::Session>(root)),
      step_name(step_name)
{
}

std::shared_ptr<dataset::Reader> Dataset::create_reader()
{
    return std::make_shared<archive::Reader>(
        static_pointer_cast<Dataset>(shared_from_this()));
}

std::shared_ptr<dataset::Checker> Dataset::create_checker()
{
    return std::make_shared<archive::Checker>(
        static_pointer_cast<Dataset>(shared_from_this()));
}

Reader::Reader(std::shared_ptr<Dataset> dataset)
    : DatasetAccess(dataset),
      archives(new archive::ArchivesReaderRoot(dataset->root, dataset))
{
    archives->rescan();
}

Reader::~Reader() { delete archives; }

std::string Reader::type() const { return "archives"; }

bool Reader::impl_query_data(const query::Data& q, metadata_dest_func dest)
{
    return archives->iter(
        [&](dataset::Reader& r) { return r.query_data(q, dest); });
}

void Reader::impl_stream_query_bytes(const query::Bytes& q, StreamOutput& out)
{
    archives->iter([&](dataset::Reader& r) {
        r.query_bytes(q, out);
        return true;
    });
}

void Reader::summary_for_all(Summary& out)
{
    auto sum_file = archives->dataset_root / ".summaries" / "archives.summary";
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
    : DatasetAccess(dataset),
      archives(new ArchivesCheckerRoot(dataset->root, dataset))
{
    archives->rescan();
}

Checker::~Checker() { delete archives; }

std::string Checker::type() const { return "archives"; }

void Checker::segments_recursive(
    CheckerConfig& opts,
    std::function<void(segmented::Checker&, segmented::CheckerSegment&)> dest)
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
    if (!opts.offline)
        return;
    archives->iter([&](dataset::Checker& a) {
        a.tar(opts);
        return true;
    });
}

void Checker::zip(CheckerConfig& opts)
{
    if (!opts.offline)
        return;
    archives->iter([&](dataset::Checker& a) {
        a.zip(opts);
        return true;
    });
}

void Checker::compress(CheckerConfig& opts, unsigned groupsize)
{
    if (!opts.offline)
        return;
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
    if (!opts.offline)
        return;
    archives->iter([&](dataset::Checker& a) {
        a.check_issue51(opts);
        return true;
    });
}

void Checker::state(CheckerConfig& opts)
{
    if (!opts.offline)
        return;
    archives->iter([&](dataset::Checker& a) {
        a.state(opts);
        return true;
    });
}

static std::string poppath(std::filesystem::path& path)
{
    auto path_iter = path.begin();
    if (path_iter == path.end())
        return std::string();
    std::string res = *path_iter;

    ++path_iter;
    auto new_path = std::filesystem::path();
    for (; path_iter != path.end(); ++path_iter)
        new_path /= *path_iter;

    path = new_path;
    return res;
}

void Checker::index_segment(const std::filesystem::path& relpath,
                            metadata::Collection&& mds)
{
    auto path = relpath;
    auto name = poppath(path);
    if (auto a = archives->lookup(name))
    {
        if (auto sc = dynamic_pointer_cast<segmented::Checker>(a))
        {
            auto segment =
                sc->dataset().segment_session->segment_from_relpath(path);
            sc->segment(segment)->index(std::move(mds));
        }
        else
            throw std::runtime_error(this->name() + ": cannot acquire " +
                                     relpath.native() + ": archive " + name +
                                     " is not writable");
    }
    else
        throw std::runtime_error(this->name() + ": cannot acquire " +
                                 relpath.native() + ": archive " + name +
                                 " does not exist in " +
                                 archives->archive_root.native());
    archives->invalidate_summary_cache();
}

arki::metadata::Collection Checker::release_segment(
    const std::filesystem::path& relpath,
    std::shared_ptr<const segment::Session> segment_session,
    const std::filesystem::path& new_relpath)
{
    arki::metadata::Collection res;
    auto path = relpath;
    auto name = poppath(path);
    if (name != "last")
        throw std::runtime_error(this->name() + ": cannot release segment " +
                                 relpath.native() +
                                 ": segment is not in last/ archive");

    if (auto a = archives->lookup(name))
    {
        if (auto sc = dynamic_pointer_cast<segmented::Checker>(a))
        {
            auto segment =
                sc->dataset().segment_session->segment_from_relpath(path);
            res = sc->segment(segment)->release(segment_session, new_relpath);
        }
        else
            throw std::runtime_error(this->name() + ": cannot acquire " +
                                     relpath.native() + ": archive " + name +
                                     " is not writable");
    }
    else
        throw std::runtime_error(this->name() + ": cannot acquire " +
                                 relpath.native() + ": archive " + name +
                                 " does not exist in " +
                                 archives->archive_root.native());
    archives->invalidate_summary_cache();
    return res;
}

unsigned Checker::test_count_archives() const
{
    return archives->archives.size();
}

} // namespace archive
} // namespace dataset
} // namespace arki
