#include "pool.h"
#include "arki/utils/string.h"
#include "arki/core/cfg.h"
#include "arki/dataset.h"
#include "arki/dataset/local.h"
#include "arki/dataset/session.h"
#include "arki/utils/sys.h"
#include "arki/utils/string.h"
#include "arki/metadata.h"
#include "arki/types/source/blob.h"

using namespace std;
using namespace arki::utils;

namespace {

struct FSPos
{
    dev_t dev;
    ino_t ino;

    FSPos(const struct stat& st)
        : dev(st.st_dev), ino(st.st_ino)
    {
    }

    bool operator<(const FSPos& o) const
    {
        if (dev < o.dev) return true;
        if (dev > o.dev) return false;
        return ino < o.ino;
    }

    bool operator==(const FSPos& o) const
    {
        return dev == o.dev && ino == o.ino;
    }
};

struct PathMatch
{
    std::set<FSPos> parents;

    PathMatch(const std::string& pathname)
    {
        fill_parents(pathname);
    }

    void fill_parents(const std::string& pathname)
    {
        struct stat st;
        sys::stat(pathname, st);
        auto i = parents.insert(FSPos(st));
        // If we already knew of that fs position, stop here: we reached the
        // top or a loop
        if (i.second == false) return;
        // Otherwise, go up a level and scan again
        fill_parents(str::normpath(str::joinpath(pathname, "..")));
    }

    bool is_under(const std::string& pathname)
    {
        struct stat st;
        sys::stat(pathname, st);
        return parents.find(FSPos(st)) != parents.end();
    }
};

}


namespace arki {
namespace dataset {

Datasets::Datasets(std::shared_ptr<Session> session, const core::cfg::Sections& cfg)
{
    for (const auto& si: cfg)
        datasets.insert(make_pair(si.first, session->dataset(si.second)));
}

std::shared_ptr<dataset::Dataset> Datasets::get(const std::string& name) const
{
    auto res = datasets.find(name);
    if (res == datasets.end())
        throw std::runtime_error("dataset " + name + " not found");
    return res->second;
}

bool Datasets::has(const std::string& name) const
{
    return datasets.find(name) != datasets.end();
}

std::shared_ptr<dataset::Dataset> Datasets::locate_metadata(Metadata& md)
{
    const auto& source = md.sourceBlob();
    std::string pathname = source.absolutePathname();

    PathMatch pmatch(pathname);

    for (const auto& dsi: datasets)
    {
        auto lcfg = dynamic_pointer_cast<dataset::local::Dataset>(dsi.second);
        if (!lcfg) continue;
        if (pmatch.is_under(lcfg->path))
        {
            md.set_source(source.makeRelativeTo(lcfg->path));
            return dsi.second;
        }
    }

    return std::shared_ptr<dataset::local::Dataset>();
}


WriterPool::WriterPool(const Datasets& datasets)
    : datasets(datasets)
{
}

WriterPool::~WriterPool()
{
}

std::shared_ptr<dataset::Writer> WriterPool::get(const std::string& name)
{
    auto ci = cache.find(name);
    if (ci == cache.end())
    {
        auto config = datasets.get(name);
        auto writer(config->create_writer());
        cache.insert(make_pair(name, writer));
        return writer;
    } else {
        return ci->second;
    }
}

std::shared_ptr<dataset::Writer> WriterPool::get_error()
{
    return get("error");
}

std::shared_ptr<dataset::Writer> WriterPool::get_duplicates()
{
    if (datasets.has("duplicates"))
        return get("duplicates");
    else
        return get_error();
}


void WriterPool::flush()
{
    for (auto& i: cache)
        i.second->flush();
}

}
}
