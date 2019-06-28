#include "datasets.h"
#include "utils/string.h"
#include "core/cfg.h"
#include "dataset.h"
#include "dataset/local.h"
#include "utils/sys.h"
#include "utils/string.h"
#include "metadata.h"
#include "types/source/blob.h"
#include "config.h"

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

Datasets::Datasets(const core::cfg::Sections& cfg)
{
    for (const auto& si: cfg)
        configs.insert(make_pair(si.first, dataset::Config::create(si.second)));
}

std::shared_ptr<const dataset::Config> Datasets::get(const std::string& name) const
{
    auto res = configs.find(name);
    if (res == configs.end())
        throw std::runtime_error("dataset " + name + " not found");
    return res->second;
}

bool Datasets::has(const std::string& name) const
{
    return configs.find(name) != configs.end();
}

std::shared_ptr<const dataset::Config> Datasets::locate_metadata(Metadata& md)
{
    const auto& source = md.sourceBlob();
    std::string pathname = source.absolutePathname();

    PathMatch pmatch(pathname);

    for (const auto& dsi: configs)
    {
        auto lcfg = dynamic_pointer_cast<const dataset::LocalConfig>(dsi.second);
        if (!lcfg) continue;
        if (pmatch.is_under(lcfg->path))
        {
            md.set_source(source.makeRelativeTo(lcfg->path));
            return dsi.second;
        }
    }

    return std::shared_ptr<const dataset::LocalConfig>();
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
        shared_ptr<dataset::Writer> writer(config->create_writer());
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
