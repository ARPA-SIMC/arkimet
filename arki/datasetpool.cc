#include "arki/datasetpool.h"
#include "arki/utils/string.h"
#include "arki/configfile.h"
#include "arki/dataset.h"
#include "arki/dataset/local.h"
#include "arki/utils/sys.h"
#include "arki/utils/string.h"
#include "config.h"

using namespace std;
using namespace arki::utils;

namespace arki {

WriterPool::WriterPool(const ConfigFile& cfg)
    : cfg(cfg)
{
}


WriterPool::~WriterPool()
{
    // Delete the cached datasets
    for (auto& i: cache)
        delete i.second;
}

dataset::Writer* WriterPool::get(const std::string& name)
{
    auto ci = cache.find(name);
    if (ci == cache.end())
    {
        ConfigFile* cfgsec = cfg.section(name);
        if (!cfgsec) return nullptr;
        auto config = dataset::Config::create(*cfgsec);
        dataset::Writer* writer = config->create_writer().release();
        cache.insert(make_pair(name, writer));
        return writer;
    } else {
        return ci->second;
    }
}


void WriterPool::flush()
{
    for (auto& i: cache)
        i.second->flush();
}

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

dataset::Writer* WriterPool::for_path(const std::string& pathname)
{
    PathMatch pmatch(pathname);

    for (const auto& dsi: cache)
    {
        dataset::LocalWriter* writer = dynamic_cast<dataset::LocalWriter*>(dsi.second);
        if (!writer) continue;
        string dspath = writer->path();
        if (pmatch.is_under(dspath))
            return writer;
    }

    return nullptr;
}

}
