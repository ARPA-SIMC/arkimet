#include "pool.h"
#include "arki/utils/string.h"
#include "arki/core/cfg.h"
#include "arki/dataset.h"
#include "arki/dataset/local.h"
#include "arki/dataset/session.h"
#include "arki/dataset/querymacro.h"
#include "arki/dataset/merged.h"
#include "arki/types/source/blob.h"
#include "arki/utils/sys.h"
#include "arki/metadata.h"
#include "arki/metadata/collection.h"
#include "arki/nag.h"

using namespace std;
using namespace arki::utils;

namespace arki {
namespace dataset {

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

    PathMatch(const std::filesystem::path& pathname)
    {
        fill_parents(pathname);
    }

    void fill_parents(const std::filesystem::path& pathname)
    {
        struct stat st;
        sys::stat(pathname, st);
        auto i = parents.insert(FSPos(st));
        // If we already knew of that fs position, stop here: we reached the
        // top or a loop
        if (i.second == false) return;
        // Otherwise, go up a level and scan again
        fill_parents(pathname.parent_path());
    }

    bool is_under(const std::filesystem::path& pathname)
    {
        struct stat st;
        sys::stat(pathname, st);
        return parents.find(FSPos(st)) != parents.end();
    }
};

}

/*
 * Pool
 */

void Pool::add_dataset(const core::cfg::Section& cfg, bool load_aliases)
{
    auto ds = m_session->dataset(cfg);
    auto old = dataset_pool.find(ds->name());
    if (old != dataset_pool.end())
    {
        nag::warning(
            "dataset \"%s\" in \"%s\" already loaded from \"%s\": keeping only the first one",
            ds->name().c_str(), ds->config->value("path").c_str(),
            old->second->config->value("path").c_str());
        return;
    }

    // Handle merging remote aliases
    if (load_aliases && ds->config->value("type") == "remote")
    {
        // Skip if we have already loaded aliases from this server
        auto server = ds->config->value("server");
        m_session->load_remote_aliases(server);
    }

    dataset_pool.emplace(ds->name(), ds);
}

bool Pool::has_dataset(const std::string& name) const
{
    return dataset_pool.find(name) != dataset_pool.end();
}

bool Pool::has_datasets() const
{
    return !dataset_pool.empty();
}

std::shared_ptr<Dataset> Pool::dataset(const std::string& name)
{
    auto res = dataset_pool.find(name);
    if (res == dataset_pool.end())
        throw std::runtime_error("dataset " + name + " not found in session pool");
    return res->second;
}

size_t Pool::size() const
{
    return dataset_pool.size();
}

bool Pool::foreach_dataset(std::function<bool(std::shared_ptr<dataset::Dataset>)> dest)
{
    for (auto& i: dataset_pool)
        if (!dest(i.second))
            return false;
    return true;
}

std::string Pool::get_common_remote_server() const
{
    std::string base;
    for (const auto& si: dataset_pool)
    {
        std::string type = str::lower(si.second->config->value("type"));
        if (type != "remote") return std::string();
        std::string server = si.second->config->value("server");
        if (base.empty())
            base = server;
        else if (base != server)
            return std::string();
    }
    return base;
}

std::shared_ptr<Dataset> Pool::querymacro(const std::string& macro_name, const std::string& macro_query)
{
    // If all the datasets are on the same server, we can run the macro remotely
    std::string baseurl = get_common_remote_server();

    // TODO: macro_arg seems to be ignored (and lost) here

    if (baseurl.empty())
    {
        // Either all datasets are local, or they are on different servers: run the macro locally
        arki::nag::verbose("Running query macro %s locally", macro_name.c_str());
        return std::make_shared<arki::dataset::QueryMacro>(shared_from_this(), macro_name, macro_query);
    } else {
        // Create the remote query macro
        arki::nag::verbose("Running query macro %s remotely on %s", macro_name.c_str(), baseurl.c_str());
        arki::core::cfg::Section cfg;
        cfg.set("name", macro_name);
        cfg.set("type", "remote");
        cfg.set("path", baseurl);
        cfg.set("qmacro", macro_query);
        return m_session->dataset(cfg);
    }
}

std::shared_ptr<Dataset> Pool::merged()
{
    return std::make_shared<merged::Dataset>(shared_from_this());
}

std::shared_ptr<dataset::Dataset> Pool::locate_metadata(Metadata& md)
{
    const auto& source = md.sourceBlob();
    std::string pathname = source.absolutePathname();

    PathMatch pmatch(pathname);

    for (const auto& dsi: dataset_pool)
    {
        auto lcfg = std::dynamic_pointer_cast<dataset::local::Dataset>(dsi.second);
        if (!lcfg) continue;
        if (pmatch.is_under(lcfg->path))
        {
            md.set_source(source.makeRelativeTo(lcfg->path));
            return dsi.second;
        }
    }

    return std::shared_ptr<dataset::local::Dataset>();
}


/*
 * DispatchPool
 */

DispatchPool::DispatchPool(std::shared_ptr<Pool> pool)
    : pool(pool)
{
}

DispatchPool::~DispatchPool()
{
}

std::shared_ptr<dataset::Writer> DispatchPool::get(const std::string& name)
{
    auto ci = cache.find(name);
    if (ci == cache.end())
    {
        auto ds = pool->dataset(name);
        auto writer(ds->create_writer());
        cache.insert(make_pair(name, writer));
        return writer;
    } else {
        return ci->second;
    }
}

std::shared_ptr<dataset::Writer> DispatchPool::get_error()
{
    return get("error");
}

std::shared_ptr<dataset::Writer> DispatchPool::get_duplicates()
{
    if (pool->has_dataset("duplicates"))
        return get("duplicates");
    else
        return get_error();
}

void DispatchPool::flush()
{
    for (auto& i: cache)
        i.second->flush();
}


/*
 * CheckPool
 */

CheckPool::CheckPool(std::shared_ptr<Pool> pool)
    : pool(pool)
{
}

CheckPool::~CheckPool()
{
}

std::shared_ptr<dataset::Checker> CheckPool::get(const std::string& name)
{
    auto ci = cache.find(name);
    if (ci == cache.end())
    {
        auto ds = pool->dataset(name);
        auto checker(ds->create_checker());
        cache.insert(make_pair(name, checker));
        return checker;
    } else {
        return ci->second;
    }
}

void CheckPool::remove(const metadata::Collection& todolist, bool simulate)
{
    // Group metadata by dataset
    std::unordered_map<std::string, metadata::Collection> by_dataset;
    unsigned idx = 1;
    for (const auto& md: todolist)
    {
        if (!md->has_source_blob())
        {
            std::stringstream ss;
            ss << "cannot remove data #" << idx << ": metadata does not come from an on-disk dataset";
            throw std::runtime_error(ss.str());
        }

        auto ds = pool->locate_metadata(*md);
        if (!ds)
        {
            std::stringstream ss;
            ss << "cannot remove data #" << idx << " is does not come from any known dataset";
            throw std::runtime_error(ss.str());
        }

        by_dataset[ds->name()].push_back(md);
        ++idx;
    }

    if (simulate)
    {
        for (const auto& i: by_dataset)
            arki::nag::warning("%s: %zd data would be deleted", i.first.c_str(), i.second.size());
        return;
    }

    // Perform removals
    for (const auto& i: by_dataset)
    {
        auto ds = get(i.first);
        bool removed = false;
        try {
            ds->remove(i.second);
            removed = true;
        } catch (std::exception& e) {
            arki::nag::warning("Cannot remove %zd messages from dataset %s: %s",
                    i.second.size(), i.first.c_str(), e.what());
        }

        if (removed)
            arki::nag::verbose("%s: %zd data marked as deleted", i.first.c_str(), i.second.size());
    }
}

}
}
