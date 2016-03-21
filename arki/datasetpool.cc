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

template<typename DATASET>
DatasetPool<DATASET>::DatasetPool(const ConfigFile& cfg) : cfg(cfg)
{
}

template<typename DATASET>
DatasetPool<DATASET>::~DatasetPool()
{
	// Delete the cached datasets
	for (typename std::map<std::string, DATASET*>::iterator i = cache.begin();
			i != cache.end(); ++i)
		delete i->second;
}

template<typename DATASET>
DATASET* DatasetPool<DATASET>::get(const std::string& name)
{
	typename map<string, DATASET*>::iterator ci = cache.find(name);
	if (ci == cache.end())
	{
		ConfigFile* cfgsec = cfg.section(name);
		if (!cfgsec) return 0;
		DATASET* target = DATASET::create(*cfgsec);
		cache.insert(make_pair(name, target));
		return target;
	} else {
		return ci->second;
	}
}


ReaderPool::ReaderPool(const ConfigFile& cfg)
    : DatasetPool<dataset::Reader>(cfg) {}

WriterPool::WriterPool(const ConfigFile& cfg)
    : DatasetPool<dataset::Writer>(cfg) {}

void WriterPool::flush()
{
    for (auto& i: cache)
        i.second->flush();
}

LocalWriterPool::LocalWriterPool(const ConfigFile& cfg)
    : DatasetPool<dataset::LocalWriter>(cfg) {}

void LocalWriterPool::flush()
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

dataset::LocalWriter* LocalWriterPool::for_path(const std::string& pathname)
{
    PathMatch pmatch(pathname);

    for (const auto& dsi: cache)
    {
        string dspath = dsi.second->path();
        if (pmatch.is_under(dspath))
            return dsi.second;
    }

    return nullptr;
}

// Explicit template instantiations
template class DatasetPool<dataset::Reader>;
template class DatasetPool<dataset::Writer>;
template class DatasetPool<dataset::LocalWriter>;

}
