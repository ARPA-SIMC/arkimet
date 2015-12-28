#include "config.h"

#include <arki/datasetpool.h>

#include <arki/wibble/exception.h>
#include <arki/utils/string.h>
#include <arki/configfile.h>
#include <arki/dataset.h>

using namespace std;

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
    for (std::map<std::string, dataset::Writer*>::iterator i = cache.begin();
            i != cache.end(); ++i)
        i->second->flush();
}

// Explicit template instantiations
template class DatasetPool<dataset::Reader>;
template class DatasetPool<dataset::Writer>;

}
// vim:set ts=4 sw=4:
