/*
 * datasetpool - Pool of datasets, opened on demand
 *
 * Copyright (C) 2007--2011  ARPA-SIM <urpsim@smr.arpa.emr.it>
 *
 * This program is free software; you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation; either version 2 of the License, or
 * (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License along
 * with this program; if not, write to the Free Software Foundation, Inc.,
 * 51 Franklin Street, Fifth Floor, Boston, MA 02110-1301 USA.
 *
 * Author: Enrico Zini <enrico@enricozini.com>
 */

#include "config.h"

#include <arki/datasetpool.h>

#include <wibble/exception.h>
#include <arki/utils/string.h>
#include <arki/configfile.h>
#include <arki/dataset.h>

using namespace std;
using namespace wibble;

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

ReadonlyDatasetPool::ReadonlyDatasetPool(const ConfigFile& cfg)
	: DatasetPool<ReadonlyDataset>(cfg) {}

WritableDatasetPool::WritableDatasetPool(const ConfigFile& cfg)
	: DatasetPool<WritableDataset>(cfg) {}

void WritableDatasetPool::flush()
{
	for (std::map<std::string, WritableDataset*>::iterator i = cache.begin();
			i != cache.end(); ++i)
		i->second->flush();
}

// Explicit template instantiations
template class DatasetPool<ReadonlyDataset>;
template class DatasetPool<WritableDataset>;

}
// vim:set ts=4 sw=4:
