#ifndef ARKI_DATASETPOOL_H
#define ARKI_DATASETPOOL_H

/*
 * datasetpool - Pool of datasets, opened on demand
 *
 * Copyright (C) 2007,2008  ARPA-SIM <urpsim@smr.arpa.emr.it>
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

#include <string>
#include <map>

namespace arki {

class ConfigFile;
class ReadonlyDataset;
class WritableDataset;

/**
 * Infrastructure to dispatch metadata into various datasets
 */
template<typename DATASET>
class DatasetPool
{
protected:
	const ConfigFile& cfg;

	// Dataset cache
	std::map<std::string, DATASET*> cache;

public:
	DatasetPool(const ConfigFile& cfg);
	~DatasetPool();

	/**
	 * Get a dataset, either from the cache or instantiating it.
	 *
	 * If \a name does not correspond to any dataset in the configuration,
	 * returns 0
	 */
	DATASET* get(const std::string& name);
};

class ReadonlyDatasetPool : public DatasetPool<ReadonlyDataset>
{
public:	
	ReadonlyDatasetPool(const ConfigFile& cfg);
};

class WritableDatasetPool : public DatasetPool<WritableDataset>
{
public:
	WritableDatasetPool(const ConfigFile& cfg);
	
	/**
	 * Flush all dataset data do disk
	 */
	void flush();
};

}

// vim:set ts=4 sw=4:
#endif
