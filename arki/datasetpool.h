#ifndef ARKI_DATASETPOOL_H
#define ARKI_DATASETPOOL_H

/// Pool of datasets, opened on demand

#include <string>
#include <map>

namespace arki {
class ConfigFile;

namespace dataset {
class Reader;
class Writer;
}

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

class ReaderPool : public DatasetPool<dataset::Reader>
{
public:	
	ReaderPool(const ConfigFile& cfg);
};

class WriterPool : public DatasetPool<dataset::Writer>
{
public:
    WriterPool(const ConfigFile& cfg);

    /**
     * Flush all dataset data do disk
     */
    void flush();
};

}
#endif
