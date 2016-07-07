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

class WriterPool
{
protected:
    const ConfigFile& cfg;

    // Dataset cache
    std::map<std::string, dataset::Writer*> cache;

public:
    WriterPool(const ConfigFile& cfg);
    ~WriterPool();

    /**
     * Get a dataset, either from the cache or instantiating it.
     *
     * If \a name does not correspond to any dataset in the configuration,
     * returns 0
     */
    dataset::Writer* get(const std::string& name);

    /**
     * Flush all dataset data to disk
     */
    void flush();

    /**
     * Get the dataset that contains the given pathname
     *
     * If no dataset can contain that pathname, returns 0.
     */
    dataset::Writer* for_path(const std::string& pathname);
};

}
#endif
