#ifndef ARKI_DATASETPOOL_H
#define ARKI_DATASETPOOL_H

/// Pool of datasets, opened on demand

#include <arki/core/fwd.h>
#include <string>
#include <map>
#include <memory>

namespace arki {
class Metadata;

namespace dataset {
class Config;
class Reader;
class Writer;
}

/**
 * Manage a pool of datasets
 */
class Datasets
{
protected:
    std::map<std::string, std::shared_ptr<const dataset::Config>> configs;

public:
    explicit Datasets(const core::cfg::Sections& cfg);

    /// Get the configuration for the given dataset
    std::shared_ptr<const dataset::Config> get(const std::string& name) const;

    /// Check if the pool has a dataset with the given name
    bool has(const std::string& name) const;

    /**
     * Look for the dataset that contains the given metadata item, and make the
     * metadata source relative to it.
     *
     * If not dataset contains the metadata, returns an empty shared_ptr and
     * leaves \a md untouched.
     */
    std::shared_ptr<const dataset::Config> locate_metadata(Metadata& md);
};


class WriterPool
{
protected:
    const Datasets& datasets;

    // Dataset cache
    std::map<std::string, std::shared_ptr<dataset::Writer>> cache;

public:
    WriterPool(const Datasets& datasets);
    ~WriterPool();

    /**
     * Get a dataset, either from the cache or instantiating it.
     *
     * If \a name does not correspond to any dataset in the configuration,
     * returns 0
     */
    std::shared_ptr<dataset::Writer> get(const std::string& name);

    /**
     * Get the "error" dataset
     */
    std::shared_ptr<dataset::Writer> get_error();

    /**
     * Get the "duplicates" dataset
     */
    std::shared_ptr<dataset::Writer> get_duplicates();

    /**
     * Flush all dataset data to disk
     */
    void flush();
};

}
#endif
