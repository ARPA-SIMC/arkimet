#ifndef ARKI_DATASET_POOL_H
#define ARKI_DATASET_POOL_H

/// Pool of datasets, opened on demand

#include <arki/core/fwd.h>
#include <arki/metadata/fwd.h>
#include <arki/dataset/fwd.h>
#include <unordered_map>
#include <string>
#include <map>
#include <memory>

namespace arki {
namespace dataset {

class Pool: public std::enable_shared_from_this<Pool>
{
protected:
    std::shared_ptr<Session> m_session;

    /// Pool of known, reusable datasets
    std::unordered_map<std::string, std::shared_ptr<dataset::Dataset>> dataset_pool;

public:
    Pool(std::shared_ptr<Session> session) : m_session(session) {}

    std::shared_ptr<Session> session() { return m_session; }

    /**
     * Instantiate a dataset and add it to the pool.
     *
     * If a dataset with the same name already exists in the pool, it throws
     * std::runtime_error.
     *
     * If the dataset is remote, the aliases used by the remote server will be
     * added to the session alias database. If different servers define some
     * aliases differently, it throws std::runtime_error
     */
    void add_dataset(const core::cfg::Section& cfg, bool load_aliases=true);

    /// Check if the dataset pool has a dataset with the given name
    bool has_dataset(const std::string& name) const;

    /// Check if the dataset pool contains datasets
    bool has_datasets() const;

    /**
     * Return a dataset from the session pool.
     *
     * If the named dataset does not exist in the pool, it throws
     * std::runtime_error
     */
    std::shared_ptr<Dataset> dataset(const std::string& name);

    /// Return how many datasets are in the dataset pool
    size_t size() const;

    /**
     * Iterate all datasets in the pool.
     *
     * Return true if dest returned true each time it was called.
     *
     * If dest returns false, stop iteration and return false.
     */
    bool foreach_dataset(std::function<bool(std::shared_ptr<dataset::Dataset>)> dest);

    /**
     * Check if all the datasets in the session pool are remote and from the
     * same server.
     *
     * @return the common server URL, or the empty string if some datasets
     * are local or from different servers
     */
    std::string get_common_remote_server() const;

    /**
     * Create a QueryMacro dataset querying datasets from this session's pool.
     */
    std::shared_ptr<Dataset> querymacro(const std::string& macro_name, const std::string& macro_query);

    /**
     * Create a Merged dataset querying datasets from this session's pool.
     */
    std::shared_ptr<Dataset> merged();

    /**
     * Look for the dataset in the pool that contains the given metadata item,
     * and make the metadata source relative to it.
     *
     * If no dataset contains the metadata, it returns an empty shared_ptr and
     * leaves \a md untouched.
     */
    std::shared_ptr<dataset::Dataset> locate_metadata(Metadata& md);
};


class DispatchPool
{
protected:
    std::shared_ptr<Pool> pool;

    // Dataset cache
    std::map<std::string, std::shared_ptr<dataset::Writer>> cache;

public:
    DispatchPool(std::shared_ptr<Pool> pool);
    ~DispatchPool();

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


class CheckPool
{
protected:
    std::shared_ptr<Pool> pool;

    // Dataset cache
    std::map<std::string, std::shared_ptr<dataset::Checker>> cache;

public:
    CheckPool(std::shared_ptr<Pool> pool);
    ~CheckPool();

    /**
     * Get a dataset, either from the cache or instantiating it.
     *
     * If \a name does not correspond to any dataset in the configuration,
     * returns 0
     */
    std::shared_ptr<dataset::Checker> get(const std::string& name);

    /**
     * Mark the given data as deleted
     */
    void remove(const arki::metadata::Collection& todolist, bool simulate);
};

}
}
#endif
