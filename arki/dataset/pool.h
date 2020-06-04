#ifndef ARKI_DATASET_POOL_H
#define ARKI_DATASET_POOL_H

/// Pool of datasets, opened on demand

#include <arki/core/fwd.h>
#include <arki/metadata/fwd.h>
#include <arki/dataset/fwd.h>
#include <string>
#include <map>
#include <memory>

namespace arki {
namespace dataset {

class WriterPool
{
protected:
    std::shared_ptr<Session> session;

    // Dataset cache
    std::map<std::string, std::shared_ptr<dataset::Writer>> cache;

public:
    WriterPool(std::shared_ptr<Session> session);
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
}
#endif
