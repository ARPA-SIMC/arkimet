#ifndef ARKI_DATASET_INDEX_H
#define ARKI_DATASET_INDEX_H

#include <arki/dataset.h>

namespace arki {
namespace dataset {

struct Index
{
    virtual ~Index();

    /**
     * Query this index, returning metadata
     *
     * @return true if the index could be used for the query, false if the
     * query does not use the index and a full scan should be used instead
     */
    virtual bool query_data(const dataset::DataQuery& q, metadata_dest_func) = 0;

    /**
     * Query this index, returning a summary
     *
     * Summary caches are used if available.
     *
     * @return true if the index could be used for the query, false if the
     * query does not use the index and a full scan should be used instead
     */
    virtual bool query_summary(const Matcher& matcher, Summary& summary) = 0;
};

}
}

#endif
