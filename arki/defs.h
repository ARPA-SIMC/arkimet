#ifndef ARKI_DEFS_H
#define ARKI_DEFS_H

/// Core defines common to all arkimet code

#include <arki/libconfig.h>
#include <functional>
#include <memory>

namespace arki {
struct Metadata;

/**
 * Function signature for metadata consumers
 */
typedef std::function<bool(std::shared_ptr<Metadata>)> metadata_dest_func;


/// Identifier codes used for binary serialisation of Types
enum TypeCode
{
    TYPE_INVALID         =  0,
    TYPE_ORIGIN          =  1,
    TYPE_PRODUCT         =  2,
    TYPE_LEVEL           =  3,
    TYPE_TIMERANGE       =  4,
    TYPE_REFTIME         =  5,
    TYPE_NOTE            =  6,
    TYPE_SOURCE          =  7,
    TYPE_ASSIGNEDDATASET =  8,
    TYPE_AREA            =  9,
    TYPE_PRODDEF         = 10,
    TYPE_SUMMARYITEM     = 11,
    TYPE_SUMMARYSTATS    = 12,
    TYPE_TIME            = 13,
    TYPE_BBOX            = 14,
    TYPE_RUN             = 15,
    TYPE_TASK            = 16, // utilizzato per OdimH5 /how.task
    TYPE_QUANTITY        = 17, // utilizzato per OdimH5 /what.quantity
    TYPE_VALUE           = 18,
    TYPE_MAXCODE
};

namespace dataset {

/// Possible outcomes of acquire
enum WriterAcquireResult {
    /// Acquire successful
    ACQ_OK,
    /// Acquire failed because the data is already in the database
    ACQ_ERROR_DUPLICATE,
    /// Acquire failed for other reasons than duplicates
    ACQ_ERROR
};

}

}

#endif
