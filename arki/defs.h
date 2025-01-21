#ifndef ARKI_DEFS_H
#define ARKI_DEFS_H

/// Core defines common to all arkimet code

#include <functional>
#include <memory>
#include <iosfwd>

namespace arki {
class Metadata;

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

/// Supported data formats
enum class DataFormat : int
{
    GRIB = 1,
    BUFR = 2,
    VM2 = 3,
    ODIMH5 = 4,
    NETCDF = 5,
    JPEG = 6,
};

/// String version of a format name
const std::string& format_name(DataFormat format);

/// Format from its string version
DataFormat format_from_string(const std::string& format);

std::ostream& operator<<(std::ostream& o, DataFormat format);


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
