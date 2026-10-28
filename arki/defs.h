#ifndef ARKI_DEFS_H
#define ARKI_DEFS_H

/// Core defines common to all arkimet code

#include <functional>
#include <iosfwd>
#include <memory>
#include <string>

namespace arki {
class Metadata;

/**
 * Function signature for metadata consumers
 */
typedef std::function<bool(std::shared_ptr<Metadata>)> metadata_dest_func;

/// Identifier codes used for binary serialisation of Types
enum TypeCode {
    TYPE_INVALID         = 0,
    /// Originating centre and process
    TYPE_ORIGIN          = 1,
    /// Identification of the product
    TYPE_PRODUCT         = 2,
    /// Vertical level or layer
    TYPE_LEVEL           = 3,
    /// Time range (instant, accumulation, average...)
    TYPE_TIMERANGE       = 4,
    /// Reference time
    TYPE_REFTIME         = 5,
    /// Annotation about arkimet processing
    TYPE_NOTE            = 6,
    /// Location of the data
    TYPE_SOURCE          = 7,
    /// Dataset where the data is archived
    TYPE_ASSIGNEDDATASET = 8,
    /// Geographical area
    TYPE_AREA            = 9,
    /// Further product definition (i.e. ensamble parameters)
    TYPE_PRODDEF         = 10,
    /// Unique set of metadata in a summary
    TYPE_SUMMARYITEM     = 11,
    /// Statistics for a summary item
    TYPE_SUMMARYSTATS    = 12,
    /// Instant of time
    TYPE_TIME            = 13,
    /// Geographical bounding box
    TYPE_BBOX            = 14,
    /// Model run identification
    TYPE_RUN             = 15,
    /// OdimH5 task
    TYPE_TASK            = 16,
    /// OdimH5 quantity
    TYPE_QUANTITY        = 17,
    /// Partial data stored in the metadata to allow to reconstruct the whole
    /// in smallfiles optimization
    TYPE_VALUE           = 18,
    /// Application field
    TYPE_FIELD           = 19,
    /// Number of type codes defined
    TYPE_MAXCODE
};

/// Supported data formats
enum class DataFormat : unsigned {
    GRIB   = 1,
    BUFR   = 2,
    VM2    = 3,
    ODIMH5 = 4,
    NETCDF = 5,
    JPEG   = 6,
    __END__,
};

/// String version of a format name
const std::string& format_name(DataFormat format);

/// Format from its string version
DataFormat format_from_string(const std::string& format);

std::ostream& operator<<(std::ostream& o, DataFormat format);

/// What to do if duplicate data is found
enum class ReplaceStrategy {
    /// Default strategy
    DEFAULT,
    /// Never replace
    NEVER,
    /// Always replace
    ALWAYS,
    /**
     * Replace if update sequence number is higher (do not replace if USN
     * not available)
     */
    HIGHER_USN,
};

std::ostream& operator<<(std::ostream& o, ReplaceStrategy strategy);

/// Intended special user for a dataset
enum class DatasetUse {
    /// No special use intended
    DEFAULT,
    /// Store data that errored during dispatching
    ERRORS,
    /// Store duplicates detected during dispatching
    DUPLICATES,
};
std::ostream& operator<<(std::ostream& o, DatasetUse use);

} // namespace arki

#endif
