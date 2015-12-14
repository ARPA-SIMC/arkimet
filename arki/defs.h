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
typedef std::function<bool(std::unique_ptr<Metadata>)> metadata_dest_func;


/**
 * Abstract interface to things that return a line of text at a time
 */
struct LineReader
{
    virtual ~LineReader() {}

    /**
     * Similar to std::getline, raises an exception if anything went wrong.
     *
     * Returns false if EOF is reached, in which case line will be empty.
     */
    virtual bool getline(std::string& line) = 0;

    /**
     * Check if the last getline returned eof. This is always false before
     * getline is called for the first time.
     */
    virtual bool eof() const = 0;
};


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

}

#endif
