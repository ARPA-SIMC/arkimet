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

}

#endif
