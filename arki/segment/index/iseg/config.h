#ifndef ARKI_SEGMENT_INDEX_ISEG__H
#define ARKI_SEGMENT_INDEX_ISEG__H

#include <arki/types/fwd.h>
#include <string>
#include <set>

namespace arki::segment::index::iseg {

/// Configuration for iseg indexing
struct Config
{
    std::string format;
    std::set<types::Code> index;
    std::set<types::Code> unique;
    bool trace_sql;

    /**
     * Trade write reliability and write concurrency in favour of performance.
     *
     * Useful for writing fast temporary private datasets.
     */
    bool eatmydata = false;
};

}

#endif
