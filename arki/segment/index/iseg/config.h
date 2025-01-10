#ifndef ARKI_SEGMENT_INDEX_ISEG__H
#define ARKI_SEGMENT_INDEX_ISEG__H

#include <arki/types/fwd.h>
#include <string>
#include <set>

namespace arki::segment::index::iseg {

/// Configuration for iseg indexing
struct Config
{
    DataFormat format;
    std::set<types::Code> index;
    std::set<types::Code> unique;
    bool trace_sql;

    /**
     * If true, try to store the content of small files in the index if
     * possible, to avoid extra I/O when querying
     */
    bool smallfiles = false;

    /**
     * Trade write reliability and write concurrency in favour of performance.
     *
     * Useful for writing fast temporary private datasets.
     */
    bool eatmydata = false;
};

}

#endif
