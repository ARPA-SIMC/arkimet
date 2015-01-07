#ifndef ARKI_DATASET_INDEX_SUMMARYCACHE_H
#define ARKI_DATASET_INDEX_SUMMARYCACHE_H

/*
 * dataset/index/summarycache - Cache precomputed summaries
 *
 * Copyright (C) 2007--2013  ARPA-SIM <urpsim@smr.arpa.emr.it>
 *
 * This program is free software; you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation; either version 2 of the License, or
 * (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License along
 * with this program; if not, write to the Free Software Foundation, Inc.,
 * 51 Franklin Street, Fifth Floor, Boston, MA 02110-1301 USA.
 *
 * Author: Enrico Zini <enrico@enricozini.com>
 */
#include <arki/types.h>
#include <arki/types/time.h>
#include <string>

namespace arki {
class Metadata;
class Summary;

namespace dataset {
namespace index {

class SummaryCache
{
protected:
    /// Absolute path to the summary cache directory
    std::string m_scache_root;

public:
    SummaryCache(const std::string& root);
    ~SummaryCache();

    void openRO();
    void openRW();

    /// Read summary for all the period. Returns true if found, false if not in cache.
    bool read(Summary& s);

    /// Read summary for the given month. Returns true if found, false if not in cache
    bool read(Summary& s, int year, int month);

    /// Write the global cache file; returns false if the cache dir is not writable.
    bool write(Summary& s);

    /// Write the cache file for the given month; returns false if the cache dir is not writable.
    bool write(Summary& s, int year, int month);

    /// Remove all contents of the cache
    void invalidate();

    /// Remove cache contents for the given month
    void invalidate(int year, int month);

    /// Remove cache contents for the period found in the given metadata
    void invalidate(const Metadata& md);

    /// Remove cache contents for all dates from tmin and tmax (inclusive)
    void invalidate(const types::Time& tmin, const types::Time& tmax);

    /// Run consistency checks on the summary cache
    bool check(const std::string& dsname, std::ostream& log) const;
};



}
}
}

#endif
