#ifndef ARKI_DATASET_MEMORY_H
#define ARKI_DATASET_MEMORY_H

/*
 * dataset/memory - Dataset interface for metadata::Collection
 *
 * Copyright (C) 2007--2015  ARPA-SIM <urpsim@smr.arpa.emr.it>
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

#include <arki/metadata/collection.h>
#include <arki/dataset.h>

namespace arki {
struct Summary;

namespace sort {
struct Compare;
}

namespace dataset {

/**
 * Consumer that collects all metadata into a vector
 */
struct Memory : public metadata::Collection, public Reader
{
public:
    Memory();
    virtual ~Memory();

    void query_data(const dataset::DataQuery& q, std::function<bool(std::unique_ptr<Metadata>)> dest) override;
    void querySummary(const Matcher& matcher, Summary& summary) override;
};

}
}

#endif

