/*
 * Copyright (C) 2007--2011  ARPA-SIM <urpsim@smr.arpa.emr.it>
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

#include <arki/tests/test-utils.h>
#include <arki/metadata/clusterer.h>
#include <arki/metadata/collection.h>
#include <arki/scan/any.h>

namespace tut {
using namespace std;
using namespace arki;

struct arki_metadata_clusterer_shar {
    metadata::Collection mdc;

    arki_metadata_clusterer_shar()
    {
        scan::scan("inbound/test.grib1", mdc);
    }
};
TESTGRP(arki_metadata_clusterer);

namespace {

struct ClusterCounter : public metadata::Clusterer
{
    unsigned clusters_processed;
    ClusterCounter()
        : metadata::Clusterer(), clusters_processed(0) {}

    virtual void flush()
    {
        ++clusters_processed;
        metadata::Clusterer::flush();
    }
};

}

// Test clustering by count
template<> template<>
void to::test<1>()
{
    ClusterCounter clusterer;
    clusterer.max_count = 10;

    for (unsigned i = 0; i < 121; ++i)
        clusterer(mdc[0]);
    clusterer.flush();

    atest(equals, 13u, clusterer.clusters_processed);
}

// Test clustering by size
template<> template<>
void to::test<2>()
{
    // mdc[0]'s data is 2234 bytes
    ClusterCounter clusterer;
    clusterer.max_bytes = 22340;

    for (unsigned i = 0; i < 121; ++i)
        clusterer(mdc[0]);
    clusterer.flush();

    atest(equals, 13u, clusterer.clusters_processed);
}

// Test clustering by interval
template<> template<>
void to::test<3>()
{
    ClusterCounter clusterer;
    clusterer.max_interval = 2; // month

    for (unsigned i = 0; i < 3; ++i)
        clusterer(mdc[i]);
    clusterer.flush();

    atest(equals, 2u, clusterer.clusters_processed);
}

// Test clustering by timerange
template<> template<>
void to::test<4>()
{
    ClusterCounter clusterer;
    clusterer.split_timerange = true;

    clusterer(mdc[0]);
    clusterer(mdc[2]);
    clusterer(mdc[1]);
    clusterer.flush();

    atest(equals, 2u, clusterer.clusters_processed);
}

}

// vim:set ts=4 sw=4:
