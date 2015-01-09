/*
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

#include <arki/tests/tests.h>
#include <arki/metadata/clusterer.h>
#include <arki/metadata/collection.h>
#include <arki/scan/any.h>

namespace tut {
using namespace std;
using namespace arki;
using namespace wibble::tests;

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

    virtual void flush_batch()
    {
        ++clusters_processed;
        metadata::Clusterer::flush_batch();
    }
};

inline auto_ptr<Metadata> wrap(const Metadata& md)
{
    return auto_ptr<Metadata>(new Metadata(md));
}

}

// Test clustering by count
template<> template<>
void to::test<1>()
{
    WIBBLE_TEST_INFO(info);
    unsigned items[] = { 0, 1, 10, 11, 20, 21, 121 };
    for (unsigned* i = items; i != items + sizeof(items) / sizeof(items[0]); ++i)
    {
        info() << *i << " iterations";
        ClusterCounter clusterer;
        clusterer.max_count = 10;

        for (unsigned x = 0; x < *i; ++x)
            clusterer.eat(wrap(mdc[0]));
        clusterer.flush();

        wassert(actual(clusterer.clusters_processed) == (*i + 10 - 1) / 10);
    }
}

// Test clustering by size
template<> template<>
void to::test<2>()
{
    // mdc[0] has 7218b of data
    // mdc[1] has 34960b of data
    // mdc[2] has 2234b of data
    WIBBLE_TEST_INFO(info);
    unsigned items[] = { 0, 1, 10, 11, 20, 21, 121 };
    for (unsigned* i = items; i != items + sizeof(items) / sizeof(items[0]); ++i)
    {
        info() << *i << " iterations";
        // mdc[0]'s data is 7218 bytes, so this makes clusters of 10 elements
        ClusterCounter clusterer;
        clusterer.max_bytes = 72180;

        for (unsigned x = 0; x < *i; ++x)
            clusterer.eat(wrap(mdc[0]));
        clusterer.flush();

        wassert(actual(clusterer.clusters_processed) == (*i + 10 - 1) / 10);
    }

    // If max_bytes is bigger than the data, we should get a cluster with only the one item
    {
        ClusterCounter clusterer;
        clusterer.max_bytes = 1;

        clusterer.eat(wrap(mdc[0]));
        clusterer.flush();

        wassert(actual(clusterer.clusters_processed) == 1);
    }
    {
        ClusterCounter clusterer;
        clusterer.max_bytes = 8000;

        clusterer.eat(wrap(mdc[0]));
        clusterer.eat(wrap(mdc[1]));
        clusterer.eat(wrap(mdc[2]));
        clusterer.flush();

        wassert(actual(clusterer.clusters_processed) == 3);
    }
}

// Test clustering by interval
template<> template<>
void to::test<3>()
{
    ClusterCounter clusterer;
    clusterer.max_interval = 2; // month

    for (unsigned i = 0; i < 3; ++i)
        clusterer.eat(wrap(mdc[i]));
    clusterer.flush();

    wassert(actual(clusterer.clusters_processed) == 2u);
}

// Test clustering by timerange
template<> template<>
void to::test<4>()
{
    ClusterCounter clusterer;
    clusterer.split_timerange = true;

    clusterer.eat(wrap(mdc[0]));
    clusterer.eat(wrap(mdc[1]));
    clusterer.eat(wrap(mdc[2]));
    clusterer.flush();

    wassert(actual(clusterer.clusters_processed) == 2u);
}

}

// vim:set ts=4 sw=4:
