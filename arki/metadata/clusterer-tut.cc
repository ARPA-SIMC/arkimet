#include <arki/tests/tests.h>
#include <arki/metadata.h>
#include "clusterer.h"
#include "collection.h"
#include <arki/scan/any.h>

namespace tut {
using namespace std;
using namespace arki;
using namespace arki::tests;

struct arki_metadata_clusterer_shar {
    metadata::Collection mdc;

    arki_metadata_clusterer_shar()
    {
        scan::scan("inbound/test.grib1", mdc.inserter_func());
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

inline unique_ptr<Metadata> wrap(const Metadata& md)
{
    return unique_ptr<Metadata>(new Metadata(md));
}

}

// Test clustering by count
template<> template<>
void to::test<1>()
{
    ARKI_UTILS_TEST_INFO(info);
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
    ARKI_UTILS_TEST_INFO(info);
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

        wassert(actual(clusterer.clusters_processed) == 1u);
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
    clusterer.eat(wrap(mdc[2]));
    clusterer.eat(wrap(mdc[1]));
    clusterer.flush();

    wassert(actual(clusterer.clusters_processed) == 2u);
}

}

// vim:set ts=4 sw=4:
