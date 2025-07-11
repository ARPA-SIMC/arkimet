#include "arki/metadata.h"
#include "arki/tests/tests.h"
#include "clusterer.h"
#include "collection.h"

namespace {
using namespace std;
using namespace arki;
using namespace arki::tests;

struct ClusterCounter : public metadata::Clusterer
{
    unsigned clusters_processed;
    ClusterCounter() : metadata::Clusterer(), clusters_processed(0) {}

    void flush_batch() override
    {
        ++clusters_processed;
        metadata::Clusterer::flush_batch();
    }
};

inline std::shared_ptr<Metadata> wrap(const Metadata& md) { return md.clone(); }

class Tests : public TestCase
{
    using TestCase::TestCase;
    void register_tests() override;
} test("arki_metadata_clusterer");

void Tests::register_tests()
{

    // Test clustering by count
    add_method("by_count", [] {
        ARKI_UTILS_TEST_INFO(info);
        metadata::TestCollection mdc("inbound/test.grib1");
        unsigned items[] = {0, 1, 10, 11, 20, 21, 121};
        for (unsigned* i = items; i != items + sizeof(items) / sizeof(items[0]);
             ++i)
        {
            info() << *i << " iterations";
            ClusterCounter clusterer;
            clusterer.max_count = 10;

            for (unsigned x = 0; x < *i; ++x)
                clusterer.eat(wrap(mdc[0]));
            clusterer.flush();

            wassert(actual(clusterer.clusters_processed) == (*i + 10 - 1) / 10);
        }
    });

    // Test clustering by size
    add_method("by_size", [] {
        // mdc[0] has 7218b of data
        // mdc[1] has 34960b of data
        // mdc[2] has 2234b of data
        ARKI_UTILS_TEST_INFO(info);
        metadata::TestCollection mdc("inbound/test.grib1");
        unsigned items[] = {0, 1, 10, 11, 20, 21, 121};
        for (unsigned* i = items; i != items + sizeof(items) / sizeof(items[0]);
             ++i)
        {
            info() << *i << " iterations";
            // mdc[0]'s data is 7218 bytes, so this makes clusters of 10
            // elements
            ClusterCounter clusterer;
            clusterer.max_bytes = 72180;

            for (unsigned x = 0; x < *i; ++x)
                clusterer.eat(wrap(mdc[0]));
            clusterer.flush();

            wassert(actual(clusterer.clusters_processed) == (*i + 10 - 1) / 10);
        }

        // If max_bytes is bigger than the data, we should get a cluster with
        // only the one item
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

            wassert(actual(clusterer.clusters_processed) == 3u);
        }
    });

    // Test clustering by interval
    add_method("by_interval", [] {
        metadata::TestCollection mdc("inbound/test.grib1");
        ClusterCounter clusterer;
        clusterer.max_interval = 2; // month

        for (unsigned i = 0; i < 3; ++i)
            clusterer.eat(wrap(mdc[i]));
        clusterer.flush();

        wassert(actual(clusterer.clusters_processed) == 2u);
    });

    // Test clustering by timerange
    add_method("by_timerange", [] {
        metadata::TestCollection mdc("inbound/test.grib1");
        ClusterCounter clusterer;
        clusterer.split_timerange = true;

        clusterer.eat(wrap(mdc[0]));
        clusterer.eat(wrap(mdc[2]));
        clusterer.eat(wrap(mdc[1]));
        clusterer.flush();

        wassert(actual(clusterer.clusters_processed) == 2u);
    });
}

} // namespace
