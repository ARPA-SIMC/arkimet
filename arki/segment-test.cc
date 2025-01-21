#include "arki/segment/tests.h"
#include "arki/core/lock.h"
#include "arki/matcher/parser.h"
#include "arki/query.h"
#include "arki/utils/sys.h"
#include "arki/utils/string.h"
#include "segment.h"
#include "segment/metadata.h"
#include <type_traits>

namespace {
using namespace std;
using namespace arki;
using namespace arki::utils;
using namespace arki::tests;

template<class Base>
class TestFixture : public Base
{
    // static_assert(std::is_base_of_v<SegmentTestFixture<Any>, Base>, "Base is not a SegmentTestFixture");

public:
};

template<class Base>
class Tests : public FixtureTestCase<TestFixture<Base>>
{
    using FixtureTestCase<TestFixture<Base>>::FixtureTestCase;
    using FixtureTestCase<TestFixture<Base>>::add_method;
    using typename FixtureTestCase<TestFixture<Base>>::Fixture;

    void register_tests() override;
};

Tests<ScanSegmentFixture<GRIBData>> tests1("arki_segment_scan_grib");
Tests<ScanSegmentFixture<VM2Data>> tests2("arki_segment_scan_vm2");
Tests<ScanSegmentFixture<ODIMData>> tests3("arki_segment_scan_odim");

Tests<MetadataSegmentFixture<GRIBData>> testm1("arki_segment_metadata_grib");
Tests<MetadataSegmentFixture<VM2Data>> testm2("arki_segment_metadata_vm2");
Tests<MetadataSegmentFixture<ODIMData>> testm3("arki_segment_metadata_odim");

Tests<IsegSegmentFixture<GRIBData>> testi1("arki_segment_iseg_grib");
Tests<IsegSegmentFixture<VM2Data>> testi2("arki_segment_iseg_vm2");
Tests<IsegSegmentFixture<ODIMData>> testi3("arki_segment_iseg_odim");

template<typename Base>
void Tests<Base>::register_tests() {

add_method("reader", [](Fixture& f) {
    matcher::Parser parser;
    auto segment = f.create(f.td.mds);
    auto reader = segment->reader(std::make_shared<core::lock::NullReadLock>());

    wassert(actual(&reader->segment()) == segment.get());

    metadata::Collection mds;
    wassert_true(reader->read_all(mds.inserter_func()));
    wassert(actual(mds.size()) == f.td.mds.size());
    {
        ARKI_UTILS_TEST_INFO(subtest);

        for (size_t i = 0; i < mds.size(); ++i)
        {
            subtest() << "Metadata #" << i << "/" << mds.size();
            wassert(actual(mds[i]).is_similar(f.td.mds[i]));
        }
    }

    mds.clear();
    wassert_true(reader->query_data(parser.parse("reftime:=2007-07-07"), mds.inserter_func()));
    wassert(actual(mds.size()) == 1u);
    wassert(actual(mds[0]).is_similar(f.td.mds[1]));

    Summary summary;
    reader->query_summary(Matcher(), summary);
    wassert(actual(summary.get_reference_time()) == core::Interval(core::Time(2007, 7, 7), core::Time(2007, 10, 9, 0, 0, 1)));

    summary.clear();
    reader->query_summary(parser.parse("reftime:=2007-07-07"), summary);
    wassert(actual(summary.get_reference_time()) == core::Interval(core::Time(2007, 7, 7), core::Time(2007, 7, 7, 0, 0, 1)));
});

}

}
