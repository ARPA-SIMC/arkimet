#include "arki/segment/tests.h"
#include "arki/segment/data.h"
#include "arki/segment/data/tar.h"
#include "arki/segment/data/zip.h"
#include "arki/segment/data/gz.h"
#include "arki/core/lock.h"
#include "arki/matcher/parser.h"
#include "arki/query.h"
#include "arki/utils/sys.h"
#include "arki/utils/string.h"
#include "arki/types/source/blob.h"
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

add_method("checker", [](Fixture& f) {
    auto segment = f.create(f.td.mds);
    auto checker = segment->checker(std::make_shared<core::lock::NullCheckLock>());
    auto mds = checker->scan();

    wassert(actual(mds.size()) == f.td.mds.size());
    {
        ARKI_UTILS_TEST_INFO(subtest);

        for (size_t i = 0; i < mds.size(); ++i)
        {
            subtest() << "Metadata #" << i << "/" << mds.size();
            wassert(actual(mds[i]).is_similar(f.td.mds[i]));
        }
    }

    // TODO: Checker::update_data
});

add_method("mark_removed", [](Fixture& f) {
    auto segment = f.create(f.td.mds);
    auto checker = segment->checker(std::make_shared<core::lock::NullCheckLock>());
    auto mds = checker->scan();
    wassert(actual(mds[1]).contains("reftime", "2007-07-07T00:00:00Z"));

    std::set<uint64_t> to_delete;
    to_delete.emplace(mds[1].sourceBlob().offset);

    {
        // TODO: touch segment 1234000
        auto fixer = checker->fixer();
        auto res = fixer->mark_removed(to_delete);

        // TODO: wassert(actual(res.segment_mtime) > 1234000);
        wassert(actual(res.data_timespan) == core::Interval(core::Time(2007, 7, 8, 13), core::Time(2007, 10, 9, 0, 0, 1)));
    }

    auto reader = segment->reader(std::make_shared<core::lock::NullReadLock>());

    metadata::Collection new_contents;
    wassert_true(reader->read_all(new_contents.inserter_func()));
    wassert(actual(new_contents.size()) == 2u);
    wassert(actual(new_contents[0]).is_similar(f.td.mds[0]));
    wassert(actual(new_contents[1]).is_similar(f.td.mds[2]));
});

add_method("reorder", [](Fixture& f) {
    auto segment = f.create(f.td.mds);
    auto checker = segment->checker(std::make_shared<core::lock::NullCheckLock>());
    auto mds = checker->scan();

    metadata::Collection reversed;
    reversed.acquire(mds.get(2));
    reversed.acquire(mds.get(1));
    reversed.acquire(mds.get(0));

    {
        // TODO: touch segment 1234000
        auto fixer = checker->fixer();
        auto res = fixer->reorder(reversed, segment::data::RepackConfig());

        // TODO: wassert(actual(res.segment_mtime) > 1234000);
        // TODO: test size changes (requires holes)
        // TODO: res.size_pre = 0;
        // TODO: res.size_post = 0;
    }
});

add_method("tar", [](Fixture& f) {
    auto segment = f.create(f.td.mds);
    auto checker = segment->checker(std::make_shared<core::lock::NullCheckLock>());
    {
        // TODO: touch segment 1234000
        auto fixer = checker->fixer();
        auto res = fixer->tar();
        // TODO: wassert(actual(res.segment_mtime) > 1234000);
    }
    auto data = segment->data();
    wassert_true(dynamic_pointer_cast<segment::data::tar::Data>(data));
});

add_method("zip", [](Fixture& f) {
    auto segment = f.create(f.td.mds);
    auto checker = segment->checker(std::make_shared<core::lock::NullCheckLock>());
    {
        // TODO: touch segment 1234000
        auto fixer = checker->fixer();
        auto res = fixer->zip();
        // TODO: wassert(actual(res.segment_mtime) > 1234000);
    }
    auto data = segment->data();
    wassert_true(dynamic_pointer_cast<segment::data::zip::Data>(data));
});

add_method("compress", [](Fixture& f) {
    auto segment = f.create(f.td.mds);
    auto checker = segment->checker(std::make_shared<core::lock::NullCheckLock>());
    {
        // TODO: touch segment 1234000
        auto fixer = checker->fixer();
        auto res = fixer->compress(3);
        // TODO: wassert(actual(res.segment_mtime) > 1234000);
        wassert(actual(res.size_pre) > res.size_post);
    }
    auto data = segment->data();
    wassert_true(dynamic_pointer_cast<segment::data::gz::Data>(data));
});

add_method("remove", [](Fixture& f) {
    auto segment = f.create(f.td.mds);
    auto checker = segment->checker(std::make_shared<core::lock::NullCheckLock>());
    {
        // TODO: touch segment 1234000
        auto fixer = checker->fixer();
        auto res = fixer->remove(false);
        // TODO: wassert(actual(res.segment_mtime) == 1234000);
    }
    // TODO: check that index is gone
});

add_method("remove_with_data", [](Fixture& f) {
    auto segment = f.create(f.td.mds);
    auto checker = segment->checker(std::make_shared<core::lock::NullCheckLock>());
    {
        // TODO: touch segment 1234000
        auto fixer = checker->fixer();
        auto res = fixer->remove(true);
        // TODO: wassert(actual(res.segment_mtime) == 1234000);
    }
    // TODO: check that index is gone
    // TODO: check that segment is gone
});

add_method("reindex", [](Fixture& f) {
    auto segment = f.create(f.td.mds);
    auto checker = segment->checker(std::make_shared<core::lock::NullCheckLock>());
    auto mds = checker->scan();
    {
        // TODO: touch segment 1234000
        auto fixer = checker->fixer();
        // Remove the index
        fixer->remove(false);
        // TODO: check that the index is gone
        fixer->reindex(mds);
        // TODO: wassert(actual(res.segment_mtime) == 1234000);
    }
    // TODO: check that index has been recreated (for those that support it)
});

}

}
