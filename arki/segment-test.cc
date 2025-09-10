#include "arki/core/lock.h"
#include "arki/matcher/parser.h"
#include "arki/query.h"
#include "arki/segment/data.h"
#include "arki/segment/data/gz.h"
#include "arki/segment/data/tar.h"
#include "arki/segment/data/zip.h"
#include "arki/segment/reporter.h"
#include "arki/segment/tests.h"
#include "arki/types/source/blob.h"
#include "arki/utils/string.h"
#include "arki/utils/sys.h"
#include "segment.h"
#include "segment/metadata.h"
#include <type_traits>

namespace {
using namespace std;
using namespace arki;
using namespace arki::utils;
using namespace arki::tests;

template <class Base> class TestFixture : public Base
{
    // static_assert(std::is_base_of_v<SegmentTestFixture<Any>, Base>, "Base is
    // not a SegmentTestFixture");

public:
};

template <class Base> class Tests : public FixtureTestCase<TestFixture<Base>>
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

template <typename Base> void Tests<Base>::register_tests()
{

    add_method("reader", [](Fixture& f) {
        matcher::Parser parser;
        auto segment = f.create(f.td.mds);
        auto reader =
            segment->reader(std::make_shared<core::lock::NullReadLock>());

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
        wassert_true(reader->query_data(parser.parse("reftime:=2007-07-07"),
                                        mds.inserter_func()));
        wassert(actual(mds.size()) == 1u);
        wassert(actual(mds[0]).is_similar(f.td.mds[1]));

        Summary summary;
        reader->query_summary(Matcher(), summary);
        wassert(actual(summary.get_reference_time()) ==
                core::Interval(core::Time(2007, 7, 7),
                               core::Time(2007, 10, 9, 0, 0, 1)));

        summary.clear();
        reader->query_summary(parser.parse("reftime:=2007-07-07"), summary);
        wassert(actual(summary.get_reference_time()) ==
                core::Interval(core::Time(2007, 7, 7),
                               core::Time(2007, 7, 7, 0, 0, 1)));
    });

    add_method("read_relpath", [](Fixture& f) {
        matcher::Parser parser;
        auto segment = f.create(f.td.mds, "20/2007");
        auto reader =
            segment->reader(std::make_shared<core::lock::NullReadLock>());

        metadata::Collection mds;
        wassert_true(reader->read_all(mds.inserter_func()));
        wassert(actual_type(mds[0].source())
                    .is_source_blob(f.td.format, segment->root(),
                                    segment->relpath()));

        mds.clear();
        wassert_true(reader->query_data(parser.parse("reftime:=2007-07-07"),
                                        mds.inserter_func()));
        wassert(actual_type(mds[0].source())
                    .is_source_blob(f.td.format, segment->root(),
                                    segment->relpath()));

        auto checker =
            segment->checker(std::make_shared<core::lock::NullCheckLock>());
        mds = checker->scan();
        wassert(actual_type(mds[0].source())
                    .is_source_blob(f.td.format, segment->root(),
                                    segment->relpath()));
    });

    add_method("checker", [](Fixture& f) {
        auto segment = f.create(f.td.mds);
        auto checker =
            segment->checker(std::make_shared<core::lock::NullCheckLock>());
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
        auto checker =
            segment->checker(std::make_shared<core::lock::NullCheckLock>());
        auto mds = checker->scan();
        wassert(actual(mds[1]).contains("reftime", "2007-07-07T00:00:00Z"));

        std::set<uint64_t> to_delete;
        to_delete.emplace(mds[1].sourceBlob().offset);

        {
            auto fixer = checker->fixer();
            auto res   = fixer->mark_removed(to_delete);
            // Without an index the data is immediately removed
            if (f.has_index())
                wassert(actual(res.segment_mtime) == f.initial_mtime);
            else
                wassert(actual(res.segment_mtime) > f.initial_mtime);
            wassert(actual(res.data_timespan) ==
                    core::Interval(core::Time(2007, 7, 8, 13),
                                   core::Time(2007, 10, 9, 0, 0, 1)));
        }

        auto reader =
            segment->reader(std::make_shared<core::lock::NullReadLock>());

        metadata::Collection new_contents;
        wassert_true(reader->read_all(new_contents.inserter_func()));
        wassert(actual(new_contents.size()) == 2u);
        wassert(actual(new_contents[0]).is_similar(f.td.mds[0]));
        wassert(actual(new_contents[1]).is_similar(f.td.mds[2]));
    });

    add_method("reorder", [](Fixture& f) {
        auto segment = f.create(f.td.mds);
        auto checker =
            segment->checker(std::make_shared<core::lock::NullCheckLock>());
        auto mds = checker->scan();

        metadata::Collection reversed;
        reversed.acquire(mds.get(2));
        reversed.acquire(mds.get(1));
        reversed.acquire(mds.get(0));

        {
            auto fixer = checker->fixer();
            auto res   = fixer->reorder(reversed, segment::RepackConfig());
            wassert(actual(res.segment_mtime) > f.initial_mtime);
            // TODO: test size changes (requires holes)
            // TODO: res.size_pre = 0;
            // TODO: res.size_post = 0;
        }
    });

    add_method("tar", [](Fixture& f) {
        auto segment = f.create(f.td.mds);
        auto checker =
            segment->checker(std::make_shared<core::lock::NullCheckLock>());
        {
            auto fixer = checker->fixer();
            auto res   = fixer->tar();
            wassert(actual(res.segment_mtime) > f.initial_mtime);
        }
        auto data = segment::Data::create(segment);
        wassert_true(dynamic_pointer_cast<segment::data::tar::Data>(data));
    });

    add_method("zip", [](Fixture& f) {
        auto segment = f.create(f.td.mds);
        auto checker =
            segment->checker(std::make_shared<core::lock::NullCheckLock>());
        {
            auto fixer = checker->fixer();
            auto res   = fixer->zip();
            wassert(actual(res.segment_mtime) > f.initial_mtime);
        }
        auto data = segment::Data::create(segment);
        wassert_true(dynamic_pointer_cast<segment::data::zip::Data>(data));
    });

    add_method("compress", [](Fixture& f) {
        auto segment = f.create(f.td.mds);
        auto checker =
            segment->checker(std::make_shared<core::lock::NullCheckLock>());
        {
            auto fixer = checker->fixer();
            auto res   = fixer->compress(3);
            wassert(actual(res.segment_mtime) > f.initial_mtime);
            wassert(actual(res.size_pre) > res.size_post);
        }
        auto data = segment::Data::create(segment);
        wassert_true(dynamic_pointer_cast<segment::data::gz::Data>(data));
    });

    add_method("remove", [](Fixture& f) {
        auto segment = f.create(f.td.mds);
        auto checker =
            segment->checker(std::make_shared<core::lock::NullCheckLock>());
        {
            auto fixer = checker->fixer();
            /*auto res   =*/fixer->remove(false);
            // TODO: check that res is the previous size of the indices
        }
        wassert(actual(segment).data_mtime_equal(f.initial_mtime));
        // TODO: check that index is gone
    });

    add_method("remove_with_data", [](Fixture& f) {
        auto segment = f.create(f.td.mds);
        auto checker =
            segment->checker(std::make_shared<core::lock::NullCheckLock>());
        {
            auto fixer = checker->fixer();
            /*auto res   =*/fixer->remove(true);
            // TODO: check that res is the previous size of the indices plus
            // size of data
        }
        // TODO: check that index is gone
        // TODO: check that segment is gone
    });

    add_method("move", [](Fixture& f) {
        auto segment = f.create(f.td.mds);
        auto dest    = segment->session().segment_from_relpath(
            "test-moved."s + format_name(f.td.format));
        auto checker =
            segment->checker(std::make_shared<core::lock::NullCheckLock>());
        {
            auto fixer = checker->fixer();
            fixer->move(dest);
        }
        wassert(actual(dest).data_mtime_equal(f.initial_mtime));
        // TODO: check that index is moved
    });

    add_method("reindex", [](Fixture& f) {
        auto segment = f.create(f.td.mds);
        auto checker =
            segment->checker(std::make_shared<core::lock::NullCheckLock>());
        auto mds = checker->scan();
        {
            auto fixer = checker->fixer();
            // Remove the index
            fixer->remove(false);
            // TODO: check that the index is gone
            fixer->reindex(mds);
        }
        wassert(actual(segment).data_mtime_equal(f.initial_mtime));
        // TODO: check that index has been recreated (for those that support it)
    });

    add_method("fsck_no_data", [](Fixture& f) {
        auto segment = f.create_segment();
        auto checker =
            segment->checker(std::make_shared<core::lock::NullCheckLock>());
        segment::NullReporter reporter;
        auto res = checker->fsck(reporter);
        wassert(actual(res.size) == 0u);
        wassert(actual(res.mtime) == 0u);
        wassert(actual(res.interval) == core::Interval());
        wassert(actual(res.state) == segment::SEGMENT_MISSING);
    });

    add_method("fsck_no_metadata", [](Fixture& f) {
        f.skip_unless_metadata();
        auto segment = f.create(f.td.mds);
        std::filesystem::remove(segment->abspath_metadata());
        auto checker =
            segment->checker(std::make_shared<core::lock::NullCheckLock>());
        segment::NullReporter reporter;
        auto res = checker->fsck(reporter);
        wassert(actual(res.size) > 0u);
        wassert(actual(res.mtime) == f.initial_mtime);
        wassert(actual(res.interval) == core::Interval());
        wassert(actual(res.state) == segment::SEGMENT_UNALIGNED);
    });

    add_method("fsck_no_summary", [](Fixture& f) {
        f.skip_unless_metadata();
        auto mds = f.td.mds;
        mds.sort_segment();
        auto segment = f.create(mds);
        std::filesystem::remove(segment->abspath_summary());
        auto checker =
            segment->checker(std::make_shared<core::lock::NullCheckLock>());
        segment::NullReporter reporter;
        auto res = checker->fsck(reporter);
        wassert(actual(res.size) > 0u);
        wassert(actual(res.mtime) == f.initial_mtime);
        wassert(actual(res.interval) != core::Interval());
        wassert(actual(res.state) == segment::SEGMENT_UNOPTIMIZED);
    });

    add_method("fsck_no_iseg_index", [](Fixture& f) {
        f.skip_unless_iseg();
        auto mds = f.td.mds;
        mds.sort_segment();
        auto segment = f.create(mds);
        std::filesystem::remove(segment->abspath_iseg_index());
        auto checker =
            segment->checker(std::make_shared<core::lock::NullCheckLock>());
        segment::NullReporter reporter;
        auto res = checker->fsck(reporter);
        wassert(actual(res.size) > 0u);
        wassert(actual(res.mtime) == f.initial_mtime);
        wassert(actual(res.interval) == core::Interval());
        wassert(actual(res.state) == segment::SEGMENT_UNALIGNED);
    });

    add_method("fsck_deleted", [](Fixture& f) {
        metadata::Collection mds;
        auto segment = f.create(mds);
        auto checker =
            segment->checker(std::make_shared<core::lock::NullCheckLock>());
        segment::NullReporter reporter;
        auto res = checker->fsck(reporter);
        wassert(actual(res.size) == 0u);
        wassert(actual(res.mtime) == f.initial_mtime);
        wassert(actual(res.interval) == core::Interval());
        wassert(actual(res.state) == segment::SEGMENT_DELETED);
    });

    add_method("test_mark_all_removed", [](Fixture& f) {
        f.skip_unless_has_index();
        auto segment   = f.create(f.td.mds);
        auto data      = segment::Data::create(segment);
        auto data_size = data->size();
        auto checker =
            segment->checker(std::make_shared<core::lock::NullCheckLock>());
        auto fixer = checker->fixer();
        fixer->test_mark_all_removed();

        segment::NullReporter reporter;
        auto res = checker->fsck(reporter);
        wassert(actual(res.size) == data_size);
        wassert(actual(res.mtime) == f.initial_mtime);
        wassert(actual(res.interval) == core::Interval());
        wassert(actual(res.state) == segment::SEGMENT_DELETED);
    });

    add_method("test_make_overlap", [](Fixture& f) {
        f.skip_unless_has_index();
        auto segment = f.create(f.td.mds);

        auto checker =
            segment->checker(std::make_shared<core::lock::NullCheckLock>());
        {
            auto fixer = checker->fixer();
            fixer->test_make_overlap(10, 1);
        }

        auto mds = checker->scan();
    });

    add_method("test_make_hole", [](Fixture& f) {
        f.skip_unless_has_index();
        auto segment = f.create(f.td.mds);

        auto checker =
            segment->checker(std::make_shared<core::lock::NullCheckLock>());
        {
            auto fixer = checker->fixer();
            fixer->test_make_hole(10, 1);
        }
        auto mds          = checker->scan();
        const auto& blob0 = mds[0].sourceBlob();
        const auto& blob1 = mds[1].sourceBlob();
        auto data         = segment::Data::create(segment);
        wassert(actual(blob1.offset) ==
                data->next_offset(blob0.offset, blob0.size) + 10);
    });
}

} // namespace
