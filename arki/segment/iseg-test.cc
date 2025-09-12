#include "arki/core/lock.h"
#include "arki/metadata/inbound.h"
#include "arki/utils/sys.h"
#include "iseg.h"
#include "scan.h"
#include "tests.h"

namespace {
using namespace std;
using namespace arki;
using namespace arki::utils;
using namespace arki::tests;

template <class Data> class Tests : public TestCase
{
    using TestCase::add_method;
    using TestCase::TestCase;

    void register_tests() override;

    void test_setup() override
    {
        sys::rmtree_ifexists("test");
        std::filesystem::create_directories("test");
    }
};

Tests<GRIBData> test_grib("arki_segment_iseg_grib");
Tests<BUFRData> test_bufr("arki_segment_iseg_bufr");
Tests<VM2Data> test_vm2("arki_segment_iseg_vm2");
Tests<ODIMData> test_odim("arki_segment_iseg_odim");
Tests<NCData> test_netcdf("arki_segment_iseg_netcdf");
Tests<JPEGData> test_jpeg("arki_segment_iseg_jpeg");

void wipe_segment(const Segment& segment)
{
    if (std::filesystem::is_directory(segment.abspath()))
        sys::rmtree_ifexists(segment.abspath());
    else
        std::filesystem::remove(segment.abspath());
}

std::shared_ptr<Segment> make_segment(TestData& td,
                                      const char* name = "test/test")
{
    auto session = std::make_shared<segment::iseg::Session>(".");
    return session->segment_from_relpath(
        sys::with_suffix(name, "." + format_name(td.format)));
}

template <typename Data> void Tests<Data>::register_tests()
{
    add_method("segment_from", [] {
        Data td;
        auto session = std::make_shared<arki::segment::iseg::Session>(".");
        auto segment1 =
            session->segment_from_relpath("foo." + format_name(td.format));
        wassert_true(segment1.get());
        auto segment2 = session->segment_from_relpath_and_format(
            "foo." + format_name(td.format), td.format);
        wassert_true(segment2.get());
    });

    add_method("reader_before_and_after_segment_creation", [] {
        Data td;
        auto segment = make_segment(td);
        wipe_segment(*segment);
        auto lock = std::make_shared<core::lock::NullReadLock>();

        // Create a reader for a missing segment, and keep it referenced
        auto reader0 = segment->reader(lock);
        {
            auto reader = reader0;
            wassert(actual(dynamic_cast<segment::EmptyReader*>(reader.get()))
                        .istrue());
            size_t count = 0;
            reader->read_all([&](std::shared_ptr<arki::Metadata>) noexcept {
                ++count;
                return true;
            });
            wassert(actual(count) == 0u);
        }

        // Import data in the segment
        {
            arki::metadata::InboundBatch batch;
            batch.add(td.mds.get(0));
            batch.add(td.mds.get(1));
            batch.add(td.mds.get(2));
            auto writer =
                segment->writer(std::make_shared<core::lock::NullAppendLock>());
            auto res = writer->acquire(batch, segment::WriterConfig());
            wassert(actual(res.count_ok) == 3u);
            wassert(actual(res.count_failed) == 0u);
            wassert(actual(res.segment_mtime) > 0);
            wassert(actual(res.data_timespan) ==
                    core::Interval(core::Time(2007, 7, 7),
                                   core::Time(2007, 10, 9, 0, 0, 1)));
        }

        // Create a reader again
        {
            auto reader = segment->reader(lock);
            wassert(actual(dynamic_cast<segment::iseg::Reader*>(reader.get()))
                        .istrue());
            size_t count = 0;
            reader->read_all([&](std::shared_ptr<arki::Metadata>) noexcept {
                ++count;
                return true;
            });
            wassert(actual(count) == 3u);
        }
    });

    add_method("reader_before_and_after_index_creation", [] {
        auto lock = std::make_shared<core::lock::NullReadLock>();
        Data td;
        auto segment = make_segment(td);
        wipe_segment(*segment);
        fill_scan_segment(segment, td.mds);

        wassert(actual(segment->abspath_iseg_index()).not_exists());

        // Create a reader for a segment without index, and keep it
        // referenced
        auto reader0 = segment->reader(lock);
        {
            auto reader = reader0;
            wassert(actual(dynamic_cast<segment::EmptyReader*>(reader.get()))
                        .istrue());
            size_t count = 0;
            reader->read_all([&](std::shared_ptr<arki::Metadata>) noexcept {
                ++count;
                return true;
            });
            wassert(actual(count) == 0u);
        }

        // Rescan the segment
        {
            auto checker =
                segment->checker(std::make_shared<core::lock::NullCheckLock>());
            auto md    = checker->scan();
            auto fixer = checker->fixer();
            fixer->reindex(md);
        }

        wassert(actual(segment->abspath_iseg_index()).exists());

        // Create a reader again
        {
            auto reader = segment->reader(lock);
            wassert(actual(dynamic_cast<segment::iseg::Reader*>(reader.get()))
                        .istrue());
            size_t count = 0;
            reader->read_all([&](std::shared_ptr<arki::Metadata>) noexcept {
                ++count;
                return true;
            });
            wassert(actual(count) == 3u);
        }
    });

    add_method("reader_before_and_after_index_outdated", [] {
        auto lock = std::make_shared<core::lock::NullReadLock>();
        Data td;
        auto segment = make_segment(td);
        wipe_segment(*segment);
        fill_scan_segment(segment, td.mds);

        // Create a partial and outdated index
        {
            auto checker =
                segment->checker(std::make_shared<core::lock::NullCheckLock>());
            auto md = checker->scan();
            wassert(actual(md.size()) == 3u);
            md.pop_back();
            auto fixer = checker->fixer();
            fixer->reindex(md);
        }
        sys::touch(segment->abspath_iseg_index(), 10);

        // Create a reader for a segment with outdated and partial index, and
        // keep it referenced
        auto reader0 = segment->reader(lock);
        {
            auto reader = reader0;
            wassert(actual(dynamic_cast<segment::EmptyReader*>(reader.get()))
                        .istrue());
            size_t count = 0;
            reader->read_all([&](std::shared_ptr<arki::Metadata>) noexcept {
                ++count;
                return true;
            });
            wassert(actual(count) == 0u);
        }

        // Rescan the segment
        {
            auto checker =
                segment->checker(std::make_shared<core::lock::NullCheckLock>());
            auto md = checker->scan();
            wassert(actual(md.size()) == 3u);
            auto fixer = checker->fixer();
            fixer->reindex(md);
        }

        wassert(actual(segment->abspath_iseg_index()).exists());

        // Create a reader again
        {
            auto reader = segment->reader(lock);
            wassert(actual(dynamic_cast<segment::iseg::Reader*>(reader.get()))
                        .istrue());
            size_t count = 0;
            reader->read_all([&](std::shared_ptr<arki::Metadata>) noexcept {
                ++count;
                return true;
            });
            wassert(actual(count) == 3u);
        }
    });
}

} // namespace
