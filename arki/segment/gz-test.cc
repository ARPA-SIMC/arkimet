#include "gz.h"
#include "tests.h"

namespace {
using namespace std;
using namespace arki;
using namespace arki::tests;
using namespace arki::utils;

template<class Segment, class Data>
class Tests : public SegmentTests<Segment, Data>
{
    using SegmentTests<Segment, Data>::SegmentTests;
    typedef typename SegmentTests<Segment, Data>::Fixture Fixture;
    void register_tests() override;
};

Tests<segment::gzconcat::Segment, GRIBData> test1("arki_segment_gz_grib_nogroup", segment::RepackConfig(0));
Tests<segment::gzconcat::Segment, BUFRData> test2("arki_segment_gz_bufr_nogroup", segment::RepackConfig(0));
Tests<segment::gzlines::Segment, VM2Data> test3("arki_segment_gzlines_vm2_nogroup", segment::RepackConfig(0));
Tests<segment::gzconcat::Segment, GRIBData> test4("arki_segment_gz_grib", segment::RepackConfig(2));
Tests<segment::gzconcat::Segment, BUFRData> test5("arki_segment_gz_bufr", segment::RepackConfig(2));
Tests<segment::gzlines::Segment, VM2Data> test6("arki_segment_gzlines_vm2", segment::RepackConfig(2));

std::filesystem::path gz(const std::filesystem::path& path) { return sys::with_suffix(path, ".gz"); }
std::filesystem::path gzidx(const std::filesystem::path& path) { return sys::with_suffix(path, ".gz.idx"); }

template<class Segment, class Data>
void Tests<Segment, Data>::register_tests() {
SegmentTests<Segment, Data>::register_tests();

this->add_method("noidx", [&](Fixture& f) {
    auto checker = f.create(segment::RepackConfig(0));
    wassert(actual_file(gz(f.abspath)).exists());
    wassert(actual_file(gzidx(f.abspath)).not_exists());

    auto p = wcallchecked(checker->repack(f.root, f.seg_mds, segment::RepackConfig(0)));
    wassert(p.commit());
    wassert(actual_file(gz(f.abspath)).exists());
    wassert(actual_file(gzidx(f.abspath)).not_exists());

    p = wcallchecked(checker->repack(f.root, f.seg_mds, segment::RepackConfig(1)));
    wassert(p.commit());
    wassert(actual_file(gz(f.abspath)).exists());
    wassert(actual_file(gzidx(f.abspath)).exists());
});

this->add_method("idx", [&](Fixture& f) {
    auto checker = f.create(segment::RepackConfig(1));
    wassert(actual_file(gz(f.abspath)).exists());
    wassert(actual_file(gzidx(f.abspath)).exists());

    auto p = wcallchecked(checker->repack(f.root, f.seg_mds, segment::RepackConfig(1)));
    wassert(p.commit());
    wassert(actual_file(gz(f.abspath)).exists());
    wassert(actual_file(gzidx(f.abspath)).exists());

    p = wcallchecked(checker->repack(f.root, f.seg_mds, segment::RepackConfig(0)));
    wassert(p.commit());
    wassert(actual_file(gz(f.abspath)).exists());
    wassert(actual_file(gzidx(f.abspath)).not_exists());
});

this->add_method("onegroup", [&](Fixture& f) {
    auto checker = f.create(segment::RepackConfig(1024));
    wassert(actual_file(gz(f.abspath)).exists());
    wassert(actual_file(gzidx(f.abspath)).not_exists());

    auto p = wcallchecked(checker->repack(f.root, f.seg_mds, segment::RepackConfig(1024)));
    wassert(p.commit());
    wassert(actual_file(gz(f.abspath)).exists());
    wassert(actual_file(gzidx(f.abspath)).not_exists());

    p = wcallchecked(checker->repack(f.root, f.seg_mds, segment::RepackConfig(0)));
    wassert(p.commit());
    wassert(actual_file(gz(f.abspath)).exists());
    wassert(actual_file(gzidx(f.abspath)).not_exists());
});


}
#if 0

template<class Segment, class Data>
void Tests<Segment, Data>::register_tests() {
SegmentTests<Segment, Data>::register_tests();

#if 0
// TODO
// Test compression
add_method("compression", [] {
#ifndef HAVE_DBALLE
    throw TestSkipped("BUFR support not available");
#else
    static const int repeats = 1024;

    // Create a test file with `repeats` BUFR entries
    std::string bufr = sys::read_file("inbound/test.bufr");
    wassert(actual(bufr.size()) > 0u);
    bufr = bufr.substr(0, 194);

    sys::File tf("compressed.bufr", O_WRONLY|O_CREAT|O_TRUNC);
    for (int i = 0; i < repeats; ++i)
        tf.write(bufr.data(), bufr.size());
    tf.close();

    // Create metadata for the big BUFR file
    metadata::TestCollection c(tf.name());
    wassert(actual(c.size()) == (size_t)repeats);

    // Compress the data file
    wassert(scan::compress(tf.name(), std::make_shared<core::lock::Null>(), 127));
    // Remove the original file
    sys::unlink(tf.name());
    for (auto& i: c)
    {
        i->drop_cached_data();
        i->sourceBlob().unlock();
    }

    // Ensure that all data can still be read
    utils::acct::gzip_data_read_count.reset();
    utils::acct::gzip_forward_seek_bytes.reset();
    utils::acct::gzip_idx_reposition_count.reset();
    auto reader = segment::Reader::for_pathname("bufr", ".", "compressed.bufr", "compressed.bufr", std::make_shared<core::lock::Null>());
    for (int i = 0; i < repeats; ++i)
    {
        c[i].sourceBlob().lock(reader);
        const auto& b = c[i].getData();
        wassert(actual(b.size()) == bufr.size());
        wassert(actual(memcmp(b.data(), bufr.data(), bufr.size())) == 0);
    }
    // We read linearly, so there are no seeks or repositions
    wassert(actual(utils::acct::gzip_data_read_count.val()) == (size_t)repeats);
    wassert(actual(utils::acct::gzip_forward_seek_bytes.val()) == 0u);
    wassert(actual(utils::acct::gzip_idx_reposition_count.val()) == 1u);

    for (auto& i: c)
    {
        i->drop_cached_data();
        i->sourceBlob().unlock();
    }

    // Try to read backwards to avoid sequential reads
    utils::acct::gzip_data_read_count.reset();
    utils::acct::gzip_forward_seek_bytes.reset();
    utils::acct::gzip_idx_reposition_count.reset();
    reader = segment::Reader::for_pathname("bufr", ".", "compressed.bufr", "compressed.bufr", std::make_shared<core::lock::Null>());
    for (int i = repeats-1; i >= 0; --i)
    {
        c[i].sourceBlob().lock(reader);
        const auto& b = c[i].getData();
        ensure_equals(b.size(), bufr.size());
        ensure(memcmp(b.data(), bufr.data(), bufr.size()) == 0);
    }
    wassert(actual(utils::acct::gzip_data_read_count.val()) == (size_t)repeats);
    wassert(actual(utils::acct::gzip_forward_seek_bytes.val()) == 12446264u);
    wassert(actual(utils::acct::gzip_idx_reposition_count.val()) == 9u);

    for (auto& i: c)
    {
        i->drop_cached_data();
        i->sourceBlob().unlock();
    }

    // Read each other one
    utils::acct::gzip_data_read_count.reset();
    utils::acct::gzip_forward_seek_bytes.reset();
    utils::acct::gzip_idx_reposition_count.reset();
    reader = segment::Reader::for_pathname("bufr", ".", "compressed.bufr", "compressed.bufr", std::make_shared<core::lock::Null>());
    for (int i = 0; i < repeats; i += 2)
    {
        c[i].sourceBlob().lock(reader);
        const auto& b = c[i].getData();
        ensure_equals(b.size(), bufr.size());
        ensure(memcmp(b.data(), bufr.data(), bufr.size()) == 0);
    }
    wassert(actual(utils::acct::gzip_data_read_count.val()) == (size_t)repeats / 2);
    wassert(actual(utils::acct::gzip_forward_seek_bytes.val()) == 194u * 511u);
    wassert(actual(utils::acct::gzip_idx_reposition_count.val()) == 1u);
#endif
});

// Test compression when the data don't compress
add_method("uncompressible", [] {
#ifndef HAVE_DBALLE
        throw TestSkipped("BUFR support not available");
#else
    // Create a collector with only one small metadata inside
    metadata::TestCollection c("inbound/test.bufr");
    wassert(actual(c.size()) == 3u);
    c.pop_back();
    c.pop_back();
    wassert(actual(c.size()) == 1u);

    c.writeAtomically("test.md");

    metadata::Collection c1;
    c1.read_from_file("test.md");
    wassert(actual(c.size()) == 1u);
    ensure(c[0] == c1[0]);
#endif
});
#endif

}
#endif

}

#include "tests.tcc"
