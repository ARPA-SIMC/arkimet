#include "gzidx.h"
#include "tests.h"
#include "arki/metadata/collection.h"
#include "arki/types/source/blob.h"
#include "arki/utils/files.h"
#include "arki/utils/sys.h"
#include "arki/utils/string.h"
#include <fcntl.h>
#include <sstream>

namespace {
using namespace std;
using namespace arki;
using namespace arki::tests;
using namespace arki::utils;

template<class Segment, class Data>
class Tests : public SegmentTests<Segment, Data>
{
    using SegmentTests<Segment, Data>::SegmentTests;
    void register_tests() override;
};

Tests<segment::gzidxconcat::Segment, GRIBData> test1("arki_segment_gzidx_grib");
Tests<segment::gzidxconcat::Segment, BUFRData> test2("arki_segment_gzidx_bufr");
Tests<segment::gzidxlines::Segment, VM2Data> test3("arki_segment_gzidxlines_vm2");

template<class Segment, class Data>
void Tests<Segment, Data>::register_tests() {
SegmentTests<Segment, Data>::register_tests();

#if 0
// TODO
// Test compression
add_method("compression", [] {
#ifndef HAVE_DBALLE
    throw TestSkipped();
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
    throw TestSkipped();
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

}

#include "tests.tcc"
