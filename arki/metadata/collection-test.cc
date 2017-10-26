#include <arki/libconfig.h>
#include <arki/types/tests.h>
#include <arki/metadata.h>
#include <arki/metadata/collection.h>
#include <arki/utils.h>
#include <arki/utils/accounting.h>
#include <arki/utils/sys.h>
#include <arki/types/source.h>
#include <arki/types/source/blob.h>
#include <arki/summary.h>
#include <arki/scan/any.h>
#include <arki/runtime/io.h>
#include <cstring>

#include <sstream>
#include <iostream>

namespace {
using namespace std;
using namespace arki;
using namespace arki::utils;
using namespace arki::tests;

class Tests : public TestCase
{
    using TestCase::TestCase;
    void register_tests() override;
} test("arki_metadata_collection");

void Tests::register_tests() {

// Test compression
add_method("compression", [] {
    metadata::Collection c;

#ifdef HAVE_DBALLE
    static const int repeats = 1024;

    // Create a test file with `repeats` BUFR entries
    std::string bufr = sys::read_file("inbound/test.bufr");
    wassert(actual(bufr.size()) > 0u);
    bufr = bufr.substr(0, 194);

    sys::File tf(sys::File::mkstemp("test"));
    for (int i = 0; i < repeats; ++i)
        tf.write(bufr.data(), bufr.size());
    tf.close();

    // Create metadata for the big BUFR file
    scan::scan(tf.name(), c.inserter_func(), "bufr");
    wassert(actual(c.size()) == (size_t)repeats);

    // Compress the data file
    c.compressDataFile(127, "temp BUFR " + tf.name());
    // Remove the original file
    sys::unlink(tf.name());
    for (auto& i: c)
    {
        i->drop_cached_data();
        i->sourceBlob().unlock();
    }
    Metadata::flushDataReaders();

    // Ensure that all data can still be read
    utils::acct::gzip_data_read_count.reset();
    utils::acct::gzip_forward_seek_bytes.reset();
    utils::acct::gzip_idx_reposition_count.reset();
    for (int i = 0; i < repeats; ++i)
    {
        c[i].sourceBlob().lock();
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
    Metadata::flushDataReaders();

    // Try to read backwards to avoid sequential reads
    utils::acct::gzip_data_read_count.reset();
    utils::acct::gzip_forward_seek_bytes.reset();
    utils::acct::gzip_idx_reposition_count.reset();
    for (int i = repeats-1; i >= 0; --i)
    {
        c[i].sourceBlob().lock();
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
    Metadata::flushDataReaders();

    // Read each other one
    utils::acct::gzip_data_read_count.reset();
    utils::acct::gzip_forward_seek_bytes.reset();
    utils::acct::gzip_idx_reposition_count.reset();
    for (int i = 0; i < repeats; i += 2)
    {
        c[i].sourceBlob().lock();
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
    metadata::Collection c;
#ifdef HAVE_DBALLE
    // Create a collector with only one small metadata inside
    scan::scan("inbound/test.bufr", c.inserter_func());
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

}

}
