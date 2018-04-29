#include "arki/tests/tests.h"
#include "arki/utils/sys.h"
#include "compress.h"
#include <iostream>

namespace std {
static ostream& operator<<(ostream& out, const vector<uint8_t>& buf)
{
    return out.write((const char*)buf.data(), buf.size());
}
}

namespace {
using namespace std;
using namespace arki;
using namespace arki::tests;
using namespace arki::utils;
using namespace arki::utils::compress;

static void fill(vector<uint8_t>& buf, const std::string& pattern)
{
    for (size_t i = 0; i < buf.size(); ++i)
        buf[i] = pattern[i % pattern.size()];
}

class Tests : public TestCase
{
    using TestCase::TestCase;
    void register_tests() override;
} test("arki_utils_compress");

void Tests::register_tests() {

// Test compressing data that do not compress
add_method("incompressible", [] {
    vector<uint8_t> orig = { 'c', 'i', 'a', 'o' };
    vector<uint8_t> comp = lzo(orig.data(), orig.size());

    wassert(actual(comp.size()) == orig.size());
    wassert(actual(comp) == orig);
});

// Test a compression/decompression cycle
add_method("roundtrip", [] {
    // Make a long and easily compressible string
    vector<uint8_t> orig(4096);
    fill(orig, "ciao");

    vector<uint8_t> comp = lzo(orig.data(), orig.size());
    wassert(actual(comp.size()) < orig.size());
    wassert(actual(unlzo(comp.data(), comp.size(), orig.size())) == orig);
});

// Test a compression/decompression cycle on a large buffer
add_method("roundtrip_large", [] {
    // Make a long and easily compressible string
    vector<uint8_t> orig(1000000);
    fill(orig, "ciao");

    vector<uint8_t> comp = lzo(orig.data(), orig.size());
    wassert(actual(comp.size()) < orig.size());
    wassert(actual(unlzo(comp.data(), comp.size(), orig.size())) == orig);
});

// Test SeekIndex
add_method("seekindex", [] {
    SeekIndex idx;

    // Opening a nonexisting file returns false
    wassert(actual_file("this-file-does-not-exists").not_exists());
    wassert(actual(idx.read("this-file-does-not-exists")).isfalse());
});

// Test SeekIndex lookup
add_method("seekindex_lookup", [] {
    SeekIndex idx;

	// Some simple test data for a 4000bytes file compressed 50% exactly
	idx.ofs_unc.push_back(   0); idx.ofs_comp.push_back(   0);
	idx.ofs_unc.push_back(1000); idx.ofs_comp.push_back( 500);
	idx.ofs_unc.push_back(2000); idx.ofs_comp.push_back(1000);
	idx.ofs_unc.push_back(3000); idx.ofs_comp.push_back(1500);
	idx.ofs_unc.push_back(4000); idx.ofs_comp.push_back(2000);

    wassert(actual(idx.lookup(   0)) == 0u);
    wassert(actual(idx.lookup(   1)) == 0u);
    wassert(actual(idx.lookup( 999)) == 0u);
    wassert(actual(idx.lookup(1000)) == 1u);
    wassert(actual(idx.lookup(1001)) == 1u);
    wassert(actual(idx.lookup(1999)) == 1u);
    wassert(actual(idx.lookup(2000)) == 2u);
    wassert(actual(idx.lookup(2001)) == 2u);
    wassert(actual(idx.lookup(2999)) == 2u);
    wassert(actual(idx.lookup(3000)) == 3u);
    wassert(actual(idx.lookup(3001)) == 3u);
    wassert(actual(idx.lookup(3999)) == 3u);
    wassert(actual(idx.lookup(4000)) == 4u);
    wassert(actual(idx.lookup(4001)) == 4u);
    wassert(actual(idx.lookup(4999)) == 4u);
    wassert(actual(idx.lookup(9999)) == 4u);
});

add_method("gzipwriter", [] {
    sys::File fd("test.gz", O_WRONLY | O_CREAT | O_TRUNC);
    GzipWriter writer(fd);
    std::vector<uint8_t> data(4096, 't');
    for (unsigned i = 0; i < 10; ++i)
    {
        data[0] = i;
        wassert(writer.add(data));
    }
    wassert(writer.flush());
    fd.close();

    std::vector<uint8_t> d = gunzip("test.gz");
    wassert(actual(d.size()) == 40960u);
    for (unsigned i = 0; i < d.size(); ++i)
    {
        if ((i % 4096) == 0)
            wassert(actual(d[i]) == i / 4096);
        else
            wassert(actual(d[i]) == 't');
    }
});

add_method("gzipindexingwriter", [] {
    sys::File fd("test.gz", O_WRONLY | O_CREAT | O_TRUNC);
    sys::File idxfd("test.idx.gz", O_WRONLY | O_CREAT | O_TRUNC);
    GzipIndexingWriter writer(fd, 8);
    std::vector<uint8_t> data(4096, 't');
    for (unsigned i = 0; i < 10; ++i)
    {
        data[0] = i;
        wassert(writer.add(data));
        wassert(writer.close_entry());
    }
    wassert(writer.flush());
    wassert(writer.idx.write(idxfd));
    fd.close();
    idxfd.close();

    std::vector<uint8_t> d = gunzip("test.gz");
    wassert(actual(d.size()) == 40960u);
    for (unsigned i = 0; i < d.size(); ++i)
    {
        if ((i % 4096) == 0)
            wassert(actual(d[i]) == i / 4096);
        else
            wassert(actual(d[i]) == 't');
    }

    idxfd.open(O_RDONLY);
    SeekIndex idx;
    idx.read(idxfd);
    wassert(actual(idx.lookup(4096 * 0)) == 0u);
    wassert(actual(idx.lookup(4096 * 1)) == 0u);
    wassert(actual(idx.lookup(4096 * 7)) == 0u);
    wassert(actual(idx.lookup(4096 * 8)) == 1u);
    wassert(actual(idx.lookup(4096 * 9)) == 1u);

    // TODO: read at various offsets to see if the data is right
});

}

}
