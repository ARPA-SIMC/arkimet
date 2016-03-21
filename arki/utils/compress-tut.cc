#include "config.h"
#include <arki/tests/tests.h>
#include <arki/utils/compress.h>
#include <arki/utils/sys.h>
#include <sstream>
#include <iostream>

namespace std {
static ostream& operator<<(ostream& out, const vector<uint8_t>& buf)
{
    return out.write((const char*)buf.data(), buf.size());
}
}

namespace tut {
using namespace std;
using namespace arki;
using namespace arki::utils;
using namespace arki::tests;
using namespace arki::utils::compress;

static void fill(vector<uint8_t>& buf, const std::string& pattern)
{
    for (size_t i = 0; i < buf.size(); ++i)
        buf[i] = pattern[i % pattern.size()];
}

struct arki_utils_compress_shar {
};
TESTGRP(arki_utils_compress);

// Test compressing data that do not compress
def_test(1)
{
    vector<uint8_t> orig = { 'c', 'i', 'a', 'o' };
    vector<uint8_t> comp = lzo(orig.data(), orig.size());

    wassert(actual(comp.size()) == orig.size());
    wassert(actual(comp) == orig);
}

// Test a compression/decompression cycle
def_test(2)
{
	using namespace utils::compress;

    // Make a long and easily compressible string
    vector<uint8_t> orig(4096);
    fill(orig, "ciao");

    vector<uint8_t> comp = lzo(orig.data(), orig.size());
    wassert(actual(comp.size()) < orig.size());
    wassert(actual(unlzo(comp.data(), comp.size(), orig.size())) == orig);
}

// Test a compression/decompression cycle on a large buffer
def_test(3)
{
	using namespace utils::compress;

    // Make a long and easily compressible string
    vector<uint8_t> orig(1000000);
    fill(orig, "ciao");

    vector<uint8_t> comp = lzo(orig.data(), orig.size());
    wassert(actual(comp.size()) < orig.size());
    wassert(actual(unlzo(comp.data(), comp.size(), orig.size())) == orig);
}

// Test SeekIndex
def_test(4)
{
	SeekIndex idx;

    // Opening a nonexisting file returns false
    wassert(actual_file("this-file-does-not-exists").not_exists());
    wassert(actual(idx.read("this-file-does-not-exists")).isfalse());
}

// Test SeekIndex lookup
def_test(5)
{
	SeekIndex idx;

	// Some simple test data for a 4000bytes file compressed 50% exactly
	idx.ofs_unc.push_back(   0); idx.ofs_comp.push_back(   0);
	idx.ofs_unc.push_back(1000); idx.ofs_comp.push_back( 500);
	idx.ofs_unc.push_back(2000); idx.ofs_comp.push_back(1000);
	idx.ofs_unc.push_back(3000); idx.ofs_comp.push_back(1500);
	idx.ofs_unc.push_back(4000); idx.ofs_comp.push_back(2000);

	ensure_equals(idx.lookup(   0), 0u);
	ensure_equals(idx.lookup(   1), 0u);
	ensure_equals(idx.lookup( 999), 0u);
	ensure_equals(idx.lookup(1000), 1u);
	ensure_equals(idx.lookup(1001), 1u);
	ensure_equals(idx.lookup(1999), 1u);
	ensure_equals(idx.lookup(2000), 2u);
	ensure_equals(idx.lookup(2001), 2u);
	ensure_equals(idx.lookup(2999), 2u);
	ensure_equals(idx.lookup(3000), 3u);
	ensure_equals(idx.lookup(3001), 3u);
	ensure_equals(idx.lookup(3999), 3u);
	ensure_equals(idx.lookup(4000), 4u);
	ensure_equals(idx.lookup(4001), 4u);
	ensure_equals(idx.lookup(4999), 4u);
	ensure_equals(idx.lookup(9999), 4u);
}

}
