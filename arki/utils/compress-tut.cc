/*
 * Copyright (C) 2010  ARPA-SIM <urpsim@smr.arpa.emr.it>
 *
 * This program is free software; you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation; either version 2 of the License, or
 * (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License along
 * with this program; if not, write to the Free Software Foundation, Inc.,
 * 51 Franklin Street, Fifth Floor, Boston, MA 02110-1301 USA.
 *
 * Author: Enrico Zini <enrico@enricozini.com>
 */

#include <arki/tests/test-utils.h>
#include <arki/utils/compress.h>
#include <wibble/sys/fs.h>

#include <sstream>
#include <iostream>

namespace tut {
using namespace std;
using namespace arki;
using namespace wibble;
using namespace arki::utils::compress;

static void fill(sys::Buffer buf, const std::string& pattern)
{
	char* b = (char*)buf.data();
	for (size_t i = 0; i < buf.size(); ++i)
		b[i] = pattern[i % pattern.size()];
}

struct arki_utils_compress_shar {
};
TESTGRP(arki_utils_compress);

// Test compressing data that do not compress
template<> template<>
void to::test<1>()
{
	sys::Buffer orig("ciao", 4);
	sys::Buffer comp = lzo(orig.data(), orig.size());

	ensure_equals(comp.size(), orig.size());
	ensure(comp == orig);
}

// Test a compression/decompression cycle
template<> template<>
void to::test<2>()
{
	using namespace utils::compress;

	// Make a long and easily compressible string
	sys::Buffer orig(4096);
	fill(orig, "ciao");

	sys::Buffer comp = lzo(orig.data(), orig.size());
	ensure(comp.size() < orig.size());
	ensure(unlzo(comp.data(), comp.size(), orig.size()) == orig);
}

// Test a compression/decompression cycle on a large buffer
template<> template<>
void to::test<3>()
{
	using namespace utils::compress;

	// Make a long and easily compressible string
	sys::Buffer orig(1000000);
	fill(orig, "ciao");

	sys::Buffer comp = lzo(orig.data(), orig.size());
	ensure(comp.size() < orig.size());
	ensure(unlzo(comp.data(), comp.size(), orig.size()) == orig);
}

// Test SeekIndex
template<> template<>
void to::test<4>()
{
	SeekIndex idx;

	// Opening a nonexisting file returns false
	ensure(!sys::fs::access("this-file-does-not-exists", F_OK));
	ensure(!idx.read("this-file-does-not-exists"));
}

// Test SeekIndex lookup
template<> template<>
void to::test<5>()
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

// vim:set ts=4 sw=4:
