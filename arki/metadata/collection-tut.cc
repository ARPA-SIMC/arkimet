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

namespace tut {
using namespace std;
using namespace wibble::tests;
using namespace arki;
using namespace arki::types;
using namespace arki::utils;
using namespace arki::metadata;

struct arki_metadata_collection_shar {
	Collection c;

	arki_metadata_collection_shar()
	{
	}

	void acquireSamples()
	{
		scan::scan("inbound/test.grib1", c);
	}
};
TESTGRP(arki_metadata_collection);

// Test compression
template<> template<>
void to::test<1>()
{
#ifdef HAVE_DBALLE
    static const int repeats = 1024;

    // Create a test file with `repeats` BUFR entries
    std::string bufr = sys::read_file("inbound/test.bufr");
    ensure(bufr.size() > 0);
    bufr = bufr.substr(0, 194);

    sys::File tf(sys::File::mkstemp("test"));
    for (int i = 0; i < repeats; ++i)
        tf.write(bufr.data(), bufr.size());
    tf.close();

    // Create metadata for the big BUFR file
    scan::scan(tf.name(), c, "bufr");
    ensure_equals(c.size(), (size_t)repeats);

    // Compress the data file
    c.compressDataFile(127, "temp BUFR " + tf.name());
    // Remove the original file
    sys::unlink(tf.name());
    Metadata::flushDataReaders();
    for (Collection::const_iterator i = c.begin(); i != c.end(); ++i)
        (*i)->drop_cached_data();

	// Ensure that all data can still be read
	utils::acct::gzip_data_read_count.reset();
	utils::acct::gzip_forward_seek_bytes.reset();
	utils::acct::gzip_idx_reposition_count.reset();
    for (int i = 0; i < repeats; ++i)
    {
        const auto& b = c[i].getData();
        ensure_equals(b.size(), bufr.size());
        ensure(memcmp(b.data(), bufr.data(), bufr.size()) == 0);
    }
	// We read linearly, so there are no seeks or repositions
	ensure_equals(utils::acct::gzip_data_read_count.val(), (size_t)repeats);
	ensure_equals(utils::acct::gzip_forward_seek_bytes.val(), 0u);
	ensure_equals(utils::acct::gzip_idx_reposition_count.val(), 1u);

    Metadata::flushDataReaders();
    for (Collection::const_iterator i = c.begin(); i != c.end(); ++i)
        (*i)->drop_cached_data();

	// Try to read backwards to avoid sequential reads
	utils::acct::gzip_data_read_count.reset();
	utils::acct::gzip_forward_seek_bytes.reset();
	utils::acct::gzip_idx_reposition_count.reset();
    for (int i = repeats-1; i >= 0; --i)
    {
        const auto& b = c[i].getData();
        ensure_equals(b.size(), bufr.size());
        ensure(memcmp(b.data(), bufr.data(), bufr.size()) == 0);
    }
	ensure_equals(utils::acct::gzip_data_read_count.val(), (size_t)repeats);
	ensure_equals(utils::acct::gzip_forward_seek_bytes.val(), 12446264u);
	ensure_equals(utils::acct::gzip_idx_reposition_count.val(), 9u);

    Metadata::flushDataReaders();
    for (Collection::const_iterator i = c.begin(); i != c.end(); ++i)
        (*i)->drop_cached_data();

	// Read each other one
	utils::acct::gzip_data_read_count.reset();
	utils::acct::gzip_forward_seek_bytes.reset();
	utils::acct::gzip_idx_reposition_count.reset();
    for (int i = 0; i < repeats; i += 2)
    {
        const auto& b = c[i].getData();
        ensure_equals(b.size(), bufr.size());
        ensure(memcmp(b.data(), bufr.data(), bufr.size()) == 0);
    }
	ensure_equals(utils::acct::gzip_data_read_count.val(), (size_t)repeats / 2);
	ensure_equals(utils::acct::gzip_forward_seek_bytes.val(), 194u * 511u);
	ensure_equals(utils::acct::gzip_idx_reposition_count.val(), 1u);
#endif
}

// Test compression when the data don't compress
template<> template<>
void to::test<2>()
{
	using namespace utils;

#ifdef HAVE_DBALLE
	// Create a collector with only one small metadata inside
	c.clear();
	scan::scan("inbound/test.bufr", c);
	ensure_equals(c.size(), 3u);
	c.pop_back();
	c.pop_back();
	ensure_equals(c.size(), 1u);

	c.writeAtomically("test.md");

	metadata::Collection c1;
	Metadata::readFile("test.md", c1);
	ensure_equals(c.size(), 1u);
	ensure(c[0] ==  c1[0]);
#endif
}


}

// vim:set ts=4 sw=4:
