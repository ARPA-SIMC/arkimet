#include <arki/dataset/tests.h>
#include <arki/dataset/simple/datafile.h>
#include <arki/utils/files.h>
#include <arki/utils/sys.h>
#include <arki/metadata.h>
#include <arki/types/source/blob.h>
#include <arki/scan/grib.h>
#include <arki/wibble/sys/process.h>
#include <sstream>
#include <iostream>
#include <sys/stat.h>

namespace {
using namespace std;
using namespace arki;
using namespace arki::utils;
using namespace arki::types;
using namespace arki::dataset::simple;
using namespace arki::tests;
using arki::core::Time;

inline size_t datasize(const Metadata& md)
{
    return md.data_size();
}

class Tests : public TestCase
{
    using TestCase::TestCase;
    void register_tests() override;
} test("arki_dataset_simple_datafile");

void Tests::register_tests() {

// Try to append some data
add_method("append", [] {
    string fname = "testfile.grib";
    string mdfname = "testfile.grib.metadata";
    string sumfname = "testfile.grib.summary";

    system(("rm -rf " + fname).c_str());
    ino_t inomd;
    ino_t inosum;

    system(("cp inbound/test.grib1 " + fname).c_str());

    scan::Grib scanner;

    Metadata md;
    size_t totsize = 0;
    scanner.test_open("inbound/test.grib1");

    {
        datafile::MdBuf mdbuf("./" + fname);

        // Get a metadata
        ensure(scanner.next(md));
        size_t size = datasize(md);

        // Append the data
        wassert(mdbuf.add(md));

        // The new data is there
        wassert(actual_type(md.source()).is_source_blob("grib", wibble::sys::process::getcwd(), "inbound/test.grib1", 0, size));

        // Metadata and summaries don't get touched
        ensure(!sys::exists(mdfname));
        ensure(!sys::exists(sumfname));

        totsize += size;

        // Get another metadata
        ensure(scanner.next(md));
        size = datasize(md);

        mdbuf.add(md);

        // The new data is there
        wassert(actual_type(md.source()).is_source_blob("grib", wibble::sys::process::getcwd(), "inbound/test.grib1", totsize, size));

        // Metadata and summaries don't get touched
        ensure(!sys::exists(mdfname));
        ensure(!sys::exists(sumfname));

        totsize += size;

        mdbuf.flush();
        // Metadata and summaries are now there
        ensure(sys::exists(mdfname));
        ensure(sys::exists(sumfname));
        inomd = sys::inode(mdfname);
        inosum = sys::inode(sumfname);

		// Get another metadata
		ensure(scanner.next(md));
		size = datasize(md);

		mdbuf.add(md);

        // The new data is there
        wassert(actual_type(md.source()).is_source_blob("grib", wibble::sys::process::getcwd(), "inbound/test.grib1", totsize, size));

        // Metadata and summaries don't get touched
        ensure_equals(sys::inode(mdfname), inomd);
        ensure_equals(sys::inode(sumfname), inosum);
    }

    // After Datafile is destroyed, metadata and summaries are flushed
    wassert(actual(sys::inode(mdfname)) != inomd);
    wassert(actual(sys::inode(sumfname)) != inosum);
});

}

}
