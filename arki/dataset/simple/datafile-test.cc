#include "arki/dataset/tests.h"
#include "arki/core/file.h"
#include "arki/dataset/simple/datafile.h"
#include "arki/utils/sys.h"
#include "arki/metadata.h"
#include "arki/types/source/blob.h"

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

    sys::unlink_ifexists(fname);
    ino_t inomd;
    ino_t inosum;

    system(("cp inbound/test.grib1 " + fname).c_str());

    metadata::TestCollection mds("inbound/test.grib1");

    {
        datafile::MdBuf mdbuf("./" + fname, std::shared_ptr<core::lock::Null>());

        // Append the data, source is unchanged
        auto source = types::source::Blob::create_unlocked("grib", "", "test.grib1", 0, datasize(mds[0]));
        wassert(mdbuf.add(mds[0], *source));
        wassert(actual_type(mds[0].source()).is_source_blob("grib", sys::getcwd(), "inbound/test.grib1", 0, datasize(mds[0])));

        // Metadata and summaries don't get touched
        wassert(actual_file(mdfname).not_exists());
        wassert(actual_file(sumfname).not_exists());

        // Append another metadata
        source = types::source::Blob::create_unlocked("grib", "", "test.grib1", source->offset + source->size, datasize(mds[1]));
        wassert(mdbuf.add(mds[1], *source));
        wassert(actual_type(mds[1].source()).is_source_blob("grib", sys::getcwd(), "inbound/test.grib1", source->offset, datasize(mds[1])));

        // Metadata and summaries don't get touched
        wassert(actual_file(mdfname).not_exists());
        wassert(actual_file(sumfname).not_exists());

        wassert(mdbuf.flush());

        // Metadata and summaries are now there
        wassert(actual_file(mdfname).exists());
        wassert(actual_file(sumfname).exists());
        inomd = sys::inode(mdfname);
        inosum = sys::inode(sumfname);

        // Append another metadata
        source = types::source::Blob::create_unlocked("grib", sys::getcwd(), "inbound/test.grib1", source->offset + source->size, datasize(mds[2]));
        wassert(mdbuf.add(mds[2], *source));
        wassert(actual_type(mds[2].source()).is_source_blob("grib", sys::getcwd(), "inbound/test.grib1", source->offset, datasize(mds[2])));

        // Metadata and summaries don't get touched
        wassert(actual(sys::inode(mdfname)) == inomd);
        wassert(actual(sys::inode(sumfname)) == inosum);
    }

    // After Datafile is destroyed, metadata and summaries are flushed
    wassert(actual(sys::inode(mdfname)) != inomd);
    wassert(actual(sys::inode(sumfname)) != inosum);
});

}

}
