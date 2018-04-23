#include "lines.h"
#include "tests.h"
#include "arki/metadata/tests.h"
#include "arki/metadata/collection.h"
#include "arki/types/source/blob.h"
#include "arki/scan/any.h"
#include "arki/utils/files.h"
#include "arki/utils/sys.h"
#include <fcntl.h>
#include <sstream>

namespace std {
static inline std::ostream& operator<<(std::ostream& o, const arki::Metadata& m)
{
    m.write_yaml(o);
    return o;
}
}

namespace {
using namespace std;
using namespace arki;
using namespace arki::tests;
using namespace arki::utils;

const char* relpath = "testfile.grib";

template<class Segment, class Data>
class Tests : public SegmentTests<Segment, Data>
{
    using SegmentTests<Segment, Data>::SegmentTests;
    void register_tests() override;
};

Tests<segment::lines::Checker, VM2Data> test1("arki_segment_lines_vm2");

inline size_t datasize(const Metadata& md)
{
    return md.data_size();
}
/**
 * Create a writer
 * return the data::Writer so that we manage the writer lifetime, but also
 * the underlying implementation so we can test it.
 */
std::shared_ptr<segment::lines::Writer> make_w()
{
    string abspath = sys::abspath(relpath);
    return std::shared_ptr<segment::lines::Writer>(new segment::lines::Writer(sys::getcwd(), relpath, abspath));
}
std::shared_ptr<segment::lines::Checker> make_c()
{
    string abspath = sys::abspath(relpath);
    return std::shared_ptr<segment::lines::Checker>(new segment::lines::Checker(sys::getcwd(), relpath, abspath));
}

template<class Segment, class Data>
void Tests<Segment, Data>::register_tests() {
SegmentTests<Segment, Data>::register_tests();

// Try to append some data
this->add_method("append", [](Fixture& f) {
    sys::unlink_ifexists(relpath);
    metadata::TestCollection mdc("inbound/test.grib1");
    wassert(actual_file(relpath).not_exists());
    {
        auto w(make_w());

        // It should exist but be empty
        //wassert(actual(fname).fileexists());
        //wassert(actual(sys::size(fname)) == 0u);

        // Try a successful transaction
        wassert(test_append_transaction_ok(w.get(), mdc[0], 1));

        // Then fail one
        wassert(test_append_transaction_rollback(w.get(), mdc[1], 1));

        // Then succeed again
        wassert(test_append_transaction_ok(w.get(), mdc[2], 1));
    }

    // Data writer goes out of scope, file is closed and flushed
    metadata::TestCollection mdc1;

    // Scan the file we created
    wassert(mdc1.scan_from_file(relpath, false));

    // Check that it only contains the 1st and 3rd data
    wassert(actual(mdc1.size()) == 2u);
    wassert(actual(mdc1[0]).is_similar(mdc[0]));
    wassert(actual(mdc1[1]).is_similar(mdc[2]));
});

// Test with large files
this->add_method("large", [](Fixture& f) {
    sys::unlink_ifexists(relpath);
    metadata::TestCollection mdc("inbound/test.grib1");
    {
        // Make a file that looks HUGE, so that appending will make its size
        // not fit in a 32bit off_t
        make_c()->test_truncate(0x7FFFFFFF);
        wassert(actual(sys::size(relpath)) == 0x7FFFFFFFu);
    }

    {
        auto dw(make_w());

        // Try a successful transaction
        wassert(test_append_transaction_ok(dw.get(), mdc[0], 1));

        // Then fail one
        wassert(test_append_transaction_rollback(dw.get(), mdc[1], 1));

        // Then succeed again
        wassert(test_append_transaction_ok(dw.get(), mdc[2], 1));
    }

    wassert(actual(sys::size(relpath)) == 0x7FFFFFFFu + datasize(mdc[0]) + datasize(mdc[2]) + 2);

    // Won't attempt rescanning, as the grib reading library will have to
    // process gigabytes of zeros
});

}

}
#include "tests.tcc"
