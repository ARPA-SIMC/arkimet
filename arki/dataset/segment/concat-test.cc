#include "concat.h"
#include "arki/dataset/segment/tests.h"
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
    m.writeYaml(o);
    return o;
}
}

namespace {
using namespace std;
using namespace arki;
using namespace arki::tests;
using namespace arki::utils;
using namespace arki::dataset;

const char* relname = "testfile.grib";

class Tests : public TestCase
{
    using TestCase::TestCase;
    void register_tests() override;
} test("arki_dataset_segment_concat");

inline size_t datasize(const Metadata& md)
{
    return md.data_size();
}

/**
 * Create a writer
 * return the data::Writer so that we manage the writer lifetime, but also
 * the underlying implementation so we can test it.
 */
std::shared_ptr<segment::concat::Writer> make_w()
{
    string absname = sys::abspath(relname);
    return std::shared_ptr<segment::concat::Writer>(new segment::concat::Writer(sys::getcwd(), relname, absname, core::lock::policy_ofd));
}
std::shared_ptr<segment::concat::Checker> make_c()
{
    string absname = sys::abspath(relname);
    return std::shared_ptr<segment::concat::Checker>(new segment::concat::Checker(sys::getcwd(), relname, absname, core::lock::policy_ofd));
}

void Tests::register_tests() {

// Try to append some data
add_method("append", [] {
    sys::unlink_ifexists(relname);
    metadata::Collection mdc("inbound/test.grib1");
    wassert(actual_file(relname).not_exists());
    {
        auto w(make_w());

        // It should exist but be empty
        //wassert(actual(fname).fileexists());
        //wassert(actual(sys::size(fname)) == 0u);

        // Try a successful transaction
        wassert(test_append_transaction_ok(w.get(), mdc[0]));

        // Then fail one
        wassert(test_append_transaction_rollback(w.get(), mdc[1]));

        // Then succeed again
        wassert(test_append_transaction_ok(w.get(), mdc[2]));
    }

    // Data writer goes out of scope, file is closed and flushed
    metadata::Collection mdc1;

    // Scan the file we created
    wassert(actual(scan::scan(relname, mdc1.inserter_func())).istrue());

    // Check that it only contains the 1st and 3rd data
    wassert(actual(mdc1.size()) == 2u);
    wassert(actual(mdc1[0]).is_similar(mdc[0]));
    wassert(actual(mdc1[1]).is_similar(mdc[2]));
});

// Test with large files
add_method("large", [] {
    sys::unlink_ifexists(relname);
    metadata::Collection mdc("inbound/test.grib1");
    {
        // Make a file that looks HUGE, so that appending will make its size
        // not fit in a 32bit off_t
        make_c()->test_truncate(0x7FFFFFFF);
        wassert(actual(sys::size(relname)) == 0x7FFFFFFFu);
    }

    {
        auto dw(make_w());

        // Try a successful transaction
        wassert(test_append_transaction_ok(dw.get(), mdc[0]));

        // Then fail one
        wassert(test_append_transaction_rollback(dw.get(), mdc[1]));

        // Then succeed again
        wassert(test_append_transaction_ok(dw.get(), mdc[2]));
    }

    wassert(actual(sys::size(relname)) == 0x7FFFFFFFu + datasize(mdc[0]) + datasize(mdc[2]));

    // Won't attempt rescanning, as the grib reading library will have to
    // process gigabytes of zeros
});

// Common segment tests

add_method("check", [] {
    struct Test : public SegmentCheckTest
    {
        std::shared_ptr<segment::Writer> make_writer() override
        {
            return std::shared_ptr<segment::Writer>(new segment::concat::Writer(root, relname, absname, core::lock::policy_ofd));
        }
        std::shared_ptr<segment::Checker> make_checker() override
        {
            return std::shared_ptr<segment::Checker>(new segment::concat::Checker(root, relname, absname, core::lock::policy_ofd));
        }
    } test;

    wassert(test.run());
});

add_method("remove", [] {
    struct Test : public SegmentRemoveTest
    {
        std::shared_ptr<segment::Writer> make_writer() override
        {
            return std::shared_ptr<segment::Writer>(new segment::concat::Writer(root, relname, absname, core::lock::policy_ofd));
        }
        std::shared_ptr<segment::Checker> make_checker() override
        {
            return std::shared_ptr<segment::Checker>(new segment::concat::Checker(root, relname, absname, core::lock::policy_ofd));
        }
    } test;

    wassert(test.run());
});

}

}
