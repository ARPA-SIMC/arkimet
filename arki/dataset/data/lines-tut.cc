#include "lines.h"
#include "arki/dataset/data/tests.h"
#include "arki/metadata/tests.h"
#include "arki/metadata/collection.h"
#include "arki/types/source/blob.h"
#include "arki/scan/any.h"
#include "arki/utils/files.h"
#include "arki/utils/sys.h"
#include <sstream>

namespace std {
static inline std::ostream& operator<<(std::ostream& o, const arki::Metadata& m)
{
    m.writeYaml(o);
    return o;
}
}

namespace tut {
using namespace std;
using namespace arki;
using namespace arki::dataset::data;
using namespace arki::utils;
using namespace arki::tests;
using namespace arki::tests;

struct arki_data_lines_shar {
    string fname;
    metadata::Collection mdc;

    arki_data_lines_shar()
        : fname("testfile.grib")
    {
        system(("rm -f " + fname).c_str());
        // Keep some valid metadata handy
        ensure(scan::scan("inbound/test.grib1", mdc));
    }

    /**
     * Create a writer
     * return the data::Segment so that we manage the writer lifetime, but also
     * the underlying implementation so we can test it.
     */
    unique_ptr<lines::Segment> make_w(const std::string& relname)
    {
        string absname = sys::abspath(relname);
        return unique_ptr<lines::Segment>(new lines::Segment(relname, absname));
    }
};

TESTGRP(arki_data_lines);

namespace {

inline size_t datasize(const Metadata& md)
{
    return md.data_size();
}

}

// Try to append some data
template<> template<>
void to::test<1>()
{
    wassert(actual_file(fname).not_exists());
    {
        unique_ptr<lines::Segment> dw(make_w(fname));

        // It should exist but be empty
        //wassert(actual(fname).fileexists());
        //wassert(actual(sys::size(fname)) == 0u);

        // Try a successful transaction
        wruntest(test_append_transaction_ok, dw.get(), mdc[0], 1);

        // Then fail one
        wruntest(test_append_transaction_rollback, dw.get(), mdc[1]);

        // Then succeed again
        wruntest(test_append_transaction_ok, dw.get(), mdc[2], 1);
    }

    // Data writer goes out of scope, file is closed and flushed
    metadata::Collection mdc1;

    // Scan the file we created
    wassert(actual(scan::scan(fname, mdc1)).istrue());

    // Check that it only contains the 1st and 3rd data
    wassert(actual(mdc1.size()) == 2u);
    wassert(actual(mdc1[0]).is_similar(mdc[0]));
    wassert(actual(mdc1[1]).is_similar(mdc[2]));
}

// Test with large files
template<> template<>
void to::test<2>()
{
    {
        unique_ptr<lines::Segment> dw(make_w(fname));

        // Make a file that looks HUGE, so that appending will make its size
        // not fit in a 32bit off_t
        dw->truncate(0x7FFFFFFF);
        wassert(actual(sys::size(fname)) == 0x7FFFFFFFu);

        // Try a successful transaction
        wruntest(test_append_transaction_ok, dw.get(), mdc[0], 1);

        // Then fail one
        wruntest(test_append_transaction_rollback, dw.get(), mdc[1]);

        // Then succeed again
        wruntest(test_append_transaction_ok, dw.get(), mdc[2], 1);
    }

    wassert(actual(sys::size(fname)) == 0x7FFFFFFFu + datasize(mdc[0]) + datasize(mdc[2]) + 2);

    // Won't attempt rescanning, as the grib reading library will have to
    // process gigabytes of zeros
}

// Test stream writer
template<> template<>
void to::test<3>()
{
    std::stringstream out;
    const OstreamWriter* w = OstreamWriter::get("vm2");
    w->stream(mdc[0], out);
    wassert(actual(out.str().size()) == datasize(mdc[0]) + 1);
}

// Test raw append
template<> template<>
void to::test<4>()
{
    wassert(actual_file(fname).not_exists());
    {
        unique_ptr<lines::Segment> dw(make_w(fname));
        vector<uint8_t> buf = { 'c', 'i', 'a', 'o' };
        ensure_equals(dw->append(buf), 0);
        ensure_equals(dw->append(buf), 5);
    }

    wassert(actual(utils::files::read_file(fname)) == "ciao\nciao\n");
}

// Common segment tests

template<> template<>
void to::test<5>()
{
    struct Test : public SegmentCheckTest
    {
        lines::Segment* make_segment() override
        {
            return new lines::Segment(relname, absname);
        }
    } test;

    wruntest(test.run);
}
template<> template<>
void to::test<6>()
{
    struct Test : public SegmentRemoveTest
    {
        lines::Segment* make_segment() override
        {
            return new lines::Segment(relname, absname);
        }
    } test;

    wruntest(test.run);
}

}

