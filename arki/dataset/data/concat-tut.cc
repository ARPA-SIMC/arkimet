#include "concat.h"
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
using namespace arki::types;
using namespace arki::tests;
using namespace wibble::tests;

struct arki_data_concat_shar {
    string fname;
    metadata::Collection mdc;

    arki_data_concat_shar()
        : fname("testfile.grib")
    {
        system(("rm -f " + fname).c_str());
        // Keep some valid metadata handy
        ensure(scan::scan("inbound/test.grib1", mdc));
    }

    /**
     * Create a writer
     * return the data::Writer so that we manage the writer lifetime, but also
     * the underlying implementation so we can test it.
     */
    unique_ptr<concat::Segment> make_w(const std::string& relname)
    {
        string absname = sys::abspath(relname);
        return unique_ptr<concat::Segment>(new concat::Segment(relname, absname));
    }
};

TESTGRP(arki_data_concat);

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
    wassert(!actual(fname).fileexists());
    {
        unique_ptr<concat::Segment> w(make_w(fname));

        // It should exist but be empty
        //wassert(actual(fname).fileexists());
        //wassert(actual(sys::size(fname)) == 0u);

        // Try a successful transaction
        wruntest(test_append_transaction_ok, w.get(), mdc[0]);

        // Then fail one
        wruntest(test_append_transaction_rollback, w.get(), mdc[1]);

        // Then succeed again
        wruntest(test_append_transaction_ok, w.get(), mdc[2]);
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
        unique_ptr<concat::Segment> dw(make_w(fname));

        // Make a file that looks HUGE, so that appending will make its size
        // not fit in a 32bit off_t
        dw->truncate(0x7FFFFFFF);
        wassert(actual(sys::size(fname)) == 0x7FFFFFFFu);

        // Try a successful transaction
        wruntest(test_append_transaction_ok, dw.get(), mdc[0]);

        // Then fail one
        wruntest(test_append_transaction_rollback, dw.get(), mdc[1]);

        // Then succeed again
        wruntest(test_append_transaction_ok, dw.get(), mdc[2]);
    }

    wassert(actual(sys::size(fname)) == 0x7FFFFFFFu + datasize(mdc[0]) + datasize(mdc[2]));

    // Won't attempt rescanning, as the grib reading library will have to
    // process gigabytes of zeros
}

// Test stream writer
template<> template<>
void to::test<3>()
{
    std::stringstream out;
    const OstreamWriter* w = OstreamWriter::get("grib");
    w->stream(mdc[0], out);
    wassert(actual(out.str().size()) == datasize(mdc[0]));
}

// Test raw append
template<> template<>
void to::test<4>()
{
    wassert(!actual(fname).fileexists());
    {
        unique_ptr<concat::Segment> dw(make_w(fname));
        vector<uint8_t> buf = { 'c', 'i', 'a', 'o' };
        wassert(actual(dw->append(buf)) == 0);
        wassert(actual(dw->append(buf)) == 4);
    }

    wassert(actual(utils::files::read_file(fname)) == "ciaociao");
}

// Common segment tests

template<> template<>
void to::test<5>()
{
    struct Test : public SegmentCheckTest
    {
        concat::Segment* make_segment() override
        {
            return new concat::Segment(relname, absname);
        }
    } test;

    wruntest(test.run);
}
template<> template<>
void to::test<6>()
{
    struct Test : public SegmentRemoveTest
    {
        concat::Segment* make_segment() override
        {
            return new concat::Segment(relname, absname);
        }
    } test;

    wruntest(test.run);
}

}
