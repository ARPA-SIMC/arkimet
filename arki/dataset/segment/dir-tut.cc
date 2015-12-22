#include "config.h"
#include "dir.h"
#include "arki/dataset/segment/tests.h"
#include "arki/metadata/tests.h"
#include "arki/metadata/collection.h"
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

namespace {

inline size_t datasize(const arki::Metadata& md)
{
    return md.data_size();
}

}

namespace tut {
using namespace std;
using namespace arki;
using namespace arki::dataset::segment;
using namespace arki::utils;
using namespace arki::tests;
using namespace arki::tests;

struct arki_data_dir_shar {
    string fname;
    metadata::Collection mdc;

    arki_data_dir_shar()
        : fname("testfile.grib")
    {
        system(("rm -rf " + fname).c_str());
        // Keep some valid metadata handy
        ensure(scan::scan("inbound/test.grib1", mdc.inserter_func()));
    }

    /**
     * Create a writer
     * return the data::Writer so that we manage the writer lifetime, but also
     * the underlying implementation so we can test it.
     */
    unique_ptr<dir::Segment> make_w(const std::string& relname)
    {
        string absname = sys::abspath(relname);
        return unique_ptr<dir::Segment>(new dir::Segment("grib", relname, absname));
    }
};

TESTGRP(arki_data_dir);

// Try to append some data
def_test(1)
{
    typedef types::source::Blob Blob;

    wassert(actual_file(fname).not_exists());
    {
        unique_ptr<dir::Segment> w(make_w(fname));

        // It should exist but be empty
        //wassert(actual(fname).fileexists());
        //wassert(actual(sys::fs::size(fname)) == 0u);

        // Try a successful transaction
        {
            Metadata& md = mdc[0];

            // Make a snapshot of everything before appending
            unique_ptr<types::Source> orig_source(md.source().clone());
            size_t data_size = md.data_size();

            // Start the append transaction, the file is written
            off_t ofs;
            Pending p = w->append(md, &ofs);
            wassert(actual((size_t)ofs) == 0);
            wassert(actual(sys::size(str::joinpath(w->absname, "000000.grib"))) == data_size);
            wassert(actual_type(md.source()) == *orig_source);

            // Commit
            p.commit();

            // After commit, metadata is updated
            wassert(actual_type(md.source()).is_source_blob("grib1", "", w->absname, 0, data_size));
        }

        // Then fail one
        {
            Metadata& md = mdc[1];

            // Make a snapshot of everything before appending
            unique_ptr<types::Source> orig_source(md.source().clone());
            size_t data_size = md.data_size();

            // Start the append transaction, the file is written
            off_t ofs;
            Pending p = w->append(md, &ofs);
            wassert(actual((size_t)ofs) == 1);
            wassert(actual(sys::size(str::joinpath(w->absname, "000001.grib"))) == data_size);
            wassert(actual_type(md.source()) == *orig_source);

            // Rollback
            p.rollback();

            // After rollback, the file has been deleted
            wassert(actual(sys::exists(str::joinpath(w->absname, "000001.grib"))).isfalse());
            wassert(actual_type(md.source()) == *orig_source);
        }

        // Then succeed again
        {
            Metadata& md = mdc[2];

            // Make a snapshot of everything before appending
            unique_ptr<types::Source> orig_source(md.source().clone());
            size_t data_size = md.data_size();

            // Start the append transaction, the file is written
            // Rolling back a transaction does leave a gap in the sequence
            off_t ofs;
            Pending p = w->append(md, &ofs);
            wassert(actual((size_t)ofs) == 2);
            wassert(actual(sys::size(str::joinpath(w->absname, "000002.grib"))) == data_size);
            wassert(actual_type(md.source()) == *orig_source);

            // Commit
            p.commit();

            // After commit, metadata is updated
            wassert(actual_type(md.source()).is_source_blob("grib1", "", w->absname, 2, data_size));
        }
    }

    // Data writer goes out of scope, file is closed and flushed
    metadata::Collection mdc1;

    // Scan the file we created
    wassert(actual(scan::scan(fname, mdc1.inserter_func())).istrue());

    // Check that it only contains the 1st and 3rd data
    wassert(actual(mdc1.size()) == 2u);
    wassert(actual(mdc1[0]).is_similar(mdc[0]));
    wassert(actual(mdc1[1]).is_similar(mdc[2]));
}

// Common segment tests

def_test(2)
{
    struct Test : public SegmentCheckTest
    {
        dataset::segment::Segment* make_segment() override
        {
            return new dir::Segment(format, relname, absname);
        }
    } test;

    wruntest(test.run);
}
def_test(3)
{
    struct Test : public SegmentRemoveTest
    {
        dataset::segment::Segment* make_segment() override
        {
            return new dir::Segment(format, relname, absname);
        }
    } test;

    wruntest(test.run);
}

}
