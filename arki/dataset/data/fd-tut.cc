#include "config.h"
#include "fd.h"
#include "arki/metadata/tests.h"
#include "arki/metadata/collection.h"
#include "arki/scan/any.h"
#include "arki/utils/files.h"

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

struct arki_data_fd_shar {
#if 0
    string fname;
    metadata::Collection mdc;

    arki_data_fd_shar()
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
    data::Writer make_w(const std::string& fname, fd::Writer*& w)
    {
        data::Writer res = data::Writer::get("grib", fname);

        // Access and upcast the underlying implementation
        w = dynamic_cast<fd::Writer*>(res._implementation());

        if (!w)
            throw wibble::exception::Consistency("creating test writer", "the writer we got in not a fd::Writer");

        return res;
    }
#endif
};

TESTGRP(arki_data_fd);

#if 0
namespace {

inline size_t datasize(const Metadata& md)
{
    return md.source.upcast<types::source::Blob>()->size;
}

void test_append_transaction_ok(LOCPRM, data::Writer dw, Metadata& md)
{
    typedef types::source::Blob Blob;

    // Make a snapshot of everything before appending
    Item<types::Source> orig_source = md.source;
    size_t data_size = datasize(md);
    size_t orig_fsize = files::size(dw.fname());

    // Start the append transaction, nothing happens until commit
    off_t ofs;
    Pending p = dw.append(md, &ofs);
    wtest(equals, orig_fsize, (size_t)ofs);
    wtest(equals, orig_fsize, files::size(dw.fname()));
    wtest(equals, orig_source, md.source);

    // Commit
    p.commit();

    // After commit, data is appended
    wtest(equals, orig_fsize + data_size, files::size(dw.fname()));

    // And metadata is updated
    UItem<Blob> s = md.source.upcast<Blob>();
    wtest(equals, "grib1", s->format);
    wtest(equals, orig_fsize, s->offset);
    wtest(equals, data_size, s->size);
    wtest(equals, dw.fname(), s->filename);
}

void test_append_transaction_rollback(LOCPRM, data::Writer dw, Metadata& md)
{
    typedef types::source::Blob Blob;

    // Make a snapshot of everything before appending
    Item<types::Source> orig_source = md.source;
    size_t orig_fsize = files::size(dw.fname());

    // Start the append transaction, nothing happens until commit
    off_t ofs;
    Pending p = dw.append(md, &ofs);
    wtest(equals, orig_fsize, (size_t)ofs);
    wtest(equals, orig_fsize, files::size(dw.fname()));
    wtest(equals, orig_source, md.source);

    // Rollback
    p.rollback();

    // After rollback, nothing has changed
    wtest(equals, orig_fsize, files::size(dw.fname()));
    wtest(equals, orig_source, md.source);
}

}
#endif

// Try to append some data
def_test(1)
{
#if 0
    wtest(not_file_exists, fname);
    {
        fd::Writer* w;
        data::Writer dw = make_w(fname, w);

        // It should exist but be empty
        wtest(file_exists, fname);
        wtest(equals, 0u, files::size(fname));

        // Try a successful transaction
        wruntest(test_append_transaction_ok, dw, mdc[0]);

        // Then fail one
        wruntest(test_append_transaction_rollback, dw, mdc[1]);

        // Then succeed again
        wruntest(test_append_transaction_ok, dw, mdc[2]);
    }

    // Data writer goes out of scope, file is closed and flushed
    metadata::Collection mdc1;

    // Scan the file we created
    wtest(istrue, scan::scan(fname, mdc1));

    // Check that it only contains the 1st and 3rd data
    wtest(equals, 2u, mdc1.size());
    wtest(md_similar, mdc[0], mdc1[0]);
    wtest(md_similar, mdc[2], mdc1[1]);
#endif
}

// Test with large files
def_test(2)
{
#if 0
    {
        fd::Writer* w;
        data::Writer dw = make_w(fname, w);

        // Make a file that looks HUGE, so that appending will make its size
        // not fit in a 32bit off_t
        w->truncate(0x7FFFFFFF);
        wtest(equals, 0x7FFFFFFFu, files::size(fname));

        // Try a successful transaction
        wruntest(test_append_transaction_ok, dw, mdc[0]);

        // Then fail one
        wruntest(test_append_transaction_rollback, dw, mdc[1]);

        // Then succeed again
        wruntest(test_append_transaction_ok, dw, mdc[2]);
    }

    wtest(equals, 0x7FFFFFFFu + datasize(mdc[0]) + datasize(mdc[2]), files::size(fname));

    // Won't attempt rescanning, as the grib reading library will have to
    // process gigabytes of zeros
#endif
}

}

