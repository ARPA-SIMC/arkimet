/*
 * Copyright (C) 2007--2013  ARPA-SIM <urpsim@smr.arpa.emr.it>
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

#include "config.h"

#include "concat.h"
#include "arki/tests/tests.h"
#include "arki/metadata/tests.h"
#include "arki/metadata/collection.h"
#include "arki/scan/any.h"
#include "arki/utils/files.h"
#include <wibble/sys/fs.h>
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
using namespace wibble;
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
    auto_ptr<concat::Writer> make_w(const std::string& relname)
    {
        string absname = sys::fs::abspath(relname);
        return auto_ptr<concat::Writer>(new concat::Writer(relname, absname));
    }
};

TESTGRP(arki_data_concat);

namespace {

inline size_t datasize(const Metadata& md)
{
    return md.source.upcast<types::source::Blob>()->size;
}

void test_append_transaction_ok(WIBBLE_TEST_LOCPRM, Writer* dw, Metadata& md)
{
    typedef types::source::Blob Blob;

    // Make a snapshot of everything before appending
    Item<types::Source> orig_source = md.source;
    size_t data_size = datasize(md);
    size_t orig_fsize = sys::fs::size(dw->absname, 0);

    // Start the append transaction, nothing happens until commit
    off_t ofs;
    Pending p = dw->append(md, &ofs);
    wassert(actual((size_t)ofs) == orig_fsize);
    wassert(actual(sys::fs::size(dw->absname)) == orig_fsize);
    wassert(actual(md.source) == orig_source);

    // Commit
    p.commit();

    // After commit, data is appended
    wassert(actual(sys::fs::size(dw->absname)) == orig_fsize + data_size);

    // And metadata is updated
    UItem<Blob> s = md.source.upcast<Blob>();
    wassert(actual(s->format) == "grib1");
    wassert(actual(s->offset) == orig_fsize);
    wassert(actual(s->size) == data_size);
    wassert(actual(s->filename) == dw->absname);
}

void test_append_transaction_rollback(WIBBLE_TEST_LOCPRM, Writer* dw, Metadata& md)
{
    // Make a snapshot of everything before appending
    Item<types::Source> orig_source = md.source;
    size_t orig_fsize = sys::fs::size(dw->absname, 0);

    // Start the append transaction, nothing happens until commit
    off_t ofs;
    Pending p = dw->append(md, &ofs);
    wassert(actual((size_t)ofs) == orig_fsize);
    wassert(actual(sys::fs::size(dw->absname, 0)) == orig_fsize);
    wassert(actual(md.source) == orig_source);

    // Rollback
    p.rollback();

    // After rollback, nothing has changed
    wassert(actual(sys::fs::size(dw->absname, 0)) == orig_fsize);
    wassert(actual(md.source) == orig_source);
}

}

// Try to append some data
template<> template<>
void to::test<1>()
{
    wassert(!actual(fname).fileexists());
    {
        auto_ptr<concat::Writer> w(make_w(fname));

        // It should exist but be empty
        wassert(actual(fname).fileexists());
        wassert(actual(sys::fs::size(fname)) == 0u);

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
        auto_ptr<concat::Writer> dw(make_w(fname));

        // Make a file that looks HUGE, so that appending will make its size
        // not fit in a 32bit off_t
        dw->truncate(0x7FFFFFFF);
        wassert(actual(sys::fs::size(fname)) == 0x7FFFFFFFu);

        // Try a successful transaction
        wruntest(test_append_transaction_ok, dw.get(), mdc[0]);

        // Then fail one
        wruntest(test_append_transaction_rollback, dw.get(), mdc[1]);

        // Then succeed again
        wruntest(test_append_transaction_ok, dw.get(), mdc[2]);
    }

    wassert(actual(sys::fs::size(fname)) == 0x7FFFFFFFu + datasize(mdc[0]) + datasize(mdc[2]));

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
        auto_ptr<concat::Writer> dw(make_w(fname));
        sys::Buffer buf("ciao", 4);
        wassert(actual(dw->append(buf)) == 0);
        wassert(actual(dw->append(buf)) == 4);
    }

    wassert(actual(utils::files::read_file(fname)) == "ciaociao");
}

// Test Info
template<> template<>
void to::test<5>()
{
    const Info* i = Info::get("grib");
    off_t a = 10;
    size_t b = 20;
    i->raw_to_wrapped(a, b);
    wassert(actual(a) == 10);
    wassert(actual(b) == 20);
}

}

