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

#include <arki/tests/tests.h>
#include "arki/metadata.h"
#include "xargs.h"
#include "collection.h"
#include <arki/scan/any.h>
#include <arki/utils/files.h>
#include <wibble/sys/fs.h>

namespace tut {
using namespace std;
using namespace wibble;
using namespace wibble::tests;
using namespace arki;

struct arki_metadata_xargs_shar {
    metadata::Collection mdc;

    arki_metadata_xargs_shar()
    {
        scan::scan("inbound/test.grib1", mdc);
    }
};
TESTGRP(arki_metadata_xargs);

namespace {
inline auto_ptr<Metadata> wrap(const Metadata& md)
{
    return auto_ptr<Metadata>(new Metadata(md));
}
}

// Test what happens with children's stdin
template<> template<>
void to::test<1>()
{
    sys::fs::deleteIfExists("tmp-xargs");
    metadata::Xargs xargs;
    xargs.command.push_back("/bin/sh");
    xargs.command.push_back("-c");
    xargs.command.push_back("wc -c >> tmp-xargs");
    xargs.filename_argument = 1000; // Do not pass the file name

    xargs.max_count = 10;
    for (unsigned x = 0; x < 10; ++x)
        xargs.eat(wrap(mdc[0]));
    xargs.flush();

    string out = utils::files::read_file("tmp-xargs");
    // Nothing should be send to the child's stdin
    wassert(actual(out) == "0\n");
}

// Test that env vars are set
template<> template<>
void to::test<2>()
{
    sys::fs::deleteIfExists("tmp-xargs");
    metadata::Xargs xargs;
    xargs.command.push_back("/bin/sh");
    xargs.command.push_back("-c");
    xargs.command.push_back("set | grep ARKI_XARGS_ >> tmp-xargs");
    xargs.filename_argument = 1000; // Do not pass the file name

    xargs.max_count = 10;
    for (unsigned x = 0; x < 10; ++x)
        xargs.eat(wrap(mdc[0]));
    xargs.flush();

    string out = utils::files::read_file("tmp-xargs");
    wassert(actual(out).contains("ARKI_XARGS_FILENAME="));
    wassert(actual(out).matches("ARKI_XARGS_FORMAT=['\"]?GRIB1['\"]?\n"));
    wassert(actual(out).matches("ARKI_XARGS_COUNT=['\"]?10['\"]?\n"));
    wassert(actual(out).contains("ARKI_XARGS_TIME_START='2007-07-08 13:00:00Z'\n"));
    wassert(actual(out).contains("ARKI_XARGS_TIME_END='2007-07-08 13:00:00Z'\n"));
}

}

// vim:set ts=4 sw=4:
