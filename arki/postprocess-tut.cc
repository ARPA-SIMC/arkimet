/*
 * Copyright (C) 2008--2015  ARPA-SIM <urpsim@smr.arpa.emr.it>
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

#include <arki/matcher/tests.h>
#include <arki/postprocess.h>
#include <arki/configfile.h>
#include <arki/metadata.h>
#include <arki/scan/grib.h>
#include <arki/utils/files.h>
#include <arki/utils/fd.h>
#include <wibble/sys/fs.h>

#include <sstream>
#include <iostream>
#include <cstdio>

#include <sys/types.h>
#include <sys/stat.h>
#include <fcntl.h>

#ifndef STDERR_FILENO
#define STDERR_FILENO 2
#endif

using namespace wibble;
using namespace wibble::tests;

namespace tut {
using namespace std;
using namespace arki;

struct arki_postprocess_shar {
    ConfigFile config;

    arki_postprocess_shar()
    {
        // In-memory dataset configuration
        string conf =
            "[testall]\n"
            "type = test\n"
            "step = daily\n"
            "filter = origin: GRIB1\n"
            "index = origin, reftime\n"
            "postprocess = null\n"
            "name = testall\n"
            "path = testall\n";
        stringstream incfg(conf);
        config.parse(incfg, "(memory)");
    }

    void produceGRIB(metadata::Consumer& c)
    {
        Metadata md;
        scan::Grib scanner;
        scanner.open("inbound/test.grib1");
        while (scanner.next(md))
            c(md);
    }
};
TESTGRP(arki_postprocess);

// See if the postprocess makes a difference
template<> template<>
void to::test<1>()
{
    Postprocess p("null");
    p.set_output(STDERR_FILENO);
    p.validate(config.section("testall")->values());
    p.start();

    produceGRIB(p);

    p.flush();
}

// Check that it works without validation, too
template<> template<>
void to::test<2>()
{
    Postprocess p("null");
    p.set_output(STDERR_FILENO);
    p.start();

    produceGRIB(p);

    p.flush();
}

// Test actually sending some data
template<> template<>
void to::test<3>()
{
    stringstream str;
    Postprocess p("countbytes");
    p.set_output(str);
    p.start();

    produceGRIB(p);
    p.flush();

    ensure_equals(str.str(), "45027\n");
}

// Test actually sending some data
template<> template<>
void to::test<4>()
{
    // Get the normal data
    string plain;
    {
        struct Writer : public metadata::Consumer
        {
            string& out;
            Writer(string& out) : out(out) {}
            bool operator()(Metadata& md)
            {
                out += md.encodeBinary();
                wibble::sys::Buffer data = md.getData();
                out.append((const char*)data.data(), data.size());
                return true;
            }
        } writer(plain);

        Metadata md;
        scan::Grib scanner;
        scanner.open("inbound/test.grib1");
        while (scanner.next(md))
            writer(md);
    }

    // Get the postprocessed data
    stringstream postprocessed;
    Postprocess p("cat");
    p.set_output(postprocessed);
    p.start();

    {
        Metadata md;
        scan::Grib scanner;
        scanner.open("inbound/test.grib1");
        while (scanner.next(md))
            p(md);
        p.flush();
    }

    ensure(plain == postprocessed.str());
}

// Try to shift a sizeable chunk of data to the postprocessor
template<> template<>
void to::test<5>()
{
    stringstream str;
    Postprocess p("countbytes");
    p.set_output(str);
    p.start();

    for (unsigned i = 0; i < 128; ++i)
        produceGRIB(p);
    p.flush();

    ensure_equals(str.str(), "5763456\n");
}

// Try to shift a sizeable chunk of data out of the postprocessor
template<> template<>
void to::test<6>()
{
    const char* fname = "postprocess_output";
    stringstream str;
    Postprocess p("zeroes 4096");

    int fd = ::open(fname, O_WRONLY | O_CREAT | O_NOCTTY, 0666);
    if (fd == -1)
        throw wibble::exception::File(fname, "opening/creating file");
    utils::fd::TempfileHandleWatch hwfd(fname, fd);

    p.set_output(fd);
    p.start();

    produceGRIB(p);
    p.flush();

    wassert(actual(sys::fs::size(fname)) == 4096*1024);
}

// Try to shift a sizeable chunk of data to and out of the postprocessor
template<> template<>
void to::test<7>()
{
    const char* fname = "postprocess_output";
    stringstream str;
    Postprocess p("zeroes 4096");

    int fd = ::open(fname, O_WRONLY | O_CREAT | O_NOCTTY, 0666);
    if (fd == -1)
        throw wibble::exception::File(fname, "opening/creating file");
    utils::fd::TempfileHandleWatch hwfd(fname, fd);

    p.set_output(fd);
    p.start();

    for (unsigned i = 0; i < 128; ++i)
        produceGRIB(p);
    p.flush();

    wassert(actual(sys::fs::size(fname)) == 4096*1024);
}


}

// vim:set ts=4 sw=4:
