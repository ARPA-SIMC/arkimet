/*
 * Copyright (C) 2008--2011  ARPA-SIM <urpsim@smr.arpa.emr.it>
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

#include <arki/matcher/test-utils.h>
#include <arki/postprocess.h>
#include <arki/configfile.h>
#include <arki/metadata.h>
#include <arki/scan/grib.h>

#include <sstream>
#include <iostream>
#include <stdio.h>

#ifndef STDERR_FILENO
#define STDERR_FILENO 2
#endif

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

    ensure_equals(str.str(), "45063\n");
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
                out += md.encode();
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

}

// vim:set ts=4 sw=4:
