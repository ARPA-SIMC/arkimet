#include "matcher/tests.h"
#include "postprocess.h"
#include "configfile.h"
#include "metadata.h"
#include "scan/any.h"
#include "utils/files.h"
#include "utils/sys.h"
#include "binary.h"
#include <sstream>
#include <iostream>
#include <cstdio>
#include <sys/types.h>
#include <sys/stat.h>
#include <fcntl.h>

#ifndef STDERR_FILENO
#define STDERR_FILENO 2
#endif

using namespace arki::tests;

namespace tut {
using namespace std;
using namespace arki;
using namespace arki::utils;

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
        config.parse(conf, "(memory)");
    }

    void produceGRIB(Postprocess& p)
    {
        scan::scan("inbound/test.grib1", [&](unique_ptr<Metadata> md) { return p.process(move(md)); });
    }
};
TESTGRP(arki_postprocess);

// See if the postprocess makes a difference
def_test(1)
{
    Postprocess p("null");
    Stderr out;
    p.set_output(out);
    p.validate(config.section("testall")->values());
    p.start();

    produceGRIB(p);

    p.flush();
}

// Check that it works without validation, too
def_test(2)
{
    Postprocess p("null");
    Stderr out;
    p.set_output(out);
    p.start();

    produceGRIB(p);

    p.flush();
}

// Test actually sending some data
def_test(3)
{
    sys::File out(sys::File::mkstemp("test"));
    Postprocess p("countbytes");
    p.set_output(out);
    p.start();

    produceGRIB(p);
    p.flush();
    out.close();

    ensure_equals(sys::read_file(out.name()), "44961\n");
}

// Test actually sending some data
def_test(4)
{
    // Get the normal data
    vector<uint8_t> plain;
    {
        BinaryEncoder enc(plain);
        scan::scan("inbound/test.grib1", [&](unique_ptr<Metadata> md) {
            md->makeInline();
            md->encodeBinary(enc);
            const auto& data = md->getData();
            enc.add_raw(data);
            return true;
        });
    }

    // Get the postprocessed data
    sys::File out(sys::File::mkstemp("test"));
    Postprocess p("cat");
    p.set_output(out);
    p.start();
    scan::scan("inbound/test.grib1", [&](unique_ptr<Metadata> md) { return p.process(move(md)); });
    p.flush();
    out.close();

    string postprocessed = sys::read_file(out.name());
    wassert(actual(vector<uint8_t>(postprocessed.begin(), postprocessed.end()) == plain));
}

// Try to shift a sizeable chunk of data to the postprocessor
def_test(5)
{
    sys::File out(sys::File::mkstemp("test"));
    Postprocess p("countbytes");
    p.set_output(out);
    p.start();

    for (unsigned i = 0; i < 128; ++i)
        produceGRIB(p);
    p.flush();
    out.close();

    wassert(actual(sys::read_file(out.name())) == "5755008\n");
}

// Try to shift a sizeable chunk of data out of the postprocessor
def_test(6)
{
    const char* fname = "postprocess_output";
    stringstream str;
    Postprocess p("zeroes 4096");

    sys::File fd(fname, O_WRONLY | O_CREAT | O_NOCTTY, 0666);
    p.set_output(fd);
    p.start();

    produceGRIB(p);
    p.flush();

    wassert(actual(sys::size(fname)) == 4096*1024u);

    sys::unlink(fname);
}

// Try to shift a sizeable chunk of data to and out of the postprocessor
def_test(7)
{
    const char* fname = "postprocess_output";
    stringstream str;
    Postprocess p("zeroes 4096");

    sys::File fd(fname, O_WRONLY | O_CREAT | O_NOCTTY, 0666);
    p.set_output(fd);
    p.start();

    for (unsigned i = 0; i < 128; ++i)
        produceGRIB(p);
    p.flush();

    wassert(actual(sys::size(fname)) == 4096*1024u);

    sys::unlink(fname);
}

}
