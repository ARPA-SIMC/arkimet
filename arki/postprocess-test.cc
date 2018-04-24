#include "arki/tests/tests.h"
#include "core/file.h"
#include "scan/any.h"
#include "utils/sys.h"
#include "configfile.h"
#include "metadata.h"
#include "postprocess.h"
#include "binary.h"

#ifndef STDERR_FILENO
#define STDERR_FILENO 2
#endif

namespace {
using namespace std;
using namespace arki;
using namespace arki::core;
using namespace arki::tests;
using namespace arki::utils;

void produceGRIB(Postprocess& p)
{
    scan::scan("inbound/test.grib1", std::make_shared<core::lock::Null>(), [&](unique_ptr<Metadata> md) { return p.process(move(md)); });
}

class Tests : public TestCase
{
    using TestCase::TestCase;
    void register_tests() override;
} test("arki_postprocess");

void Tests::register_tests() {

// See if the postprocess makes a difference
add_method("null_validate", [] {
    string conf =
        "[testall]\n"
        "type = test\n"
        "step = daily\n"
        "filter = origin: GRIB1\n"
        "index = origin, reftime\n"
        "postprocess = null\n"
        "name = testall\n"
        "path = testall\n";
    ConfigFile config(conf, "(memory)");

    Postprocess p("null");
    Stderr out;
    p.set_output(out);
    p.validate(config.section("testall")->values());
    p.start();

    produceGRIB(p);

    p.flush();
});

// Check that it works without validation, too
add_method("null", [] {
    Postprocess p("null");
    Stderr out;
    p.set_output(out);
    p.start();

    produceGRIB(p);

    p.flush();
});

add_method("countbytes", [] {
    sys::File out(sys::File::mkstemp("test"));
    Postprocess p("countbytes");
    p.set_output(out);
    p.start();

    produceGRIB(p);
    p.flush();
    out.close();

    wassert(actual(sys::read_file(out.name())) == "44937\n");
});

add_method("cat", [] {
    // Get the normal data
    vector<uint8_t> plain;
    {
        BinaryEncoder enc(plain);
        scan::scan("inbound/test.grib1", std::make_shared<core::lock::Null>(), [&](unique_ptr<Metadata> md) {
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
    scan::scan("inbound/test.grib1", std::make_shared<core::lock::Null>(), [&](unique_ptr<Metadata> md) { return p.process(move(md)); });
    p.flush();
    out.close();

    string postprocessed = sys::read_file(out.name());
    wassert(actual(vector<uint8_t>(postprocessed.begin(), postprocessed.end()) == plain));
});

// Try to shift a sizeable chunk of data to the postprocessor
add_method("countbytes_large", [] {
    sys::File out(sys::File::mkstemp("test"));
    Postprocess p("countbytes");
    p.set_output(out);
    p.start();

    for (unsigned i = 0; i < 128; ++i)
        produceGRIB(p);
    p.flush();
    out.close();

    wassert(actual(sys::read_file(out.name())) == "5751936\n");
});

add_method("zeroes_arg", [] {
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
});

add_method("zeroes_arg_large", [] {
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
});

}

}
