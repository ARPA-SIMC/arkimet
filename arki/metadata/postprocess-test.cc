#include "arki/tests/tests.h"
#include "arki/segment.h"
#include "arki/metadata/data.h"
#include "arki/types/source/blob.h"
#include "arki/core/file.h"
#include "arki/core/cfg.h"
#include "arki/core/binary.h"
#include "arki/utils/sys.h"
#include "arki/metadata.h"
#include "postprocess.h"
#include <unistd.h>
#include <signal.h>
#include <csetjmp>

#ifndef STDERR_FILENO
#define STDERR_FILENO 2
#endif

namespace {
using namespace std;
using namespace arki;
using namespace arki::core;
using namespace arki::tests;
using namespace arki::utils;
using namespace arki::metadata;

std::jmp_buf timeout_jump;

[[noreturn]] static void timed_out(int signum)
{
    std::longjmp(timeout_jump, 1);
}

struct Timeout
{
    int seconds;

    Timeout(int seconds)
        : seconds(seconds)
    {
    }

    ~Timeout()
    {
        alarm(0);
        signal(SIGALRM, SIG_DFL);
    }

    void arm()
    {
        signal(SIGALRM, timed_out);
        alarm(seconds);
    }
};

#define TIMEOUT(secs) \
    Timeout timeout(secs); \
    switch (setjmp(timeout_jump)) { \
        case 0: timeout.arm(); break; \
        case 1: throw std::runtime_error("test timed out"); \
        default: throw std::runtime_error("unexpected value from signal handler"); \
    }

void produceGRIB(Postprocess& p)
{
    auto reader = Segment::detect_reader("grib", ".", "inbound/test.grib1", "inbound/test.grib1", std::make_shared<core::lock::Null>());
    reader->scan([&](std::shared_ptr<Metadata> md) { return p.process(move(md)); });
}

class Tests : public TestCase
{
    using TestCase::TestCase;
    void register_tests() override;
} test("arki_metadata_postprocess");

void Tests::register_tests() {

// See if the postprocess makes a difference
add_method("null_validate", [] {
    string conf =
        "type = test\n"
        "step = daily\n"
        "filter = origin: GRIB1\n"
        "index = origin, reftime\n"
        "postprocess = null\n"
        "name = testall\n"
        "path = testall\n";
    auto config = core::cfg::Section::parse(conf, "(memory)");

    Postprocess p("null");
    Stderr out;
    p.set_output(out);
    p.validate(config);
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
    auto reader = Segment::detect_reader("grib", ".", "inbound/test.grib1", "inbound/test.grib1", std::make_shared<core::lock::Null>());

    // Get the normal data
    vector<uint8_t> plain;
    {
        core::BinaryEncoder enc(plain);
        reader->scan([&](std::shared_ptr<Metadata> md) {
            md->makeInline();
            md->encodeBinary(enc);
            const auto& data = md->get_data().read();
            enc.add_raw(data);
            return true;
        });
    }

    // Get the postprocessed data
    sys::File out(sys::File::mkstemp("test"));
    Postprocess p("cat");
    p.set_output(out);
    p.start();
    reader->scan([&](std::shared_ptr<Metadata> md) { return p.process(md); });
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

add_method("issue209", [] {
    metadata::DataManager& data_manager = metadata::DataManager::get();

    Postprocess p("cat");

    TIMEOUT(2);

    sys::File fd("/dev/null", O_WRONLY);
    p.set_output(fd);
    p.start();

    Metadata::read_file("inbound/issue209.arkimet", [&](std::shared_ptr<Metadata> md) {
        md->set_source_inline("bufr",
                data_manager.to_data("bufr",
                    std::vector<uint8_t>(md->sourceBlob().size)));
        p.process(md);
        return true;
    });

    p.flush();
});

add_method("partialread", [] {
    metadata::DataManager& data_manager = metadata::DataManager::get();

    Postprocess p("partialread");

    TIMEOUT(2);

    sys::File fd("/dev/null", O_WRONLY);
    p.set_output(fd);
    p.start();

    try {
        Metadata::read_file("inbound/issue209.arkimet", [&](std::shared_ptr<Metadata> md) {
            md->set_source_inline("bufr",
                    data_manager.to_data("bufr",
                        std::vector<uint8_t>(md->sourceBlob().size)));
            p.process(md);
            return true;
        });

        p.flush();
    } catch (std::runtime_error& e) {
        wassert(actual(e.what()) == "cannot run postprocessing filter: postprocess command \"partialread\" exited with code 1; stderr: test: simulating stopping read with an error");
    }
});

}

}
