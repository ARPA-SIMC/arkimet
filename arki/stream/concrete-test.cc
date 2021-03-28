#include "tests.h"
#include "base.h"
#include "arki/utils/sys.h"

using namespace std;
using namespace arki;
using namespace arki::core;
using namespace arki::utils;
using namespace arki::tests;

namespace {

struct CommonTestsFixture : public stream::StreamTestsFixture
{
    sys::Tempfile tf;

    CommonTestsFixture()
    {
        set_output(StreamOutput::create(tf));
    }

    std::string streamed_contents() override
    {
        std::string res;
        tf.lseek(0);

        char buf[4096];
        while (size_t sz = tf.read(buf, 4096))
            res.append(buf, sz);

        return res;
    }
};

class Tests : public stream::StreamTests
{
    using StreamTests::StreamTests;

    void register_tests() override;

    std::unique_ptr<stream::StreamTestsFixture> make_fixture() override
    {
        return std::unique_ptr<stream::StreamTestsFixture>(new CommonTestsFixture);
    }
};

Tests test("arki_stream_concrete");

void Tests::register_tests() {
StreamTests::register_tests();

add_method("empty", [] {
});

}

}
