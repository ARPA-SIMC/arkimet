#include "tests.h"
#include "base.h"
#include "filter.h"
#include "arki/core/file.h"
#include <vector>

using namespace std;
using namespace arki;
using namespace arki::core;
using namespace arki::utils;
using namespace arki::tests;

namespace {

struct CommonTestsFixture : public stream::StreamTestsFixture
{
    std::vector<uint8_t> buffer;

    CommonTestsFixture()
    {
        set_output(StreamOutput::create(buffer));
    }

    std::string streamed_contents() override
    {
        return std::string(buffer.begin(), buffer.end());
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

Tests test("arki_stream_buffer");

void Tests::register_tests() {
StreamTests::register_tests();

}

}
