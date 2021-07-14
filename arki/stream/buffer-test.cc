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

add_method("syscalls_buffer_filtered", [this] {
    auto writer = make_fixture();
    auto filter = writer->stream().start_filter({"cat"});

    // No timeout
    {
        stream::ExpectedSyscalls expected({
            new stream::ExpectedPoll(filter->cmd.get_stdin(), POLLOUT, -1, POLLOUT, 1),
            new stream::ExpectedWrite(filter->cmd.get_stdin(), "1234", 4),
        });
        wassert(actual(writer->send_buffer("1234", 4)) == stream::SendResult());
        wassert(expected.check_not_called());
    }

    {
        stream::ExpectedSyscalls expected({
            new stream::ExpectedPoll(filter->cmd.get_stdout(), POLLIN, -1, POLLIN, 1),
            new stream::ExpectedRead(filter->cmd.get_stdout(), "1234", 32768, 4),
            new stream::ExpectedPoll(filter->cmd.get_stdout(), POLLIN, -1, POLLHUP, 1),
            new stream::ExpectedPoll(filter->cmd.get_stderr(), POLLIN, -1, POLLIN, 1),
            new stream::ExpectedRead(filter->cmd.get_stderr(), "FAIL", 32768, 4),
            new stream::ExpectedPoll(filter->cmd.get_stderr(), POLLIN, -1, POLLHUP, 1),
        });
        auto flt = wcallchecked(writer->stream().stop_filter());
        wassert(actual(flt->size_stdin) == 4u);
        wassert(actual(flt->size_stdout) == 4u);
        wassert(actual(flt->errors.str()) == "FAIL");
        wassert(actual(flt->cmd.raw_returncode()) == 0);
        wassert(expected.check_not_called());
    }

    wassert(actual(writer->streamed_contents()) == "1234");
});


}

}
