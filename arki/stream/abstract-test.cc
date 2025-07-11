#include "abstract.h"
#include "arki/utils/sys.h"
#include "filter.h"
#include "tests.h"

using namespace std;
using namespace arki;
using namespace arki::core;
using namespace arki::utils;
using namespace arki::tests;

namespace {

struct TestStream : public stream::AbstractStreamOutput<stream::TestingBackend>
{
    using AbstractStreamOutput::AbstractStreamOutput;

    std::string streamed;

    std::string name() const override { return "test stream"; }
    std::filesystem::path path() const override { return "test stream"; }

    stream::SendResult _write_output_buffer(const void* data,
                                            size_t size) override
    {
        streamed += std::string((const char*)data, size);
        return stream::SendResult();
    }
};

class Tests : public arki::tests::TestCase
{
    using TestCase::TestCase;

    void register_tests() override;

    // std::unique_ptr<stream::StreamTestsFixture> make_fixture() override
    // {
    //     return std::unique_ptr<stream::StreamTestsFixture>(new
    //     CommonTestFixture);
    // }

    // std::unique_ptr<stream::StreamTestsFixture>
    // make_concrete_fixture(std::shared_ptr<core::NamedFileDescriptor> out)
    // {
    //     return std::unique_ptr<stream::StreamTestsFixture>(new Fixture(out));
    // }
};

Tests test("arki_stream_abstract");

void Tests::register_tests()
{

    add_method("syscalls_buffer_filtered", [this] {
        TestStream writer;
        auto filter = writer.start_filter({"cat"});

        // No timeout
        {
            stream::ExpectedSyscalls expected({
                new stream::ExpectedPoll(filter->cmd.get_stdin(), POLLOUT, -1,
                                         POLLOUT, 1),
                new stream::ExpectedWrite(filter->cmd.get_stdin(), "1234", 4),
            });
            wassert(actual(writer.send_buffer("1234", 4)) ==
                    stream::SendResult());
            wassert(expected.check_not_called());
        }

        {
            stream::ExpectedSyscalls expected({
                new stream::ExpectedPoll(filter->cmd.get_stdout(), POLLIN, -1,
                                         POLLIN, 1),
                new stream::ExpectedRead(filter->cmd.get_stdout(), "1234",
                                         16384, 4),
                new stream::ExpectedPoll(filter->cmd.get_stdout(), POLLIN, -1,
                                         POLLHUP, 1),
                new stream::ExpectedPoll(filter->cmd.get_stderr(), POLLIN, -1,
                                         POLLIN, 1),
                new stream::ExpectedRead(filter->cmd.get_stderr(), "FAIL", 256,
                                         4),
                new stream::ExpectedPoll(filter->cmd.get_stderr(), POLLIN, -1,
                                         POLLHUP, 1),
            });
            auto flt = wcallchecked(writer.stop_filter());
            wassert(actual(flt->size_stdin) == 4u);
            wassert(actual(flt->size_stdout) == 4u);
            wassert(actual(flt->errors.str()) == "FAIL");
            wassert(actual(flt->cmd.raw_returncode()) == 0);
            wassert(expected.check_not_called());
        }

        wassert(actual(writer.streamed) == "1234");
    });

    add_method("syscalls_line_filtered", [this] {
        TestStream writer;
        auto filter = writer.start_filter({"cat"});

        // No timeout
        {
            stream::ExpectedSyscalls expected({
                new stream::ExpectedPoll(filter->cmd.get_stdin(), POLLOUT, -1,
                                         POLLOUT, 1),
                new stream::ExpectedWritev(filter->cmd.get_stdin(),
                                           {"1234", "\n"}, 5),
            });
            wassert(actual(writer.send_line("1234", 4)) ==
                    stream::SendResult());
            wassert(expected.check_not_called());
        }

        {
            stream::ExpectedSyscalls expected({
                new stream::ExpectedPoll(filter->cmd.get_stdout(), POLLIN, -1,
                                         POLLIN, 1),
                new stream::ExpectedRead(filter->cmd.get_stdout(), "1234\n",
                                         16384, 5),
                new stream::ExpectedPoll(filter->cmd.get_stdout(), POLLIN, -1,
                                         POLLHUP, 1),
                new stream::ExpectedPoll(filter->cmd.get_stderr(), POLLIN, -1,
                                         POLLIN, 1),
                new stream::ExpectedRead(filter->cmd.get_stderr(), "FAIL", 256,
                                         4),
                new stream::ExpectedPoll(filter->cmd.get_stderr(), POLLIN, -1,
                                         POLLHUP, 1),
            });
            auto flt = wcallchecked(writer.stop_filter());
            wassert(actual(flt->size_stdin) == 5u);
            wassert(actual(flt->size_stdout) == 5u);
            wassert(actual(flt->errors.str()) == "FAIL");
            wassert(actual(flt->cmd.raw_returncode()) == 0);
            wassert(expected.check_not_called());
        }
    });

    add_method("syscalls_file_filtered", [this] {
        TestStream writer;
        auto filter = writer.start_filter({"cat"});
        sys::Tempfile tf;
        tf.write_all_or_throw(std::string("testfile"));

        // No timeout
        {
            stream::ExpectedSyscalls expected({
                new stream::ExpectedPoll(filter->cmd.get_stdin(), POLLOUT, -1,
                                         POLLOUT, 1),
                new stream::ExpectedSendfile(filter->cmd.get_stdin(), tf, 1, 4,
                                             5, 4),
            });
            wassert(actual(writer.send_file_segment(tf, 1, 4)) ==
                    stream::SendResult());
            wassert(expected.check_not_called());
        }

        {
            stream::ExpectedSyscalls expected({
                new stream::ExpectedPoll(filter->cmd.get_stdout(), POLLIN, -1,
                                         POLLIN, 1),
                new stream::ExpectedRead(filter->cmd.get_stdout(), "estf",
                                         16384, 4),
                new stream::ExpectedPoll(filter->cmd.get_stdout(), POLLIN, -1,
                                         POLLHUP, 1),
                new stream::ExpectedPoll(filter->cmd.get_stderr(), POLLIN, -1,
                                         POLLIN, 1),
                new stream::ExpectedRead(filter->cmd.get_stderr(), "FAIL", 256,
                                         4),
                new stream::ExpectedPoll(filter->cmd.get_stderr(), POLLIN, -1,
                                         POLLHUP, 1),
            });
            auto flt = wcallchecked(writer.stop_filter());
            wassert(actual(flt->size_stdin) == 4u);
            wassert(actual(flt->size_stdout) == 4u);
            wassert(actual(flt->errors.str()) == "FAIL");
            wassert(actual(flt->cmd.raw_returncode()) == 0);
            wassert(expected.check_not_called());
        }
    });
}

} // namespace
