#include "tests.h"
#include "concrete.h"
#include "filter.h"
#include "arki/utils/sys.h"
#include <system_error>

using namespace std;
using namespace arki;
using namespace arki::core;
using namespace arki::utils;
using namespace arki::tests;

namespace {

/**
 * Test fixture used by common stream tests from stream/tests.cc
 */
struct CommonTestFixture : public stream::StreamTestsFixture
{
    std::shared_ptr<sys::Tempfile> tf;

    CommonTestFixture()
        : tf(std::make_shared<sys::Tempfile>())
    {
        set_output(StreamOutput::create(tf));
    }

    std::string streamed_contents() override
    {
        std::string res;
        tf->lseek(0);

        char buf[4096];
        while (size_t sz = tf->read(buf, 4096))
            res.append(buf, sz);

        return res;
    }
};


/**
 * Stream fixtures used by tests here
 */
struct ConcreteTestFixture : public stream::StreamTestsFixture
{
    std::shared_ptr<core::NamedFileDescriptor> out;

    ConcreteTestFixture(std::shared_ptr<core::NamedFileDescriptor> out)
        : out(out)
    {
        set_output(
                std::unique_ptr<arki::StreamOutput>(
                    new stream::ConcreteStreamOutputBase<stream::TestingBackend>(out, get_timeout_ms())));
    }

    static int get_timeout_ms() { return -1; }
};

class ConcreteFallbackTestFixture : public ConcreteTestFixture
{
    using ConcreteTestFixture::ConcreteTestFixture;

    stream::DisableSendfileSplice no_sendfile_and_splice;
};

struct ConcreteTimeoutTestFixture : public stream::StreamTestsFixture
{
    std::shared_ptr<core::NamedFileDescriptor> out;

    ConcreteTimeoutTestFixture(std::shared_ptr<core::NamedFileDescriptor> out)
        : out(out)
    {
        set_output(
                std::unique_ptr<arki::StreamOutput>(
                    new stream::ConcreteStreamOutputBase<stream::TestingBackend>(out, get_timeout_ms())));
    }

    static int get_timeout_ms() { return 1000; }
};

class ConcreteTimeoutFallbackTestFixture : public ConcreteTimeoutTestFixture
{
    using ConcreteTimeoutTestFixture::ConcreteTimeoutTestFixture;

    stream::DisableSendfileSplice no_sendfile_and_splice;
};

struct ClosedPipe
{
    int pipefds[2];
    std::shared_ptr<sys::NamedFileDescriptor> fd;

    ClosedPipe()
    {
        if (pipe(pipefds) < 0)
            throw std::system_error(errno, std::system_category(), "cannot create new pipe");
        // Close read end of the pipe
        close(pipefds[0]);

        fd = std::make_shared<sys::NamedFileDescriptor>(pipefds[1], "write end of pipe");
    }
};


template<typename Fixture>
class Tests : public stream::StreamTests
{
    using StreamTests::StreamTests;

    void register_tests() override;

    std::unique_ptr<stream::StreamTestsFixture> make_fixture() override
    {
        return std::unique_ptr<stream::StreamTestsFixture>(new CommonTestFixture);
    }

    std::unique_ptr<stream::StreamTestsFixture> make_concrete_fixture(std::shared_ptr<core::NamedFileDescriptor> out)
    {
        return std::unique_ptr<stream::StreamTestsFixture>(new Fixture(out));
    }
};

Tests<ConcreteTestFixture> test1("arki_stream_concrete");
Tests<ConcreteFallbackTestFixture> test2("arki_stream_concrete_fallback");
Tests<ConcreteTimeoutTestFixture> test3("arki_stream_concrete_timeout");
Tests<ConcreteTimeoutFallbackTestFixture> test4("arki_stream_concrete_timeout_fallback");

template<typename Fixture>
void Tests<Fixture>::register_tests() {
stream::StreamTests::register_tests();

add_method("closed_pipe_send_buffer", [&] {
    ClosedPipe cp;
    auto f = make_concrete_fixture(cp.fd);
    wassert(actual(f->send_buffer("A", 1)) == stream::SendResult(stream::SendResult::SEND_PIPE_EOF_DEST));
});

add_method("closed_pipe_send_line", [&] {
    ClosedPipe cp;
    auto f = make_concrete_fixture(cp.fd);
    wassert(actual(f->send_line("A", 1)) == stream::SendResult(stream::SendResult::SEND_PIPE_EOF_DEST));
});

add_method("closed_pipe_send_file_segment", [&] {
    ClosedPipe cp;
    auto f = make_concrete_fixture(cp.fd);

    sys::Tempfile tf;
    tf.write_all_or_retry("test", 4);
    wassert(actual(f->send_file_segment(tf, 1, 1)) == stream::SendResult(stream::SendResult::SEND_PIPE_EOF_DEST));
});

add_method("read_eof", [&] {
    struct ReadEof : stream::DisableSendfileSplice
    {
        size_t read_pos = 0;
        size_t available;

        ReadEof(size_t lead_size=0)
            : available(lead_size)
        {
            stream::TestingBackend::read = [this](int fd, void *buf, size_t count) -> ssize_t {
                if (read_pos < available) {
                    count = std::min(count, available - read_pos);
                    memset(buf, 0, count);
                    read_pos += count;
                    return count;
                } else {
                    return 0;
                }
            };
            stream::TestingBackend::pread = [this](int fd, void *buf, size_t count, off_t offset) -> ssize_t {
                if ((size_t)offset < available) {
                    count = std::min(count, available - offset);
                    memset(buf, 0, count);
                    return count;
                } else {
                    return 0;
                }
            };
        }
    };

    {
        ReadEof reof(10);
        auto f = make_concrete_fixture(std::make_shared<sys::File>("/dev/null", O_WRONLY));
        sys::NamedFileDescriptor input(0, "mock input");
        wassert(actual(f->send_file_segment(input, 5, 15)) == stream::SendResult(stream::SendResult::SEND_PIPE_EOF_SOURCE));
    }
});

add_method("syscalls_buffer", [this] {
    auto outfile = std::make_shared<sys::File>("/dev/null", O_WRONLY);
    auto writer = make_concrete_fixture(outfile);

    // No timeout
    {
        stream::ExpectedSyscalls expected({
            new stream::ExpectedPoll(*outfile, POLLOUT, Fixture::get_timeout_ms(), POLLOUT, 1),
            new stream::ExpectedWrite(*outfile, "1234", 4),
        });
        wassert(actual(writer->send_buffer("1234", 4)) == stream::SendResult());
        wassert(expected.check_not_called());
    }

    {
        stream::ExpectedSyscalls expected({
            new stream::ExpectedPoll(*outfile, POLLOUT, Fixture::get_timeout_ms(), POLLOUT, 1),
            new stream::ExpectedWrite(*outfile, "1234", 2),
            new stream::ExpectedPoll(*outfile, POLLOUT, Fixture::get_timeout_ms(), POLLERR, 1),
        });
        wassert(actual(writer->send_buffer("1234", 4)) == stream::SendResult(stream::SendResult::SEND_PIPE_EOF_DEST));
        wassert(expected.check_not_called());
    }

    {
        stream::ExpectedSyscalls expected({
            new stream::ExpectedPoll(*outfile, POLLOUT, Fixture::get_timeout_ms(), POLLOUT, 1),
            new stream::ExpectedWrite(*outfile, "1234", 2),
            new stream::ExpectedPoll(*outfile, POLLOUT, Fixture::get_timeout_ms(), POLLHUP, 1),
        });
        wassert(actual(writer->send_buffer("1234", 4)) == stream::SendResult(stream::SendResult::SEND_PIPE_EOF_DEST));
        wassert(expected.check_not_called());
    }

    {
        stream::ExpectedSyscalls expected({
            new stream::ExpectedPoll(*outfile, POLLOUT, Fixture::get_timeout_ms(), POLLOUT, 1),
            new stream::ExpectedWrite(*outfile, "1234", -1, EAGAIN),
            new stream::ExpectedPoll(*outfile, POLLOUT, Fixture::get_timeout_ms(), POLLOUT, 1),
            new stream::ExpectedWrite(*outfile, "1234", 4),
        });
        wassert(actual(writer->send_buffer("1234", 4)) == stream::SendResult());
        wassert(expected.check_not_called());
    }

    {
        stream::ExpectedSyscalls expected({
            new stream::ExpectedPoll(*outfile, POLLOUT, Fixture::get_timeout_ms(), POLLOUT, 1),
            new stream::ExpectedWrite(*outfile, "1234", 2),
            new stream::ExpectedPoll(*outfile, POLLOUT, Fixture::get_timeout_ms(), POLLOUT, 1),
            new stream::ExpectedWrite(*outfile, "34", -1, EAGAIN),
            new stream::ExpectedPoll(*outfile, POLLOUT, Fixture::get_timeout_ms(), POLLOUT, 1),
            new stream::ExpectedWrite(*outfile, "34", 2),
        });
        wassert(actual(writer->send_buffer("1234", 4)) == stream::SendResult());
        wassert(expected.check_not_called());
    }

    // Timeout
    {
        stream::ExpectedSyscalls expected({
            new stream::ExpectedPoll(*outfile, POLLOUT, Fixture::get_timeout_ms(), POLLOUT, 1),
            new stream::ExpectedWrite(*outfile, "1234", 2),
            new stream::ExpectedPoll(*outfile, POLLOUT, Fixture::get_timeout_ms(), 0, 0),
        });
        wassert_throws(stream::TimedOut, writer->send_buffer("1234", 4));
        wassert(expected.check_not_called());
    }
});

add_method("syscalls_buffer_filtered", [this] {
    auto outfile = std::make_shared<sys::File>("/dev/null", O_WRONLY);
    auto writer = make_concrete_fixture(outfile);
    auto filter = writer->stream().start_filter({"cat"});

    // No timeout
    {
        stream::ExpectedSyscalls expected({
            new stream::ExpectedPoll(filter->cmd.get_stdin(), POLLOUT, Fixture::get_timeout_ms(), POLLOUT, 1),
            new stream::ExpectedWrite(filter->cmd.get_stdin(), "1234", 4),
        });
        wassert(actual(writer->send_buffer("1234", 4)) == stream::SendResult());
        wassert(expected.check_not_called());
    }

    {
        stream::ExpectedSyscalls expected({
            new stream::ExpectedPoll(filter->cmd.get_stdout(), POLLIN, Fixture::get_timeout_ms(), POLLIN, 1),
            new stream::ExpectedPoll(*outfile, POLLOUT, Fixture::get_timeout_ms(), POLLOUT, 1),
            new stream::ExpectedSplice(filter->cmd.get_stdout(), *outfile, 131072, SPLICE_F_MORE | SPLICE_F_NONBLOCK, 4),
            new stream::ExpectedPoll(filter->cmd.get_stdout(), POLLIN, Fixture::get_timeout_ms(), POLLHUP, 1),
            new stream::ExpectedPoll(filter->cmd.get_stderr(), POLLIN, Fixture::get_timeout_ms(), POLLIN, 1),
            new stream::ExpectedRead(filter->cmd.get_stderr(), "FAIL", 256, 4),
            new stream::ExpectedPoll(filter->cmd.get_stderr(), POLLIN, Fixture::get_timeout_ms(), POLLHUP, 1),
        });
        auto flt = wcallchecked(writer->stream().stop_filter());
        wassert(expected.check_not_called());
        wassert(actual(flt->size_stdin) == 4u);
        wassert(actual(flt->size_stdout) == 4u);
        wassert(actual(flt->errors.str()) == "FAIL");
        wassert(actual(flt->cmd.raw_returncode()) == 0);
    }
});

add_method("syscalls_buffer_filtered_readwrite", [this] {
    auto outfile = std::make_shared<sys::File>("/dev/null", O_WRONLY);
    auto writer = make_concrete_fixture(outfile);
    auto filter = writer->stream().start_filter({"cat"});

    // No timeout
    {
        stream::ExpectedSyscalls expected({
            new stream::ExpectedPoll(filter->cmd.get_stdin(), POLLOUT, Fixture::get_timeout_ms(), POLLOUT, 1),
            new stream::ExpectedWrite(filter->cmd.get_stdin(), "1234", 4),
        });
        wassert(actual(writer->send_buffer("1234", 4)) == stream::SendResult());
        wassert(expected.check_not_called());
    }

    {
        stream::ExpectedSyscalls expected({
            new stream::ExpectedPoll(filter->cmd.get_stdout(), POLLIN, Fixture::get_timeout_ms(), POLLIN, 1),
            new stream::ExpectedPoll(*outfile, POLLOUT, Fixture::get_timeout_ms(), POLLOUT, 1),
            new stream::ExpectedSplice(filter->cmd.get_stdout(), *outfile, 131072, SPLICE_F_MORE | SPLICE_F_NONBLOCK, -1, EINVAL),
            new stream::ExpectedPoll(filter->cmd.get_stdout(), POLLIN, Fixture::get_timeout_ms(), POLLIN, 1),
            new stream::ExpectedRead(filter->cmd.get_stdout(), "1234", 32768, 4),
            new stream::ExpectedPoll(filter->cmd.get_stdout(), POLLIN, Fixture::get_timeout_ms(), POLLHUP, 1),
            new stream::ExpectedPoll(filter->cmd.get_stderr(), POLLIN, Fixture::get_timeout_ms(), POLLIN, 1),
            new stream::ExpectedRead(filter->cmd.get_stderr(), "FAIL", 256, 4),
            new stream::ExpectedPoll(filter->cmd.get_stderr(), POLLIN, Fixture::get_timeout_ms(), POLLHUP, 1),
            new stream::ExpectedPoll(*outfile, POLLOUT, Fixture::get_timeout_ms(), POLLOUT, 1),
            new stream::ExpectedWrite(*outfile, "1234", 4, 4),
        });
        auto flt = wcallchecked(writer->stream().stop_filter());
        wassert(actual(flt->size_stdin) == 4u);
        wassert(actual(flt->size_stdout) == 4u);
        wassert(actual(flt->errors.str()) == "FAIL");
        wassert(actual(flt->cmd.raw_returncode()) == 0);
        wassert(expected.check_not_called());
    }
});

add_method("syscalls_buffer_filtered_regression1", [this] {
    auto outfile = std::make_shared<sys::File>("/dev/null", O_WRONLY);
    auto writer = make_concrete_fixture(outfile);
    auto filter = writer->stream().start_filter({"cat"});

    // No timeout
    {
        stream::ExpectedSyscalls expected({
            new stream::ExpectedPoll(filter->cmd.get_stdin(), POLLOUT, Fixture::get_timeout_ms(), POLLOUT, 1),
            new stream::ExpectedWrite(filter->cmd.get_stdin(), "1234", 4),
        });
        wassert(actual(writer->send_buffer("1234", 4)) == stream::SendResult());
        wassert(expected.check_not_called());
    }

    {
        stream::ExpectedSyscalls expected({
            new stream::ExpectedPoll(filter->cmd.get_stderr(), POLLIN, Fixture::get_timeout_ms(), POLLHUP, 1),
            new stream::ExpectedPoll(*outfile, POLLOUT, Fixture::get_timeout_ms(), POLLOUT, 1),
            new stream::ExpectedPoll(filter->cmd.get_stdout(), POLLIN, Fixture::get_timeout_ms(), POLLIN, 1),
            new stream::ExpectedSplice(filter->cmd.get_stdout(), *outfile, 131072, SPLICE_F_MORE | SPLICE_F_NONBLOCK, 4),
            new stream::ExpectedPoll(filter->cmd.get_stdout(), POLLIN, Fixture::get_timeout_ms(), POLLHUP, 1),
        });
        auto flt = wcallchecked(writer->stream().stop_filter());
        wassert(expected.check_not_called());
        wassert(actual(flt->size_stdin) == 4u);
        wassert(actual(flt->size_stdout) == 4u);
        wassert(actual(flt->errors.str()) == "");
        wassert(actual(flt->cmd.raw_returncode()) == 0);
    }
});

add_method("syscalls_line", [this] {
    auto outfile = std::make_shared<sys::File>("/dev/null", O_WRONLY);
    auto writer = make_concrete_fixture(outfile);

    // No timeout
    {
        stream::ExpectedSyscalls expected({
            new stream::ExpectedPoll(*outfile, POLLOUT, Fixture::get_timeout_ms(), POLLOUT, 1),
            new stream::ExpectedWritev(*outfile, {"1234", "\n"}, 5),
        });
        wassert(actual(writer->send_line("1234", 4)) == stream::SendResult());
        wassert(expected.check_not_called());
    }

    {
        stream::ExpectedSyscalls expected({
            new stream::ExpectedPoll(*outfile, POLLOUT, Fixture::get_timeout_ms(), POLLOUT, 1),
            new stream::ExpectedWritev(*outfile, {"1234", "\n"}, 3),
            new stream::ExpectedPoll(*outfile, POLLOUT, Fixture::get_timeout_ms(), POLLERR, 1),
        });
        wassert(actual(writer->send_line("1234", 4)) == stream::SendResult(stream::SendResult::SEND_PIPE_EOF_DEST));
        wassert(expected.check_not_called());
    }

    {
        stream::ExpectedSyscalls expected({
            new stream::ExpectedPoll(*outfile, POLLOUT, Fixture::get_timeout_ms(), POLLOUT, 1),
            new stream::ExpectedWritev(*outfile, {"1234", "\n"}, 3),
            new stream::ExpectedPoll(*outfile, POLLOUT, Fixture::get_timeout_ms(), POLLHUP, 1),
        });
        wassert(actual(writer->send_line("1234", 4)) == stream::SendResult(stream::SendResult::SEND_PIPE_EOF_DEST));
        wassert(expected.check_not_called());
    }

    {
        stream::ExpectedSyscalls expected({
            new stream::ExpectedPoll(*outfile, POLLOUT, Fixture::get_timeout_ms(), POLLOUT, 1),
            new stream::ExpectedWritev(*outfile, {"1234", "\n"}, -1, EAGAIN),
            new stream::ExpectedPoll(*outfile, POLLOUT, Fixture::get_timeout_ms(), POLLOUT, 1),
            new stream::ExpectedWritev(*outfile, {"1234", "\n"}, 5),
        });
        wassert(actual(writer->send_line("1234", 4)) == stream::SendResult());
        wassert(expected.check_not_called());
    }

    {
        stream::ExpectedSyscalls expected({
            new stream::ExpectedPoll(*outfile, POLLOUT, Fixture::get_timeout_ms(), POLLOUT, 1),
            new stream::ExpectedWritev(*outfile, {"1234", "\n"}, 2),
            new stream::ExpectedPoll(*outfile, POLLOUT, Fixture::get_timeout_ms(), POLLOUT, 1),
            new stream::ExpectedWritev(*outfile, {"34", "\n"}, 3),
        });
        wassert(actual(writer->send_line("1234", 4)) == stream::SendResult());
        wassert(expected.check_not_called());
    }

    {
        stream::ExpectedSyscalls expected({
            new stream::ExpectedPoll(*outfile, POLLOUT, Fixture::get_timeout_ms(), POLLOUT, 1),
            new stream::ExpectedWritev(*outfile, {"1234", "\n"}, 3),
            new stream::ExpectedPoll(*outfile, POLLOUT, Fixture::get_timeout_ms(), POLLOUT, 1),
            new stream::ExpectedWritev(*outfile, {"4", "\n"}, 2),
        });
        wassert(actual(writer->send_line("1234", 4)) == stream::SendResult());
        wassert(expected.check_not_called());
    }

    {
        stream::ExpectedSyscalls expected({
            new stream::ExpectedPoll(*outfile, POLLOUT, Fixture::get_timeout_ms(), POLLOUT, 1),
            new stream::ExpectedWritev(*outfile, {"1234", "\n"}, 4),
            new stream::ExpectedPoll(*outfile, POLLOUT, Fixture::get_timeout_ms(), POLLOUT, 1),
            new stream::ExpectedWrite(*outfile, "\n", 1),
        });
        wassert(actual(writer->send_line("1234", 4)) == stream::SendResult());
        wassert(expected.check_not_called());
    }

    {
        stream::ExpectedSyscalls expected({
            new stream::ExpectedPoll(*outfile, POLLOUT, Fixture::get_timeout_ms(), POLLOUT, 1),
            new stream::ExpectedWritev(*outfile, {"1234", "\n"}, 4),
            new stream::ExpectedPoll(*outfile, POLLOUT, Fixture::get_timeout_ms(), POLLOUT, 1),
            new stream::ExpectedWrite(*outfile, "\n", -1, EAGAIN),
            new stream::ExpectedPoll(*outfile, POLLOUT, Fixture::get_timeout_ms(), POLLOUT, 1),
            new stream::ExpectedWrite(*outfile, "\n", 1),
        });
        wassert(actual(writer->send_line("1234", 4)) == stream::SendResult());
        wassert(expected.check_not_called());
    }

    // Timeout
    {
        stream::ExpectedSyscalls expected({
            new stream::ExpectedPoll(*outfile, POLLOUT, Fixture::get_timeout_ms(), POLLOUT, 1),
            new stream::ExpectedWritev(*outfile, {"1234", "\n"}, 2),
            new stream::ExpectedPoll(*outfile, POLLOUT, Fixture::get_timeout_ms(), 0, 0),
        });
        wassert_throws(stream::TimedOut, writer->send_line("1234", 4));
        wassert(expected.check_not_called());
    }

    {
        stream::ExpectedSyscalls expected({
            new stream::ExpectedPoll(*outfile, POLLOUT, Fixture::get_timeout_ms(), POLLOUT, 1),
            new stream::ExpectedWritev(*outfile, {"1234", "\n"}, 4),
            new stream::ExpectedPoll(*outfile, POLLOUT, Fixture::get_timeout_ms(), 0, 0),
        });
        wassert_throws(stream::TimedOut, writer->send_line("1234", 4));
        wassert(expected.check_not_called());
    }
});

add_method("syscalls_line_filtered", [this] {
    auto outfile = std::make_shared<sys::File>("/dev/null", O_WRONLY);
    auto writer = make_concrete_fixture(outfile);
    auto filter = writer->stream().start_filter({"cat"});

    // No timeout
    {
        stream::ExpectedSyscalls expected({
            new stream::ExpectedPoll(filter->cmd.get_stdin(), POLLOUT, Fixture::get_timeout_ms(), POLLOUT, 1),
            new stream::ExpectedWritev(filter->cmd.get_stdin(), {"1234", "\n"}, 5),
        });
        wassert(actual(writer->send_line("1234", 4)) == stream::SendResult());
        wassert(expected.check_not_called());
    }

    {
        stream::ExpectedSyscalls expected({
            new stream::ExpectedPoll(filter->cmd.get_stdout(), POLLIN, Fixture::get_timeout_ms(), POLLIN, 1),
            new stream::ExpectedPoll(*outfile, POLLOUT, Fixture::get_timeout_ms(), POLLOUT, 1),
            new stream::ExpectedSplice(filter->cmd.get_stdout(), *outfile, 131072, SPLICE_F_MORE | SPLICE_F_NONBLOCK, 5),
            new stream::ExpectedPoll(filter->cmd.get_stdout(), POLLIN, Fixture::get_timeout_ms(), POLLHUP, 1),
            new stream::ExpectedPoll(filter->cmd.get_stderr(), POLLIN, Fixture::get_timeout_ms(), POLLIN, 1),
            new stream::ExpectedRead(filter->cmd.get_stderr(), "FAIL", 256, 4),
            new stream::ExpectedPoll(filter->cmd.get_stderr(), POLLIN, Fixture::get_timeout_ms(), POLLHUP, 1),
        });
        auto flt = wcallchecked(writer->stream().stop_filter());
        wassert(actual(flt->size_stdin) == 5u);
        wassert(actual(flt->size_stdout) == 5u);
        wassert(actual(flt->errors.str()) == "FAIL");
        wassert(actual(flt->cmd.raw_returncode()) == 0);
        wassert(expected.check_not_called());
    }
});

add_method("syscalls_line_filtered_readwrite", [this] {
    auto outfile = std::make_shared<sys::File>("/dev/null", O_WRONLY);
    auto writer = make_concrete_fixture(outfile);
    auto filter = writer->stream().start_filter({"cat"});

    // No timeout
    {
        stream::ExpectedSyscalls expected({
            new stream::ExpectedPoll(filter->cmd.get_stdin(), POLLOUT, Fixture::get_timeout_ms(), POLLOUT, 1),
            new stream::ExpectedWritev(filter->cmd.get_stdin(), {"1234", "\n"}, 5),
        });
        wassert(actual(writer->send_line("1234", 4)) == stream::SendResult());
        wassert(expected.check_not_called());
    }

    {
        stream::ExpectedSyscalls expected({
            new stream::ExpectedPoll(filter->cmd.get_stdout(), POLLIN, Fixture::get_timeout_ms(), POLLIN, 1),
            new stream::ExpectedPoll(*outfile, POLLOUT, Fixture::get_timeout_ms(), POLLOUT, 1),
            new stream::ExpectedSplice(filter->cmd.get_stdout(), *outfile, 131072, SPLICE_F_MORE | SPLICE_F_NONBLOCK, -1, EINVAL),
            new stream::ExpectedPoll(filter->cmd.get_stdout(), POLLIN, Fixture::get_timeout_ms(), POLLIN, 1),
            new stream::ExpectedRead(filter->cmd.get_stdout(), "1234\n", 32768, 5),
            new stream::ExpectedPoll(filter->cmd.get_stdout(), POLLIN, Fixture::get_timeout_ms(), POLLHUP, 1),
            new stream::ExpectedPoll(filter->cmd.get_stderr(), POLLIN, Fixture::get_timeout_ms(), POLLIN, 1),
            new stream::ExpectedRead(filter->cmd.get_stderr(), "FAIL", 256, 4),
            new stream::ExpectedPoll(filter->cmd.get_stderr(), POLLIN, Fixture::get_timeout_ms(), POLLHUP, 1),
            new stream::ExpectedPoll(*outfile, POLLOUT, Fixture::get_timeout_ms(), POLLOUT, 1),
            new stream::ExpectedWrite(*outfile, "1234\n", 5, 5),
        });
        auto flt = wcallchecked(writer->stream().stop_filter());
        wassert(actual(flt->size_stdin) == 5u);
        wassert(actual(flt->size_stdout) == 5u);
        wassert(actual(flt->errors.str()) == "FAIL");
        wassert(actual(flt->cmd.raw_returncode()) == 0);
        wassert(expected.check_not_called());
    }
});

add_method("syscalls_file", [this] {
    auto outfile = std::make_shared<sys::File>("/dev/null", O_WRONLY);
    auto writer = make_concrete_fixture(outfile);
    sys::Tempfile tf;
    tf.write_all_or_throw(std::string("testfile"));

    // No timeout
    {
        stream::ExpectedSyscalls expected({
            new stream::ExpectedPoll(*outfile, POLLOUT, Fixture::get_timeout_ms(), POLLOUT, 1),
            new stream::ExpectedSendfile(*outfile, tf, 1, 4, 3, 2),
            new stream::ExpectedPoll(*outfile, POLLOUT, Fixture::get_timeout_ms(), POLLOUT, 1),
            new stream::ExpectedSendfile(*outfile, tf, 3, 2, 5, 2),
        });
        wassert(actual(writer->send_file_segment(tf, 1, 4)) == stream::SendResult());
        wassert(expected.check_not_called());
    }

    {
        stream::ExpectedSyscalls expected({
            new stream::ExpectedPoll(*outfile, POLLOUT, Fixture::get_timeout_ms(), POLLOUT, 1),
            new stream::ExpectedSendfile(*outfile, tf, 1, 4, 5, 4),
        });
        wassert(actual(writer->send_file_segment(tf, 1, 4)) == stream::SendResult());
        wassert(expected.check_not_called());
    }

    {
        stream::ExpectedSyscalls expected({
            new stream::ExpectedPoll(*outfile, POLLOUT, Fixture::get_timeout_ms(), POLLOUT, 1),
            new stream::ExpectedSendfile(*outfile, tf, 1, 4, 1, -1, EINVAL),
            new stream::ExpectedPoll(*outfile, POLLOUT, Fixture::get_timeout_ms(), POLLOUT, 1),
            new stream::ExpectedPread(tf, 1, 4, "estf", 4),
            new stream::ExpectedWrite(*outfile, "estf", 4),
        });
        wassert(actual(writer->send_file_segment(tf, 1, 4)) == stream::SendResult());
        wassert(expected.check_not_called());
    }

    {
        stream::ExpectedSyscalls expected({
            new stream::ExpectedPoll(*outfile, POLLOUT, Fixture::get_timeout_ms(), POLLOUT, 1),
            new stream::ExpectedSendfile(*outfile, tf, 1, 4, 1, -1, EINVAL),
            new stream::ExpectedPoll(*outfile, POLLOUT, Fixture::get_timeout_ms(), POLLOUT, 1),
            new stream::ExpectedPread(tf, 1, 4, "estf", 4),
            new stream::ExpectedWrite(*outfile, "estf", 2),
            new stream::ExpectedPoll(*outfile, POLLOUT, Fixture::get_timeout_ms(), POLLOUT, 1),
            new stream::ExpectedWrite(*outfile, "tf", 2),
        });
        wassert(actual(writer->send_file_segment(tf, 1, 4)) == stream::SendResult());
        wassert(expected.check_not_called());
    }

    {
        stream::ExpectedSyscalls expected({
            new stream::ExpectedPoll(*outfile, POLLOUT, Fixture::get_timeout_ms(), POLLOUT, 1),
            new stream::ExpectedSendfile(*outfile, tf, 1, 4, 1, -1, EINVAL),
            new stream::ExpectedPoll(*outfile, POLLOUT, Fixture::get_timeout_ms(), POLLOUT, 1),
            new stream::ExpectedPread(tf, 1, 4, "est", 3),
            new stream::ExpectedWrite(*outfile, "est", 2),
            new stream::ExpectedPoll(*outfile, POLLOUT, Fixture::get_timeout_ms(), POLLOUT, 1),
            new stream::ExpectedWrite(*outfile, "t", 1),
            new stream::ExpectedPoll(*outfile, POLLOUT, Fixture::get_timeout_ms(), POLLOUT, 1),
            new stream::ExpectedPread(tf, 4, 1, "f", 1),
            new stream::ExpectedWrite(*outfile, "f", 1),
        });
        wassert(actual(writer->send_file_segment(tf, 1, 4)) == stream::SendResult());
        wassert(expected.check_not_called());
    }

    // Timeout
    {
        stream::ExpectedSyscalls expected({
            new stream::ExpectedPoll(*outfile, POLLOUT, Fixture::get_timeout_ms(), POLLOUT, 1),
            new stream::ExpectedSendfile(*outfile, tf, 1, 4, 3, 2),
            new stream::ExpectedPoll(*outfile, POLLOUT, Fixture::get_timeout_ms(), 0, 0),
        });
        wassert_throws(stream::TimedOut, writer->send_file_segment(tf, 1, 4));
        wassert(expected.check_not_called());
    }
//    {
//        sys::Tempfile tf1;
//        tf1.write_all_or_throw(std::string("testfile"));
//        tf1.lseek(0);
//        wassert_throws(stream::TimedOut, writer->send_from_pipe(tf1));
//    }
});

add_method("syscalls_file_filtered", [this] {
    auto outfile = std::make_shared<sys::File>("/dev/null", O_WRONLY);
    auto writer = make_concrete_fixture(outfile);
    auto filter = writer->stream().start_filter({"cat"});
    sys::Tempfile tf;
    tf.write_all_or_throw(std::string("testfile"));

    // No timeout
    {
        stream::ExpectedSyscalls expected({
            new stream::ExpectedPoll(filter->cmd.get_stdin(), POLLOUT, Fixture::get_timeout_ms(), POLLOUT, 1),
            new stream::ExpectedSendfile(filter->cmd.get_stdin(), tf, 1, 4, 5, 4),
        });
        wassert(actual(writer->send_file_segment(tf, 1, 4)) == stream::SendResult());
        wassert(expected.check_not_called());
    }

    {
        stream::ExpectedSyscalls expected({
            new stream::ExpectedPoll(filter->cmd.get_stdout(), POLLIN, Fixture::get_timeout_ms(), POLLIN, 1),
            new stream::ExpectedPoll(*outfile, POLLOUT, Fixture::get_timeout_ms(), POLLOUT, 1),
            new stream::ExpectedSplice(filter->cmd.get_stdout(), *outfile, 131072, SPLICE_F_MORE | SPLICE_F_NONBLOCK, 4),
            new stream::ExpectedPoll(filter->cmd.get_stdout(), POLLIN, Fixture::get_timeout_ms(), POLLHUP, 1),
            new stream::ExpectedPoll(filter->cmd.get_stderr(), POLLIN, Fixture::get_timeout_ms(), POLLIN, 1),
            new stream::ExpectedRead(filter->cmd.get_stderr(), "FAIL", 256, 4),
            new stream::ExpectedPoll(filter->cmd.get_stderr(), POLLIN, Fixture::get_timeout_ms(), POLLHUP, 1),
        });
        auto flt = wcallchecked(writer->stream().stop_filter());
        wassert(actual(flt->size_stdin) == 4u);
        wassert(actual(flt->size_stdout) == 4u);
        wassert(actual(flt->errors.str()) == "FAIL");
        wassert(actual(flt->cmd.raw_returncode()) == 0);
        wassert(expected.check_not_called());
    }
});

add_method("syscalls_file_filtered_readwrite", [this] {
    auto outfile = std::make_shared<sys::File>("/dev/null", O_WRONLY);
    auto writer = make_concrete_fixture(outfile);
    auto filter = writer->stream().start_filter({"cat"});
    sys::Tempfile tf;
    tf.write_all_or_throw(std::string("testfile"));

    // No timeout
    {
        stream::ExpectedSyscalls expected({
            new stream::ExpectedPoll(filter->cmd.get_stdin(), POLLOUT, Fixture::get_timeout_ms(), POLLOUT, 1),
            new stream::ExpectedSendfile(filter->cmd.get_stdin(), tf, 1, 4, 5, 4),
        });
        wassert(actual(writer->send_file_segment(tf, 1, 4)) == stream::SendResult());
        wassert(expected.check_not_called());
    }

    {
        stream::ExpectedSyscalls expected({
            new stream::ExpectedPoll(filter->cmd.get_stdout(), POLLIN, Fixture::get_timeout_ms(), POLLIN, 1),
            new stream::ExpectedPoll(*outfile, POLLOUT, Fixture::get_timeout_ms(), POLLOUT, 1),
            new stream::ExpectedSplice(filter->cmd.get_stdout(), *outfile, 131072, SPLICE_F_MORE | SPLICE_F_NONBLOCK, -1, EINVAL),
            new stream::ExpectedPoll(filter->cmd.get_stdout(), POLLIN, Fixture::get_timeout_ms(), POLLIN, 1),
            new stream::ExpectedRead(filter->cmd.get_stdout(), "estf", 32768, 4),
            new stream::ExpectedPoll(filter->cmd.get_stdout(), POLLIN, Fixture::get_timeout_ms(), POLLHUP, 1),
            new stream::ExpectedPoll(filter->cmd.get_stderr(), POLLIN, Fixture::get_timeout_ms(), POLLIN, 1),
            new stream::ExpectedRead(filter->cmd.get_stderr(), "FAIL", 256, 4),
            new stream::ExpectedPoll(filter->cmd.get_stderr(), POLLIN, Fixture::get_timeout_ms(), POLLHUP, 1),
            new stream::ExpectedPoll(*outfile, POLLOUT, Fixture::get_timeout_ms(), POLLOUT, 1),
            new stream::ExpectedWrite(*outfile, "estf", 4, 4),
        });
        auto flt = wcallchecked(writer->stream().stop_filter());
        wassert(actual(flt->size_stdin) == 4u);
        wassert(actual(flt->size_stdout) == 4u);
        wassert(actual(flt->errors.str()) == "FAIL");
        wassert(actual(flt->cmd.raw_returncode()) == 0);
        wassert(expected.check_not_called());
    }
});

}

}
