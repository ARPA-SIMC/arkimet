#include "tests.h"
#include "concrete.h"
#include "arki/utils/sys.h"

using namespace std;
using namespace arki;
using namespace arki::core;
using namespace arki::utils;
using namespace arki::tests;

namespace {

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

struct ConcreteTestFixture : public stream::StreamTestsFixture
{
    std::shared_ptr<core::NamedFileDescriptor> out;

    ConcreteTestFixture(std::shared_ptr<core::NamedFileDescriptor> out, int timeout_ms=-1)
        : out(out)
    {
        set_output(
                std::unique_ptr<arki::StreamOutput>(
                    new stream::ConcreteStreamOutputBase<stream::ConcreteTestingBackend>(out, timeout_ms)));
    }
};

class ConcreteFallbackTestFixture : public ConcreteTestFixture
{
    using ConcreteTestFixture::ConcreteTestFixture;

    stream::DisableSendfileSplice no_sendfile_and_splice;
};

struct ConcreteTimeoutTestFixture : public stream::StreamTestsFixture
{
    std::shared_ptr<core::NamedFileDescriptor> out;

    ConcreteTimeoutTestFixture(std::shared_ptr<core::NamedFileDescriptor> out, int timeout_ms=1000)
        : out(out)
    {
        set_output(
                std::unique_ptr<arki::StreamOutput>(
                    new stream::ConcreteStreamOutputBase<stream::ConcreteTestingBackend>(out, timeout_ms)));
    }
};

class ConcreteTimeoutFallbackTestFixture : public ConcreteTimeoutTestFixture
{
    using ConcreteTimeoutTestFixture::ConcreteTimeoutTestFixture;

    stream::DisableSendfileSplice no_sendfile_and_splice;
};


template<typename Fixture>
class Tests : public stream::ConcreteStreamTests
{
    using ConcreteStreamTests::ConcreteStreamTests;

    void register_tests() override;

    std::unique_ptr<stream::StreamTestsFixture> make_fixture() override
    {
        return std::unique_ptr<stream::StreamTestsFixture>(new CommonTestFixture);
    }

    std::unique_ptr<stream::StreamTestsFixture> make_concrete_fixture(std::shared_ptr<core::NamedFileDescriptor> out, int timeout_ms=-1) override
    {
        return std::unique_ptr<stream::StreamTestsFixture>(new Fixture(out, timeout_ms));
    }
};

Tests<ConcreteTestFixture> test1("arki_stream_concrete");
Tests<ConcreteFallbackTestFixture> test2("arki_stream_concrete_fallback");
Tests<ConcreteTimeoutTestFixture> test3("arki_stream_concrete_timeout");
Tests<ConcreteTimeoutFallbackTestFixture> test4("arki_stream_concrete_timeout_fallback");

template<typename Fixture>
void Tests<Fixture>::register_tests() {
ConcreteStreamTests::register_tests();

add_method("timeout_buffer", [this] {
    auto outfile = std::make_shared<sys::File>("/dev/null", O_WRONLY);
    auto writer = make_concrete_fixture(outfile, 1);

    // No timeout
    {
        stream::ExpectedSyscalls expected({
            new stream::ExpectedPoll(*outfile, POLLOUT, 1, POLLOUT, 1),
            new stream::ExpectedWrite(*outfile, "1234", 4),
        });
        wassert(actual(writer->send_buffer("1234", 4)) == stream::SendResult());
    }

    {
        stream::ExpectedSyscalls expected({
            new stream::ExpectedPoll(*outfile, POLLOUT, 1, POLLOUT, 1),
            new stream::ExpectedWrite(*outfile, "1234", 2),
            new stream::ExpectedPoll(*outfile, POLLOUT, 1, POLLERR, 1),
        });
        wassert(actual(writer->send_buffer("1234", 4)) == stream::SendResult(stream::SendResult::SEND_PIPE_EOF_DEST));
    }

    {
        stream::ExpectedSyscalls expected({
            new stream::ExpectedPoll(*outfile, POLLOUT, 1, POLLOUT, 1),
            new stream::ExpectedWrite(*outfile, "1234", 2),
            new stream::ExpectedPoll(*outfile, POLLOUT, 1, POLLHUP, 1),
        });
        wassert(actual(writer->send_buffer("1234", 4)) == stream::SendResult(stream::SendResult::SEND_PIPE_EOF_DEST));
    }

    {
        stream::ExpectedSyscalls expected({
            new stream::ExpectedPoll(*outfile, POLLOUT, 1, POLLOUT, 1),
            new stream::ExpectedWrite(*outfile, "1234", -1, EAGAIN),
            new stream::ExpectedPoll(*outfile, POLLOUT, 1, POLLOUT, 1),
            new stream::ExpectedWrite(*outfile, "1234", 4),
        });
        wassert(actual(writer->send_buffer("1234", 4)) == stream::SendResult());
    }

    {
        stream::ExpectedSyscalls expected({
            new stream::ExpectedPoll(*outfile, POLLOUT, 1, POLLOUT, 1),
            new stream::ExpectedWrite(*outfile, "1234", 2),
            new stream::ExpectedPoll(*outfile, POLLOUT, 1, POLLOUT, 1),
            new stream::ExpectedWrite(*outfile, "34", -1, EAGAIN),
            new stream::ExpectedPoll(*outfile, POLLOUT, 1, POLLOUT, 1),
            new stream::ExpectedWrite(*outfile, "34", 2),
        });
        wassert(actual(writer->send_buffer("1234", 4)) == stream::SendResult());
    }

    // Timeout
    {
        stream::ExpectedSyscalls expected({
            new stream::ExpectedPoll(*outfile, POLLOUT, 1, POLLOUT, 1),
            new stream::ExpectedWrite(*outfile, "1234", 2),
            new stream::ExpectedPoll(*outfile, POLLOUT, 1, 0, 0),
        });
        wassert_throws(stream::TimedOut, writer->send_buffer("1234", 4));
    }
});

add_method("timeout_line", [this] {
    auto outfile = std::make_shared<sys::File>("/dev/null", O_WRONLY);
    auto writer = make_concrete_fixture(outfile, 1);

    // No timeout
    {
        stream::ExpectedSyscalls expected({
            new stream::ExpectedPoll(*outfile, POLLOUT, 1, POLLOUT, 1),
            new stream::ExpectedWritev(*outfile, {"1234", "\n"}, 5),
        });
        wassert(actual(writer->send_line("1234", 4)) == stream::SendResult());
    }

    {
        stream::ExpectedSyscalls expected({
            new stream::ExpectedPoll(*outfile, POLLOUT, 1, POLLOUT, 1),
            new stream::ExpectedWritev(*outfile, {"1234", "\n"}, 3),
            new stream::ExpectedPoll(*outfile, POLLOUT, 1, POLLERR, 1),
        });
        wassert(actual(writer->send_line("1234", 4)) == stream::SendResult(stream::SendResult::SEND_PIPE_EOF_DEST));
    }

    {
        stream::ExpectedSyscalls expected({
            new stream::ExpectedPoll(*outfile, POLLOUT, 1, POLLOUT, 1),
            new stream::ExpectedWritev(*outfile, {"1234", "\n"}, 3),
            new stream::ExpectedPoll(*outfile, POLLOUT, 1, POLLHUP, 1),
        });
        wassert(actual(writer->send_line("1234", 4)) == stream::SendResult(stream::SendResult::SEND_PIPE_EOF_DEST));
    }

    {
        stream::ExpectedSyscalls expected({
            new stream::ExpectedPoll(*outfile, POLLOUT, 1, POLLOUT, 1),
            new stream::ExpectedWritev(*outfile, {"1234", "\n"}, -1, EAGAIN),
            new stream::ExpectedPoll(*outfile, POLLOUT, 1, POLLOUT, 1),
            new stream::ExpectedWritev(*outfile, {"1234", "\n"}, 5),
        });
        wassert(actual(writer->send_line("1234", 4)) == stream::SendResult());
    }

    {
        stream::ExpectedSyscalls expected({
            new stream::ExpectedPoll(*outfile, POLLOUT, 1, POLLOUT, 1),
            new stream::ExpectedWritev(*outfile, {"1234", "\n"}, 2),
            new stream::ExpectedPoll(*outfile, POLLOUT, 1, POLLOUT, 1),
            new stream::ExpectedWritev(*outfile, {"34", "\n"}, 3),
        });
        wassert(actual(writer->send_line("1234", 4)) == stream::SendResult());
    }

    {
        stream::ExpectedSyscalls expected({
            new stream::ExpectedPoll(*outfile, POLLOUT, 1, POLLOUT, 1),
            new stream::ExpectedWritev(*outfile, {"1234", "\n"}, 3),
            new stream::ExpectedPoll(*outfile, POLLOUT, 1, POLLOUT, 1),
            new stream::ExpectedWritev(*outfile, {"4", "\n"}, 2),
        });
        wassert(actual(writer->send_line("1234", 4)) == stream::SendResult());
    }

    {
        stream::ExpectedSyscalls expected({
            new stream::ExpectedPoll(*outfile, POLLOUT, 1, POLLOUT, 1),
            new stream::ExpectedWritev(*outfile, {"1234", "\n"}, 4),
            new stream::ExpectedPoll(*outfile, POLLOUT, 1, POLLOUT, 1),
            new stream::ExpectedWrite(*outfile, "\n", 1),
        });
        wassert(actual(writer->send_line("1234", 4)) == stream::SendResult());
    }

    {
        stream::ExpectedSyscalls expected({
            new stream::ExpectedPoll(*outfile, POLLOUT, 1, POLLOUT, 1),
            new stream::ExpectedWritev(*outfile, {"1234", "\n"}, 4),
            new stream::ExpectedPoll(*outfile, POLLOUT, 1, POLLOUT, 1),
            new stream::ExpectedWrite(*outfile, "\n", -1, EAGAIN),
            new stream::ExpectedPoll(*outfile, POLLOUT, 1, POLLOUT, 1),
            new stream::ExpectedWrite(*outfile, "\n", 1),
        });
        wassert(actual(writer->send_line("1234", 4)) == stream::SendResult());
    }

    // Timeout
    {
        stream::ExpectedSyscalls expected({
            new stream::ExpectedPoll(*outfile, POLLOUT, 1, POLLOUT, 1),
            new stream::ExpectedWritev(*outfile, {"1234", "\n"}, 2),
            new stream::ExpectedPoll(*outfile, POLLOUT, 1, 0, 0),
        });
        wassert_throws(stream::TimedOut, writer->send_line("1234", 4));
    }

    {
        stream::ExpectedSyscalls expected({
            new stream::ExpectedPoll(*outfile, POLLOUT, 1, POLLOUT, 1),
            new stream::ExpectedWritev(*outfile, {"1234", "\n"}, 4),
            new stream::ExpectedPoll(*outfile, POLLOUT, 1, 0, 0),
        });
        wassert_throws(stream::TimedOut, writer->send_line("1234", 4));
    }
});

add_method("timeout_file", [this] {
    auto outfile = std::make_shared<sys::File>("/dev/null", O_WRONLY);
    auto writer = make_concrete_fixture(outfile, 1);
    sys::Tempfile tf;
    tf.write_all_or_throw(std::string("testfile"));

    // No timeout
    {
        stream::ExpectedSyscalls expected({
            new stream::ExpectedPoll(*outfile, POLLOUT, 1, POLLOUT, 1),
            new stream::ExpectedSendfile(*outfile, tf, 1, 4, 3, 2),
            new stream::ExpectedPoll(*outfile, POLLOUT, 1, POLLOUT, 1),
            new stream::ExpectedSendfile(*outfile, tf, 3, 2, 5, 2),
        });
        wassert(actual(writer->send_file_segment(tf, 1, 4)) == stream::SendResult());
    }

    {
        stream::ExpectedSyscalls expected({
            new stream::ExpectedPoll(*outfile, POLLOUT, 1, POLLOUT, 1),
            new stream::ExpectedSendfile(*outfile, tf, 1, 4, 5, 4),
        });
        wassert(actual(writer->send_file_segment(tf, 1, 4)) == stream::SendResult());
    }

    {
        stream::ExpectedSyscalls expected({
            new stream::ExpectedPoll(*outfile, POLLOUT, 1, POLLOUT, 1),
            new stream::ExpectedSendfile(*outfile, tf, 1, 4, 1, -1, EINVAL),
            new stream::ExpectedPoll(*outfile, POLLOUT, 1, POLLOUT, 1),
            new stream::ExpectedPread(tf, 1, 4, "estf", 4),
            new stream::ExpectedWrite(*outfile, "estf", 4),
        });
        wassert(actual(writer->send_file_segment(tf, 1, 4)) == stream::SendResult());
    }

    {
        stream::ExpectedSyscalls expected({
            new stream::ExpectedPoll(*outfile, POLLOUT, 1, POLLOUT, 1),
            new stream::ExpectedSendfile(*outfile, tf, 1, 4, 1, -1, EINVAL),
            new stream::ExpectedPoll(*outfile, POLLOUT, 1, POLLOUT, 1),
            new stream::ExpectedPread(tf, 1, 4, "estf", 4),
            new stream::ExpectedWrite(*outfile, "estf", 2),
            new stream::ExpectedPoll(*outfile, POLLOUT, 1, POLLOUT, 1),
            new stream::ExpectedWrite(*outfile, "tf", 2),
        });
        wassert(actual(writer->send_file_segment(tf, 1, 4)) == stream::SendResult());
    }

    {
        stream::ExpectedSyscalls expected({
            new stream::ExpectedPoll(*outfile, POLLOUT, 1, POLLOUT, 1),
            new stream::ExpectedSendfile(*outfile, tf, 1, 4, 1, -1, EINVAL),
            new stream::ExpectedPoll(*outfile, POLLOUT, 1, POLLOUT, 1),
            new stream::ExpectedPread(tf, 1, 4, "est", 3),
            new stream::ExpectedWrite(*outfile, "est", 2),
            new stream::ExpectedPoll(*outfile, POLLOUT, 1, POLLOUT, 1),
            new stream::ExpectedWrite(*outfile, "t", 1),
            new stream::ExpectedPoll(*outfile, POLLOUT, 1, POLLOUT, 1),
            new stream::ExpectedPread(tf, 4, 1, "f", 1),
            new stream::ExpectedWrite(*outfile, "f", 1),
        });
        wassert(actual(writer->send_file_segment(tf, 1, 4)) == stream::SendResult());
    }

    // Timeout
    {
        stream::ExpectedSyscalls expected({
            new stream::ExpectedPoll(*outfile, POLLOUT, 1, POLLOUT, 1),
            new stream::ExpectedSendfile(*outfile, tf, 1, 4, 3, 2),
            new stream::ExpectedPoll(*outfile, POLLOUT, 1, 0, 0),
        });
        wassert_throws(stream::TimedOut, writer->send_file_segment(tf, 1, 4));
    }
//    {
//        sys::Tempfile tf1;
//        tf1.write_all_or_throw(std::string("testfile"));
//        tf1.lseek(0);
//        wassert_throws(stream::TimedOut, writer->send_from_pipe(tf1));
//    }
});

}

}
