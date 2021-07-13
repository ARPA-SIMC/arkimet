#include "tests.h"
#include "concrete.h"
#include "arki/utils/sys.h"
#include <system_error>
#include <sys/ioctl.h>

using namespace std;
using namespace arki;
using namespace arki::core;
using namespace arki::utils;
using namespace arki::tests;

namespace {

class BlockingSink
{
    int fds[2];
    int pipe_sz;
    std::shared_ptr<core::NamedFileDescriptor> outfd;
    std::vector<uint8_t> filler;

public:
    BlockingSink()
    {
        // Create a pipe with a known buffer
        // fds[1] will be the writing end
        if (pipe(fds) < 0)
            throw std::system_error(errno, std::system_category(), "cannot create a pipe");

        // Shrink the output buffer as much as possible, and get its shrunk size
        pipe_sz = fcntl(fds[1], F_SETPIPE_SZ, 0);
        if (pipe_sz < 0)
            throw std::system_error(errno, std::system_category(), "cannot shrink pipe size");
        // Writing more than pipe_sz will now block unless we read

        outfd = std::make_shared<sys::NamedFileDescriptor>(fds[1], "blocking test pipe");

        // Make the read end nonblocking
        int fl = fcntl(fds[0], F_GETFL);
        if (fl < 0)
            throw std::system_error(errno, std::system_category(), "cannot get file descriptor flags for read end of BlockingSink pipe");
        if (fcntl(fds[0], F_SETFL, fl | O_NONBLOCK) < 0)
            throw std::system_error(errno, std::system_category(), "cannot set nonblocking file descriptor flags for BlockingSink pipe");

        // Allocate the filler buffer
        filler.resize(pipe_sz);
    }
    ~BlockingSink()
    {
        close(fds[0]);
        outfd->close();
    }

    std::shared_ptr<core::NamedFileDescriptor> fd() { return outfd; }
    size_t pipe_size() const { return pipe_sz; }

    /**
     * Return how much data is currently present in the pipe buffer
     */
    size_t data_in_pipe()
    {
        int size;
        if (ioctl(fds[1], FIONREAD, &size) < 0)
            throw std::system_error(errno, std::system_category(), "cannot read the amount of data in pipe");
        return size;
    }

    /**
     * If size is positive, send size bytes to the pipe.
     *
     * If size is negative, send pipe_size - size bytes to the pipe.
     */
    void fill(int size)
    {
        if (size > 0)
            fd()->write_all_or_retry(filler.data(), size);
        else
            fd()->write_all_or_retry(filler.data(), pipe_size() + size);
    }

    /// Flush the pipe buffer until it's completely empty
    void empty_buffer()
    {
        // fprintf(stderr, "empty_buffer in pipe: %zd\n", data_in_pipe());
        char buf[4096];
        while (true)
        {
            ssize_t res = ::read(fds[0], buf, 4096);
            // fprintf(stderr, "             read: %d\n", (int)res);
            if (res == -1)
            {
                if (errno == EAGAIN)
                    break;
                throw std::system_error(errno, std::system_category(), "cannot read from read end of BlockingSink pipe");
            }
        }
    }
};


struct WriteTest
{
    StreamOutput& writer;
    BlockingSink& sink;
    std::vector<size_t> cb_log;
    size_t total_written = 0;

    WriteTest(StreamOutput& writer, BlockingSink& sink, int prefill)
        : writer(writer), sink(sink)
    {
        if (prefill)
            sink.fill(prefill);

        writer.set_progress_callback([&](size_t written) {
            // fprintf(stderr, "write_test written: %zd\n", written);
            sink.empty_buffer();
            total_written += written;
            cb_log.push_back(written);
        });
    }
    ~WriteTest()
    {
        sink.empty_buffer();
        writer.set_progress_callback(nullptr);
    }
};


struct CommonTestFixture : public stream::StreamTestsFixture
{
    std::shared_ptr<sys::Tempfile> tf;

    CommonTestFixture()
        : tf(std::make_shared<sys::Tempfile>())
    {
        set_output(StreamOutput::create(tf, 1000));
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

    ConcreteTestFixture(std::shared_ptr<core::NamedFileDescriptor> out, int timeout_ms=1000)
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

template<typename Fixture>
struct Tests : public stream::ConcreteStreamTests
{
    using ConcreteStreamTests::ConcreteStreamTests;

    std::unique_ptr<stream::StreamTestsFixture> make_fixture() override
    {
        return std::unique_ptr<stream::StreamTestsFixture>(new CommonTestFixture);
    }

    std::unique_ptr<stream::StreamTestsFixture> make_concrete_fixture(std::shared_ptr<core::NamedFileDescriptor> out, int timeout_ms=1000) override
    {
        return std::unique_ptr<stream::StreamTestsFixture>(new Fixture(out, timeout_ms));
    }
};

class NormalTests : public Tests<ConcreteTestFixture>
{
    using Tests<ConcreteTestFixture>::Tests;

    void register_tests() override;
};

class FallbackTests : public Tests<ConcreteTestFixture>
{
    using Tests<ConcreteTestFixture>::Tests;
};

NormalTests test1("arki_stream_concrete_timeout");
FallbackTests test2("arki_stream_concrete_timeout_fallback");

void NormalTests::register_tests() {
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

add_method("concrete_timeout_send_file_segment", [] {
    // TODO: redo with mock syscalls
    BlockingSink sink;
    auto writer = StreamOutput::create(sink.fd(), 1000);

    sys::Tempfile tf1;
    tf1.write_all_or_throw(std::string("foobarbaz"));
    tf1.lseek(sink.pipe_size());
    tf1.write_all_or_throw(std::string("zabraboof"));

    {
        WriteTest t(*writer, sink, -3);
        wassert(actual(writer->send_file_segment(tf1, 0, 6)) == stream::SendResult());
        wassert(actual(t.total_written) == 6u);
        // wassert(actual(cb_log) == std::vector<size_t>{3, 3});
    }

    {
        WriteTest t(*writer, sink, -6);
        wassert(actual(writer->send_file_segment(tf1, 0, 6)) == stream::SendResult());
        wassert(actual(t.total_written) == 6u);
        wassert(actual(t.cb_log) == std::vector<size_t>{6});
    }

    {
        WriteTest t(*writer, sink, -7);
        wassert(actual(writer->send_file_segment(tf1, 0, 6)) == stream::SendResult());
        wassert(actual(t.total_written) == 6u);
        // wassert(actual(cb_log) == std::vector<size_t>{0, 7, 0, 7, 7});
    }

    {
        WriteTest t(*writer, sink, 0);
        wassert(actual(writer->send_file_segment(tf1, 0, sink.pipe_size() + 9)) == stream::SendResult());
        wassert(actual(t.total_written) == sink.pipe_size() + 9);
        // wassert(actual(t.cb_log) == std::vector<size_t>{sink.pipe_size(), 9});
    }

    {
        WriteTest t(*writer, sink, 1);
        wassert(actual(writer->send_file_segment(tf1, 0, sink.pipe_size() + 9)) == stream::SendResult());
        wassert(actual(t.total_written) == sink.pipe_size() + 9);
        // wassert(actual(t.cb_log) == std::vector<size_t>{sink.pipe_size(), 9});
    }
});

}

}
