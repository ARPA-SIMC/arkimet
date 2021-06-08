#include "tests.h"
#include "concrete-timeout.h"
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

    ConcreteTestFixture(std::shared_ptr<core::NamedFileDescriptor> out)
        : out(out)
    {
        set_output(
                std::unique_ptr<arki::StreamOutput>(
                    new stream::ConcreteTimeoutStreamOutputBase<stream::ConcreteTestingBackend>(out, 1000)));
    }
};

class ConcreteFallbackTestFixture : public ConcreteTestFixture
{
    using ConcreteTestFixture::ConcreteTestFixture;

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

    std::unique_ptr<stream::StreamTestsFixture> make_concrete_fixture(std::shared_ptr<core::NamedFileDescriptor> out) override
    {
        return std::unique_ptr<stream::StreamTestsFixture>(new Fixture(out));
    }
};

Tests<ConcreteTestFixture> test1("arki_stream_concrete_timeout");
Tests<ConcreteFallbackTestFixture> test2("arki_stream_concrete_timeout_fallback");

template<typename Fixture>
void Tests<Fixture>::register_tests() {
ConcreteStreamTests::register_tests();

add_method("concrete_timeout1", [] {
    BlockingSink sink;
    std::vector<uint8_t> filler(sink.pipe_size());

    auto writer = StreamOutput::create(sink.fd(), 1);

    // This won't block
    writer->send_buffer(filler.data(), filler.size());
    // This times out
    wassert_throws(stream::TimedOut, writer->send_buffer(" ", 1));

    sink.empty_buffer();
    wassert_throws(stream::TimedOut, writer->send_line(filler.data(), filler.size()));

    sink.empty_buffer();
    writer->send_buffer(filler.data(), filler.size());
    {
        sys::Tempfile tf1;
        tf1.write_all_or_throw(std::string("testfile"));
        wassert_throws(stream::TimedOut, writer->send_file_segment(tf1, 1, 6));
    }

    sink.empty_buffer();
    writer->send_buffer(filler.data(), filler.size());
    {
        sys::Tempfile tf1;
        tf1.write_all_or_throw(std::string("testfile"));
        tf1.lseek(0);
        wassert_throws(stream::TimedOut, writer->send_from_pipe(tf1));
    }
});

add_method("concrete_timeout_send_buffer", [] {
    BlockingSink sink;
    auto writer = StreamOutput::create(sink.fd(), 100);

    {
        WriteTest t(*writer, sink, -3);
        wassert(actual(writer->send_buffer("foobar", 6)) == 6u);
        wassert(actual(t.total_written) == 6u);
        //wassert(actual(cb_log) == std::vector<size_t>{3, 3});
    }

    {
        WriteTest t(*writer, sink, -6);
        wassert(actual(writer->send_buffer("foobar", 6)) == 6u);
        wassert(actual(t.total_written) == 6u);
        //wassert(actual(cb_log) == std::vector<size_t>{3, 3, 6});
    }

    {
        std::vector<uint8_t> buf(sink.pipe_size() * 2);
        WriteTest t(*writer, sink, 0);
        wassert(actual(writer->send_buffer(buf.data(), sink.pipe_size() + 6)) == sink.pipe_size() + 6);
        wassert(actual(t.total_written) == sink.pipe_size() + 6);
        //wassert(actual(t.cb_log) == std::vector<size_t>{sink.pipe_size(), 6});
    }

    {
        std::vector<uint8_t> buf(sink.pipe_size() * 2);
        WriteTest t(*writer, sink, 1);
        wassert(actual(writer->send_buffer(buf.data(), sink.pipe_size() + 6)) == sink.pipe_size() + 6);
        wassert(actual(t.total_written) == sink.pipe_size() + 6);
        //wassert(actual(t.cb_log) == std::vector<size_t>{sink.pipe_size() - 1, 7});
    }
});

add_method("concrete_timeout_send_line", [] {
    BlockingSink sink;
    auto writer = StreamOutput::create(sink.fd(), 1000);

    {
        WriteTest t(*writer, sink, -3);
        wassert(actual(writer->send_line("foobar", 6)) == 7u);
        wassert(actual(t.total_written) == 7u);
        wassert(actual(t.cb_log) == std::vector<size_t>{0, 7});
    }

    {
        WriteTest t(*writer, sink, -6);
        wassert(actual(writer->send_line("foobar", 6)) == 7u);
        wassert(actual(t.total_written) == 7u);
        //wassert(actual(cb_log) == std::vector<size_t>{0, 7, 0, 7});
    }

    {
        WriteTest t(*writer, sink, -7);
        wassert(actual(writer->send_line("foobar", 6)) == 7u);
        wassert(actual(t.total_written) == 7u);
        //wassert(actual(cb_log) == std::vector<size_t>{0, 7, 0, 7, 7});
    }

    {
        WriteTest t(*writer, sink, 0);
        std::vector<uint8_t> buf(sink.pipe_size() * 2);
        wassert(actual(writer->send_line(buf.data(), sink.pipe_size() + 6)) == sink.pipe_size() + 7);
        wassert(actual(t.total_written) == sink.pipe_size() + 7);
        //wassert(actual(cb_log) == std::vector<size_t>{0, 7, 0, 7, 7});
    }

    {
        WriteTest t(*writer, sink, 1);
        std::vector<uint8_t> buf(sink.pipe_size() * 2);
        wassert(actual(writer->send_line(buf.data(), sink.pipe_size() + 6)) == sink.pipe_size() + 7);
        wassert(actual(t.total_written) == sink.pipe_size() + 7);
        //wassert(actual(cb_log) == std::vector<size_t>{0, 7, 0, 7, 7});
    }
});

add_method("concrete_timeout_send_file_segment", [] {
    BlockingSink sink;
    auto writer = StreamOutput::create(sink.fd(), 1000);

    sys::Tempfile tf1;
    tf1.write_all_or_throw(std::string("foobarbaz"));
    tf1.lseek(sink.pipe_size());
    tf1.write_all_or_throw(std::string("zabraboof"));

    {
        WriteTest t(*writer, sink, -3);
        wassert(actual(writer->send_file_segment(tf1, 0, 6)) == 6u);
        wassert(actual(t.total_written) == 6u);
        // wassert(actual(cb_log) == std::vector<size_t>{3, 3});
    }

    {
        WriteTest t(*writer, sink, -6);
        wassert(actual(writer->send_file_segment(tf1, 0, 6)) == 6u);
        wassert(actual(t.total_written) == 6u);
        wassert(actual(t.cb_log) == std::vector<size_t>{6});
    }

    {
        WriteTest t(*writer, sink, -7);
        wassert(actual(writer->send_file_segment(tf1, 0, 6)) == 6u);
        wassert(actual(t.total_written) == 6u);
        // wassert(actual(cb_log) == std::vector<size_t>{0, 7, 0, 7, 7});
    }

    {
        WriteTest t(*writer, sink, 0);
        wassert(actual(writer->send_file_segment(tf1, 0, sink.pipe_size() + 9)) == sink.pipe_size() + 9);
        wassert(actual(t.total_written) == sink.pipe_size() + 9);
        // wassert(actual(t.cb_log) == std::vector<size_t>{sink.pipe_size(), 9});
    }

    {
        WriteTest t(*writer, sink, 1);
        wassert(actual(writer->send_file_segment(tf1, 0, sink.pipe_size() + 9)) == sink.pipe_size() + 9);
        wassert(actual(t.total_written) == sink.pipe_size() + 9);
        // wassert(actual(t.cb_log) == std::vector<size_t>{sink.pipe_size(), 9});
    }
});

add_method("concrete_timeout_send_from_pipe", [] {
    BlockingSink sink;
    auto writer = StreamOutput::create(sink.fd(), 1000);

    sys::Tempfile tf1;
    tf1.write_all_or_throw(std::string("foobarbaz"));
    tf1.lseek(sink.pipe_size());
    tf1.write_all_or_throw(std::string("zabraboof"));

    {
        WriteTest t(*writer, sink, -3);
        tf1.lseek(sink.pipe_size());
        wassert(actual(writer->send_from_pipe(tf1)) == stream::SendResult(9u, stream::SendResult::SEND_PIPE_EOF_SOURCE));
        wassert(actual(t.total_written) == 9u);
        // wassert(actual(cb_log) == std::vector<size_t>{3, 3});
    }

    {
        WriteTest t(*writer, sink, -9);
        tf1.lseek(sink.pipe_size());
        wassert(actual(writer->send_from_pipe(tf1)) == stream::SendResult(9u, stream::SendResult::SEND_PIPE_EOF_SOURCE));
        wassert(actual(t.total_written) == 9u);
        // wassert(actual(t.cb_log) == std::vector<size_t>{9});
    }

    {
        WriteTest t(*writer, sink, -10);
        tf1.lseek(sink.pipe_size());
        wassert(actual(writer->send_from_pipe(tf1)) == stream::SendResult(9u, stream::SendResult::SEND_PIPE_EOF_SOURCE));
        wassert(actual(t.total_written) == 9u);
        // wassert(actual(t.cb_log) == std::vector<size_t>{9});
    }

    {
        WriteTest t(*writer, sink, 0);
        tf1.lseek(0);
        wassert(actual(writer->send_from_pipe(tf1)) == stream::SendResult(sink.pipe_size() + 9, stream::SendResult::SEND_PIPE_EOF_SOURCE));
        wassert(actual(t.total_written) == sink.pipe_size() + 9);
        // wassert(actual(t.cb_log) == std::vector<size_t>{sink.pipe_size(), 9});
    }

    {
        WriteTest t(*writer, sink, 1);
        tf1.lseek(0);
        wassert(actual(writer->send_from_pipe(tf1)) == stream::SendResult(sink.pipe_size() + 9, stream::SendResult::SEND_PIPE_EOF_SOURCE));
        wassert(actual(t.total_written) == sink.pipe_size() + 9);
        // wassert(actual(t.cb_log) == std::vector<size_t>{sink.pipe_size(), 9});
    }
});


}

}
