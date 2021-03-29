#include "tests.h"
#include "base.h"
#include "arki/utils/sys.h"
#include "arki/utils/subprocess.h"
#include <numeric>
#include <unistd.h>
#include <fcntl.h>
#include <sys/ioctl.h>

using namespace arki::utils;

namespace arki {
namespace stream {

BlockingSink::BlockingSink()
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

    outfd.reset(new sys::NamedFileDescriptor(fds[1], "blocking test pipe"));

    // Make the read end nonblocking
    int fl = fcntl(fds[0], F_GETFL);
    if (fl < 0)
        throw std::system_error(errno, std::system_category(), "cannot get file descriptor flags for read end of BlockingSink pipe");
    if (fcntl(fds[0], F_SETFL, fl | O_NONBLOCK) < 0)
        throw std::system_error(errno, std::system_category(), "cannot set nonblocking file descriptor flags for BlockingSink pipe");

    // Allocate the filler buffer
    filler.resize(pipe_sz);
}

BlockingSink::~BlockingSink()
{
    close(fds[0]);
    close(fds[1]);
}

size_t BlockingSink::data_in_pipe()
{
    int size;
    if (ioctl(fds[1], FIONREAD, &size) < 0)
        throw std::system_error(errno, std::system_category(), "cannot read the amount of data in pipe");
    return size;
}

void BlockingSink::fill(int size)
{
    if (size > 0)
        fd().write_all_or_retry(filler.data(), size);
    else
        fd().write_all_or_retry(filler.data(), pipe_size() + size);
}

void BlockingSink::empty_buffer()
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


std::string StreamTestsFixture::streamed_contents()
{
    throw std::runtime_error("streamed_contents is not available in this type of fixture");
}

void StreamTestsFixture::set_output(std::unique_ptr<StreamOutput>&& output)
{
    this->output = std::move(output);

    this->output->set_progress_callback([&](size_t written) {
        cb_log.push_back(written);
    });
}

stream::SendResult StreamTestsFixture::send_line(const void* data, size_t size)
{
    cb_log.clear();
    return output->send_line(data, size);
}

stream::SendResult StreamTestsFixture::send_file_segment(arki::core::NamedFileDescriptor& fd, off_t offset, size_t size)
{
    cb_log.clear();
    return output->send_file_segment(fd, offset, size);
}

stream::SendResult StreamTestsFixture::send_buffer(const void* data, size_t size)
{
    cb_log.clear();
    return output->send_buffer(data, size);
}

stream::SendResult StreamTestsFixture::send_from_pipe(int fd)
{
    cb_log.clear();
    return output->send_from_pipe(fd);
}


WriteTest::WriteTest(StreamOutput& writer, BlockingSink& sink, int prefill)
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

WriteTest::~WriteTest()
{
    sink.empty_buffer();
    writer.set_progress_callback(nullptr);
}


namespace {

class PipeSource : public subprocess::Child
{
protected:
    std::string data;

    int main() noexcept override
    {
        sys::FileDescriptor out(1);

        for (size_t pos = 0; pos < data.size(); )
            pos += out.write(data.data() + pos, data.size() - pos);
        return 0;
    }

public:
    PipeSource(const std::string& data)
        : data(data)
    {
        set_stdout(subprocess::Redirect::PIPE);
        fork();
    }
    ~PipeSource()
    {
        terminate();
        wait();
    }
};

}


void StreamTests::register_tests() {
using namespace arki::tests;

add_method("send_line", [&] {
    auto f = make_fixture();

    wassert(actual(f->send_line("testline", 8)) == stream::SendResult(9));
    wassert(actual(f->cb_log) == std::vector<size_t>{9u});
    wassert(actual(f->send_line("testline1", 9)) == stream::SendResult(10));
    wassert(actual(f->cb_log) == std::vector<size_t>{10u});

    wassert(actual(f->streamed_contents()) == "testline\ntestline1\n");
});

add_method("send_buffer", [&] {
    auto f = make_fixture();

    wassert(actual(f->send_buffer("testbuf", 7)) == stream::SendResult(7u, 0u));
    wassert(actual(f->cb_log) == std::vector<size_t>{7u});
    wassert(actual(f->send_buffer("testbuf", 4)) == stream::SendResult(4u, 0u));
    wassert(actual(f->cb_log) == std::vector<size_t>{4u});

    wassert(actual(f->streamed_contents()) == "testbuftest");
});

add_method("send_file_segment", [&] {
    auto f = make_fixture();

    sys::Tempfile tf1;
    tf1.write_all_or_throw(std::string("testfile"));

    wassert(actual(f->send_file_segment(tf1, 1, 6)) == stream::SendResult(6u, 0u));
    wassert(actual(f->cb_log) == std::vector<size_t>{6});
    wassert(actual(f->send_file_segment(tf1, 5, 1)) == stream::SendResult(1u, 0u));
    wassert(actual(f->cb_log) == std::vector<size_t>{1});
    wassert(actual(f->send_file_segment(tf1, 0, 4)) == stream::SendResult(4u, 0u));
    wassert(actual(f->cb_log) == std::vector<size_t>{4});

    wassert(actual(f->streamed_contents()) == "estfilitest");
});

add_method("send_from_pipe_read", [&] {
    auto f = make_fixture();

    // Neither the source not the target is a pipe, so this won't trigger splice
    sys::Tempfile tf1;
    tf1.write_all_or_throw(std::string("testfile"));
    tf1.lseek(1);

    wassert(actual(f->send_from_pipe(tf1)) == stream::SendResult(7u, stream::SendResult::SEND_PIPE_EOF_SOURCE));
    wassert(actual(f->cb_log) == std::vector<size_t>{7});

    wassert(actual(f->streamed_contents()) == "estfile");
});

add_method("send_from_pipe_splice", [&] {
    auto f = make_fixture();

    // Source is a pipe and it should trigger splice
    PipeSource child("testfile");
    wassert(actual(f->send_from_pipe(child.get_stdout())) == stream::SendResult(8u, stream::SendResult::SEND_PIPE_EOF_SOURCE));
    wassert(actual(f->cb_log) == std::vector<size_t>{8});

    wassert(actual(f->streamed_contents()) == "testfile");
});

add_method("data_start_send_line", [&] {
    auto f = make_fixture();
    f->set_data_start_callback([](StreamOutput& out) { return out.send_buffer("start", 5); });

    wassert(actual(f->send_line("testline", 8)) == stream::SendResult(9u + 5u, 0u));
    wassert(actual(f->cb_log) == std::vector<size_t>{5, 9});
    wassert(actual(f->send_line("testline1", 9)) == stream::SendResult(10u, 0u));
    wassert(actual(f->cb_log) == std::vector<size_t>{10});

    wassert(actual(f->streamed_contents()) == "starttestline\ntestline1\n");

    bool fired = false;
    f->set_data_start_callback([&](StreamOutput& out) { fired=true; return SendResult(); });
    wassert(actual(f->send_line("testline", 0)) == stream::SendResult(0u, 0u));
    wassert(actual(f->cb_log) == std::vector<size_t>());
    wassert_false(fired);
});

add_method("data_start_send_buffer", [&] {
    auto f = make_fixture();
    f->set_data_start_callback([](StreamOutput& out) { return out.send_line("start", 5); });

    wassert(actual(f->send_buffer("testbuf", 7)) == stream::SendResult(7u + 6u, 0u));
    wassert(actual(f->cb_log) == std::vector<size_t>{6, 7});
    wassert(actual(f->send_buffer("testbuf", 4)) == stream::SendResult(4u, 0u));
    wassert(actual(f->cb_log) == std::vector<size_t>{4});

    wassert(actual(f->streamed_contents()) == "start\ntestbuftest");

    bool fired = false;
    f->set_data_start_callback([&](StreamOutput& out) { fired=true; return 0; });
    wassert(actual(f->send_buffer("testline", 0)) == stream::SendResult(0u, 0u));
    wassert(actual(f->cb_log) == std::vector<size_t>());
    wassert_false(fired);
});

add_method("data_start_send_file_segment", [&] {
    auto f = make_fixture();
    f->set_data_start_callback([](StreamOutput& out) {
        sys::Tempfile t;
        t.write_all_or_throw("start", 5);
        t.lseek(0);
        auto res = out.send_from_pipe(t);
        res.flags = res.flags & ~stream::SendResult::SEND_PIPE_EOF_SOURCE;
        return res;
    });

    sys::Tempfile tf1;
    tf1.write_all_or_throw(std::string("testfile"));

    wassert(actual(f->send_file_segment(tf1, 1, 6)) == stream::SendResult(6u + 5u, 0u));
    wassert(actual(f->cb_log) == std::vector<size_t>{5, 6});
    wassert(actual(f->send_file_segment(tf1, 5, 1)) == stream::SendResult(1u, 0u));
    wassert(actual(f->cb_log) == std::vector<size_t>{1});
    wassert(actual(f->send_file_segment(tf1, 0, 4)) == stream::SendResult(4u, 0u));
    wassert(actual(f->cb_log) == std::vector<size_t>{4});
    // Send past the end of file
    wassert(actual(f->send_file_segment(tf1, 6, 4)) == stream::SendResult(2u, stream::SendResult::SEND_PIPE_EOF_SOURCE));
    wassert(actual(f->cb_log) == std::vector<size_t>{2});

    wassert(actual(f->streamed_contents()) == "startestfilitestle");

    bool fired = false;
    f->set_data_start_callback([&](StreamOutput& out) { fired=true; return 0; });
    wassert(actual(f->send_file_segment(tf1, 3, 0)) == stream::SendResult(0u, 0u));
    wassert(actual(f->cb_log) == std::vector<size_t>());
    wassert_false(fired);

    fired = false;
    f->set_data_start_callback([&](StreamOutput& out) { fired=true; return 0u; });
    wassert(actual(f->send_file_segment(tf1, 8, 1)) == stream::SendResult(0u, stream::SendResult::SEND_PIPE_EOF_SOURCE));
    wassert(actual(f->cb_log) == std::vector<size_t>());
    wassert_false(fired);
});

add_method("data_start_send_from_pipe_read", [&] {
    auto f = make_fixture();
    f->set_data_start_callback([](StreamOutput& out) {
        sys::Tempfile t;
        t.write_all_or_throw("start", 5);
        return out.send_file_segment(t, 1, 3);
    });

    // Neither the source not the target is a pipe, so this won't trigger splice
    sys::Tempfile tf1;
    tf1.write_all_or_throw(std::string("testfile"));
    tf1.lseek(1);
    wassert(actual(f->send_from_pipe(tf1)) == stream::SendResult(7u + 3u, stream::SendResult::SEND_PIPE_EOF_SOURCE));
    wassert(actual(f->cb_log) == std::vector<size_t>{3, 7});

    tf1.lseek(2);
    wassert(actual(f->send_from_pipe(tf1)) == stream::SendResult(6u, stream::SendResult::SEND_PIPE_EOF_SOURCE));
    wassert(actual(f->cb_log) == std::vector<size_t>{6});

    // Pipe from end of file
    tf1.lseek(0, SEEK_END);
    wassert(actual(f->send_from_pipe(tf1)) == stream::SendResult(0u, stream::SendResult::SEND_PIPE_EOF_SOURCE));
    wassert(actual(f->cb_log) == std::vector<size_t>());

    wassert(actual(f->streamed_contents()) == "tarestfilestfile");

    bool fired = false;
    f->set_data_start_callback([&](StreamOutput& out) { fired=true; return 0; });
    tf1.lseek(0, SEEK_END);
    wassert(actual(f->send_from_pipe(tf1)) == stream::SendResult(0u, stream::SendResult::SEND_PIPE_EOF_SOURCE));
    wassert(actual(f->cb_log) == std::vector<size_t>());
    wassert_false(fired);
});

add_method("data_start_send_from_pipe_splice", [&] {
    auto f = make_fixture();
    f->set_data_start_callback([](StreamOutput& out) {
        sys::Tempfile t;
        t.write_all_or_throw("start", 5);
        return out.send_file_segment(t, 1, 3);
    });

    // Source is a pipe and it should trigger splice
    PipeSource child("testfile");
    wassert(actual(f->send_from_pipe(child.get_stdout())) == stream::SendResult(8u + 3u, stream::SendResult::SEND_PIPE_EOF_SOURCE));
    wassert(actual(f->cb_log) == std::vector<size_t>{3, 8});
    wassert(actual(f->send_from_pipe(child.get_stdout())) == stream::SendResult(0u, stream::SendResult::SEND_PIPE_EOF_SOURCE));
    wassert(actual(f->cb_log) == std::vector<size_t>());

    // Pipe from an empty pipe
    PipeSource child1("");
    wassert(actual(f->send_from_pipe(child1.get_stdout())) == stream::SendResult(0u, stream::SendResult::SEND_PIPE_EOF_SOURCE));
    wassert(actual(f->cb_log) == std::vector<size_t>());

    wassert(actual(f->streamed_contents()) == "tartestfile");

    bool fired = false;
    f->set_data_start_callback([&](StreamOutput& out) { fired=true; return 0; });
    PipeSource child2("");
    wassert(actual(f->send_from_pipe(child2.get_stdout())) == stream::SendResult(0u, stream::SendResult::SEND_PIPE_EOF_SOURCE));
    wassert(actual(f->cb_log) == std::vector<size_t>());
    wassert_false(fired);
});

add_method("large_send_line", [&] {
    auto f = make_fixture();
    std::vector<uint8_t> buf(stream::TransferBuffer::size * 3);

    wassert(actual(f->send_line(buf.data(), buf.size())) == stream::SendResult(buf.size() + 1, 0u));
    wassert(actual(f->cb_log) == std::vector<size_t>{buf.size() + 1});

    wassert(actual(f->streamed_contents().size()) == buf.size() + 1);
});

add_method("large_send_buffer", [&] {
    auto f = make_fixture();
    std::vector<uint8_t> buf(stream::TransferBuffer::size * 3);

    wassert(actual(f->send_buffer(buf.data(), buf.size())) == stream::SendResult(buf.size(), 0u));
    wassert(actual(f->cb_log) == std::vector<size_t>{buf.size()});

    wassert(actual(f->streamed_contents().size()) == buf.size());
});

add_method("large_send_file_segment", [&] {
    auto f = make_fixture();
    std::vector<uint8_t> buf(stream::TransferBuffer::size * 3);

    sys::Tempfile tf1;
    tf1.write_all_or_retry(buf);

    wassert(actual(f->send_file_segment(tf1, 0, buf.size())) == stream::SendResult(buf.size(), 0u));
    wassert(actual(std::accumulate(f->cb_log.begin(), f->cb_log.end(), 0u)) == buf.size());

    wassert(actual(f->streamed_contents().size()) == buf.size());
});

add_method("large_send_from_pipe_read", [&] {
    auto f = make_fixture();
    std::vector<uint8_t> buf(stream::TransferBuffer::size * 3);

    // Neither the source not the target is a pipe, so this won't trigger splice
    sys::Tempfile tf1;
    tf1.write_all_or_retry(buf);
    tf1.lseek(0);

    wassert(actual(f->send_from_pipe(tf1)) == stream::SendResult(buf.size(), stream::SendResult::SEND_PIPE_EOF_SOURCE));
    wassert(actual(std::accumulate(f->cb_log.begin(), f->cb_log.end(), 0u)) == buf.size());

    wassert(actual(f->streamed_contents().size()) == buf.size());
});

add_method("large_send_from_pipe_splice", [&] {
    auto f = make_fixture();
    size_t size = stream::TransferBuffer::size * 3;

    // Source is a pipe and it should trigger splice
    PipeSource child(std::string(size, 0));
    wassert(actual(f->send_from_pipe(child.get_stdout())) == stream::SendResult(size, stream::SendResult::SEND_PIPE_EOF_SOURCE));
    wassert(actual(std::accumulate(f->cb_log.begin(), f->cb_log.end(), 0u)) == size);

    wassert(actual(f->streamed_contents().size()) == size);
});

add_method("data_start_large_send_from_pipe_splice", [&] {
    auto f = make_fixture();
    f->set_data_start_callback([](StreamOutput& out) {
        return out.send_line("foo", 3);
    });

    size_t size = stream::TransferBuffer::size * 3;

    // Source is a pipe and it should trigger splice
    PipeSource child(std::string(size, ' '));
    wassert(actual(f->send_from_pipe(child.get_stdout())) == stream::SendResult(size + 4, stream::SendResult::SEND_PIPE_EOF_SOURCE));
    wassert(actual(std::accumulate(f->cb_log.begin(), f->cb_log.end(), 0u)) == size + 4);

    wassert(actual(f->streamed_contents()).startswith("foo\n        "));
});

}

namespace {

struct ClosedPipe
{
    int pipefds[2];
    sys::NamedFileDescriptor fd;

    ClosedPipe()
        : fd(-1, "write end of pipe")
    {
        if (pipe(pipefds) < 0)
            throw std::system_error(errno, std::system_category(), "cannot create new pipe");
        // Close read end of the pipe
        close(pipefds[0]);

        fd = sys::NamedFileDescriptor(pipefds[1], "write end of pipe");
    }
};

}

void ConcreteStreamTests::register_tests() {
using namespace arki::tests;
StreamTests::register_tests();

add_method("closed_pipe_send_buffer", [&] {
    ClosedPipe cp;
    auto f = make_concrete_fixture(cp.fd);
    wassert(actual(f->send_buffer("A", 1)) == stream::SendResult(0, stream::SendResult::SEND_PIPE_EOF_DEST));
});

add_method("closed_pipe_send_line", [&] {
    ClosedPipe cp;
    auto f = make_concrete_fixture(cp.fd);
    wassert(actual(f->send_line("A", 1)) == stream::SendResult(0, stream::SendResult::SEND_PIPE_EOF_DEST));
});

add_method("closed_pipe_send_file_segment", [&] {
    ClosedPipe cp;
    auto f = make_concrete_fixture(cp.fd);

    sys::Tempfile tf;
    tf.write_all_or_retry("test", 4);
    wassert(actual(f->send_file_segment(tf, 1, 1)) == stream::SendResult(0, stream::SendResult::SEND_PIPE_EOF_DEST));
});

add_method("closed_pipe_send_pipe_read", [&] {
    ClosedPipe cp;
    auto f = make_concrete_fixture(cp.fd);

    sys::Tempfile tf;
    tf.write_all_or_retry("test", 4);
    tf.lseek(0);
    wassert(actual(f->send_from_pipe(tf)) == stream::SendResult(0, stream::SendResult::SEND_PIPE_EOF_DEST));
});

add_method("closed_pipe_send_pipe_splice", [&] {
    ClosedPipe cp;
    auto f = make_concrete_fixture(cp.fd);
    PipeSource child("test");
    wassert(actual(f->send_from_pipe(child.get_stdout())) == stream::SendResult(0, stream::SendResult::SEND_PIPE_EOF_DEST));
});

}

}
}
