#include "tests.h"
#include "base.h"
#include "concrete.h"
#include "arki/utils/sys.h"
#include "arki/utils/subprocess.h"
#include <numeric>
#include <system_error>
#include <cstring>
#include <cerrno>
#include <unistd.h>
#include <fcntl.h>
#include <sys/ioctl.h>
#include <poll.h>

using namespace arki::utils;

static std::ostream& operator<<(std::ostream& out, const std::pair<size_t, size_t>& p)
{
    return out << '[' << p.first << "→" << p.second << ']';
}

static std::ostream& operator<<(std::ostream& out, const std::pair<size_t, arki::stream::SendResult>& p)
{
    return out << '[' << p.first << " " << p.second << ']';
}

namespace arki {
namespace stream {

std::string StreamTestsFixture::streamed_contents()
{
    throw std::runtime_error("streamed_contents is not available in this type of fixture");
}

void StreamTestsFixture::set_output(std::unique_ptr<StreamOutput>&& output)
{
    this->output = std::move(output);
}

stream::SendResult StreamTestsFixture::send_line(const void* data, size_t size)
{
    return output->send_line(data, size);
}

stream::SendResult StreamTestsFixture::send_file_segment(arki::core::NamedFileDescriptor& fd, off_t offset, size_t size)
{
    return output->send_file_segment(fd, offset, size);
}

stream::SendResult StreamTestsFixture::send_buffer(const void* data, size_t size)
{
    return output->send_buffer(data, size);
}

std::pair<size_t, stream::SendResult> StreamTestsFixture::send_from_pipe(int fd)
{
    return output->send_from_pipe(fd);
}

MockConcreteSyscalls::MockConcreteSyscalls()
    : orig_read(ConcreteTestingBackend::read),
      orig_write(ConcreteTestingBackend::write),
      orig_writev(ConcreteTestingBackend::writev),
      orig_sendfile(ConcreteTestingBackend::sendfile),
      orig_splice(ConcreteTestingBackend::splice),
      orig_poll(ConcreteTestingBackend::poll),
      orig_pread(ConcreteTestingBackend::pread)
{
}

MockConcreteSyscalls::~MockConcreteSyscalls()
{
    ConcreteTestingBackend::read = orig_read;
    ConcreteTestingBackend::write = orig_write;
    ConcreteTestingBackend::writev = orig_writev;
    ConcreteTestingBackend::sendfile = orig_sendfile;
    ConcreteTestingBackend::splice = orig_splice;
    ConcreteTestingBackend::poll = orig_poll;
    ConcreteTestingBackend::pread = orig_pread;
}

DisableSendfileSplice::DisableSendfileSplice()
{
    ConcreteTestingBackend::sendfile = [](int out_fd, int in_fd, off_t *offset, size_t count) -> ssize_t {
        errno = EINVAL;
        return -1;
    };
    ConcreteTestingBackend::splice = [](int fd_in, loff_t *off_in, int fd_out,
                                        loff_t *off_out, size_t len, unsigned int flags) -> ssize_t {
        errno = EINVAL;
        return -1;
    };
}

ExpectedSyscalls::ExpectedSyscalls(std::vector<ExpectedSyscallMatch*> expected) : expected(expected)
{
    ConcreteTestingBackend::read = [this](int fd, void *buf, size_t count) { return on_read(fd, buf, count); };
    ConcreteTestingBackend::write = [this](int fd, const void *buf, size_t count) { return on_write(fd, buf, count); };
    ConcreteTestingBackend::writev = [this](int fd, const struct iovec *iov, int iovcnt) { return on_writev(fd, iov, iovcnt); };
    ConcreteTestingBackend::sendfile = [this](int out_fd, int in_fd, off_t *offset, size_t count) { return on_sendfile(out_fd, in_fd, offset, count); };
    ConcreteTestingBackend::splice = [this](int fd_in, loff_t *off_in, int fd_out, 
                      loff_t *off_out, size_t len, unsigned int flags) { return on_splice(fd_in, off_in, fd_out, off_out, len, flags); };
    ConcreteTestingBackend::poll = [this](struct pollfd *fds, nfds_t nfds, int timeout) { return on_poll(fds, nfds, timeout); };
    ConcreteTestingBackend::pread = [this](int fd, void *buf, size_t count, off_t offset) { return on_pread(fd, buf, count, offset); };
}

ExpectedSyscalls::~ExpectedSyscalls()
{
    for (auto& el: expected)
        delete el;
}

std::unique_ptr<ExpectedSyscallMatch> ExpectedSyscalls::pop(const char* name)
{
    if (expected.empty())
    {
        std::string msg = "unexpected syscall ";
        msg += name;
        msg += " received: the expected list is empty";
        wfail_test(msg);
    }
    std::unique_ptr<ExpectedSyscallMatch> res(expected[0]);
    expected.erase(expected.begin());
    return res;
}

ssize_t ExpectedSyscalls::on_read(int fd, void *buf, size_t count)
{
    return pop("read")->on_read(fd, buf, count);
}

ssize_t ExpectedSyscalls::on_write(int fd, const void *buf, size_t count)
{
    return pop("write")->on_write(fd, buf, count);
}

ssize_t ExpectedSyscalls::on_writev(int fd, const struct iovec *iov, int iovcnt)
{
    return pop("writev")->on_writev(fd, iov, iovcnt);
}

ssize_t ExpectedSyscalls::on_sendfile(int out_fd, int in_fd, off_t *offset, size_t count)
{
    return pop("sendfile")->on_sendfile(out_fd, in_fd, offset, count);
}

ssize_t ExpectedSyscalls::on_splice(int fd_in, loff_t *off_in, int fd_out,
                  loff_t *off_out, size_t len, unsigned int flags)
{
    return pop("splice")->on_splice(fd_in, off_in, fd_out, off_out, len, flags);
}

int ExpectedSyscalls::on_poll(struct pollfd *fds, nfds_t nfds, int timeout)
{
    return pop("poll")->on_poll(fds, nfds, timeout);
}

ssize_t ExpectedSyscalls::on_pread(int fd, void *buf, size_t count, off_t offset)
{
    return pop("pread")->on_pread(fd, buf, count, offset);
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
        if (!terminated())
        {
            terminate();
            wait();
        }
    }
};

class NonblockingPipeSource : public subprocess::Child
{
protected:
    std::string data;

    int main() noexcept override
    {
        sys::NamedFileDescriptor in(0, "stdin");
        sys::NamedFileDescriptor out(1, "stdout");

        while (true)
        {
            uint32_t size;
            in.read_all_or_throw(&size, sizeof(uint32_t));

            if (size == 0)
                break;

            std::vector<uint8_t> buf(size, 0);
            out.write_all_or_retry(buf);
        }

        out.close();
        return 0;
    }

public:
    NonblockingPipeSource()
    {
        set_stdin(subprocess::Redirect::PIPE);
        set_stdout(subprocess::Redirect::PIPE);
        fork();

        // Make the read end nonblocking
        int fl = fcntl(get_stdout(), F_GETFL);
        if (fl < 0)
            throw std::system_error(errno, std::system_category(), "cannot get file descriptor flags for read end of NonblockingPipeSource stdout pipe");
        if (fcntl(get_stdout(), F_SETFL, fl | O_NONBLOCK) < 0)
            throw std::system_error(errno, std::system_category(), "cannot set nonblocking file descriptor flags for NonblockingPipeSource stdout pipe");
    }
    ~NonblockingPipeSource()
    {
        if (!terminated())
        {
            terminate();
            wait();
        }
    }

    /**
     * Request the child process to send size bytes of data.
     *
     * Set 0 to size to ask the child process to close its side of the pipe and
     * exit.
     */
    void request_data(uint32_t size)
    {
        sys::NamedFileDescriptor cmdfd(get_stdin(), "NonblockingPipeSource command pipe");
        cmdfd.write_all_or_retry(&size, sizeof(uint32_t));
    }

    void wait_for_data()
    {
        pollfd pollinfo;
        pollinfo.events = POLLIN;
        pollinfo.fd = get_stdout();
        pollinfo.revents = 0;

        // Wait for available input data
        int res = ::poll(&pollinfo, 1, 1000);
        if (res < 0)
            throw std::system_error(errno, std::system_category(), "poll failed on input pipe");
        if (res == 0)
            throw std::runtime_error("poll timed out on NonblockingPipeSource");
        if (pollinfo.revents & POLLERR)
            throw std::runtime_error("POLLERR on NonblockingPipeSource");
        if (pollinfo.revents & POLLHUP)
            throw std::runtime_error("POLLHUP on NonblockingPipeSource");
        if (! (pollinfo.revents & POLLIN))
            throw std::runtime_error("unsupported revents values on NonblockingPipeSource");
    }
};

}


void StreamTests::register_tests() {
using namespace arki::tests;

add_method("send_line", [&] {
    auto f = make_fixture();

    wassert(actual(f->send_line("testline", 8)) == stream::SendResult());
    wassert(actual(f->send_line("testline1", 9)) == stream::SendResult());

    wassert(actual(f->streamed_contents()) == "testline\ntestline1\n");
});

add_method("send_line_filtered", [&] {
    auto f = make_fixture();

    wassert(actual(f->send_line("testline", 8)) == stream::SendResult());

    {
        WithFilter filtered(f->stream(), {"wc", "-l"});
        wassert(actual(f->send_line("testline1", 9)) == stream::SendResult());
        wassert(actual(filtered.done()) == std::make_pair(10lu, 2lu));
    }

    wassert(actual(f->streamed_contents()) == "testline\n1\n");
});

add_method("send_buffer", [&] {
    auto f = make_fixture();

    wassert(actual(f->send_buffer("testbuf", 7)) == stream::SendResult());
    wassert(actual(f->send_buffer("testbuf", 4)) == stream::SendResult());

    wassert(actual(f->streamed_contents()) == "testbuftest");
});

add_method("send_buffer_filtered", [&] {
    auto f = make_fixture();

    wassert(actual(f->send_buffer("testbuf", 7)) == stream::SendResult());

    {
        WithFilter filtered(f->stream(), {"wc", "-c"});
        wassert(actual(f->send_buffer("testbuf", 4)) == stream::SendResult());
        wassert(actual(filtered.done()) == std::make_pair(4lu, 2lu));
    }

    wassert(actual(f->streamed_contents()) == "testbuf4\n");
});

add_method("send_file_segment", [&] {
    auto f = make_fixture();

    sys::Tempfile tf1;
    tf1.write_all_or_throw(std::string("testfile"));

    wassert(actual(f->send_file_segment(tf1, 1, 6)) == stream::SendResult());
    wassert(actual(f->send_file_segment(tf1, 5, 1)) == stream::SendResult());
    wassert(actual(f->send_file_segment(tf1, 0, 4)) == stream::SendResult());

    wassert(actual(f->streamed_contents()) == "estfilitest");
});

add_method("send_file_segment_filtered", [&] {
    auto f = make_fixture();

    sys::Tempfile tf1;
    tf1.write_all_or_throw(std::string("testfile"));

    wassert(actual(f->send_file_segment(tf1, 1, 6)) == stream::SendResult());
    {
        WithFilter filtered(f->stream(), {"wc", "-c"});
        wassert(actual(f->send_file_segment(tf1, 5, 1)) == stream::SendResult());
        wassert(actual(f->send_file_segment(tf1, 0, 4)) == stream::SendResult());
        wassert(actual(filtered.done()) == std::make_pair(5lu, 2lu));
    }

    wassert(actual(f->streamed_contents()) == "estfil5\n");
});

add_method("send_from_pipe_read", [&] {
    auto f = make_fixture();

    // Neither the source not the target is a pipe, so this won't trigger splice
    sys::Tempfile tf1;
    tf1.write_all_or_throw(std::string("testfile"));
    tf1.lseek(1);

    wassert(actual(f->send_from_pipe(tf1)) == std::make_pair(7lu, stream::SendResult(stream::SendResult::SEND_PIPE_EOF_SOURCE)));

    wassert(actual(f->streamed_contents()) == "estfile");
});

add_method("send_from_pipe_splice", [&] {
    auto f = make_fixture();

    // Source is a pipe and it should trigger splice
    PipeSource child("testfile");
    wassert(actual(f->send_from_pipe(child.get_stdout())) == std::make_pair(8lu, stream::SendResult(stream::SendResult::SEND_PIPE_EOF_SOURCE)));

    wassert(actual(f->streamed_contents()) == "testfile");
});

add_method("send_from_pipe_splice_nonblocking", [&] {
    auto f = make_fixture();

    // Source is a pipe and it should trigger splice
    NonblockingPipeSource child;
    child.request_data(8);
    child.wait_for_data();
    wassert(actual(f->send_from_pipe(child.get_stdout())) == std::make_pair(8lu, stream::SendResult(stream::SendResult::SEND_PIPE_EAGAIN_SOURCE)));
    child.request_data(123456);
    child.wait_for_data();

    size_t sent = 0;
    while (sent < 123456u)
    {
        auto res = f->send_from_pipe(child.get_stdout());
        wassert(actual(res.second.flags) == stream::SendResult::SEND_PIPE_EAGAIN_SOURCE);
        sent += res.first;
    }

    child.request_data(123);
    child.request_data(0);
    child.wait();

    auto res = f->send_from_pipe(child.get_stdout());
    wassert(actual(res.first) == 123u);
    wassert_true((res.second.flags & stream::SendResult::SEND_PIPE_EAGAIN_SOURCE) || (res.second.flags & stream::SendResult::SEND_PIPE_EOF_SOURCE));

    if (res.second.flags & stream::SendResult::SEND_PIPE_EAGAIN_SOURCE)
        wassert(actual(f->send_from_pipe(child.get_stdout())) == std::make_pair(0lu, stream::SendResult(stream::SendResult::SEND_PIPE_EOF_SOURCE)));

    wassert(actual(f->streamed_contents()) == std::string(8 + 123456 + 123, 0));
});

add_method("data_start_send_from_pipe_splice_nonblocking", [&] {
    auto f = make_fixture();
    f->set_data_start_callback([](StreamOutput& out) { return out.send_buffer("start", 5); });

    // Source is a pipe and it should trigger splice
    NonblockingPipeSource child;
    child.request_data(8);
    child.wait_for_data();
    wassert(actual(f->send_from_pipe(child.get_stdout())) == std::make_pair(8lu, stream::SendResult(stream::SendResult::SEND_PIPE_EAGAIN_SOURCE)));
    child.request_data(123456);
    child.wait_for_data();

    size_t sent = 0;
    while (sent < 123456u)
    {
        auto res = f->send_from_pipe(child.get_stdout());
        wassert(actual(res.second.flags) == stream::SendResult::SEND_PIPE_EAGAIN_SOURCE);
        sent += res.first;
    }

    child.request_data(123);
    child.request_data(0);
    child.wait();

    auto res = f->send_from_pipe(child.get_stdout());
    wassert(actual(res.first) == 123u);
    wassert_true((res.second.flags & stream::SendResult::SEND_PIPE_EAGAIN_SOURCE) || (res.second.flags & stream::SendResult::SEND_PIPE_EOF_SOURCE));

    if (res.second.flags & stream::SendResult::SEND_PIPE_EAGAIN_SOURCE)
        wassert(actual(f->send_from_pipe(child.get_stdout())) == std::make_pair(0lu, stream::SendResult(stream::SendResult::SEND_PIPE_EOF_SOURCE)));

    wassert(actual(f->streamed_contents().size()) == 8u + 5u + 123456u + 123u);
});

add_method("data_start_send_line", [&] {
    auto f = make_fixture();
    f->set_data_start_callback([](StreamOutput& out) { return out.send_buffer("start", 5); });

    wassert(actual(f->send_line("testline", 8)) == stream::SendResult());
    wassert(actual(f->send_line("testline1", 9)) == stream::SendResult());

    wassert(actual(f->streamed_contents()) == "starttestline\ntestline1\n");

    bool fired = false;
    f->set_data_start_callback([&](StreamOutput& out) { fired=true; return SendResult(); });
    wassert(actual(f->send_line("testline", 0)) == stream::SendResult());
    wassert_false(fired);
});

add_method("data_start_send_buffer", [&] {
    auto f = make_fixture();
    f->set_data_start_callback([](StreamOutput& out) { return out.send_line("start", 5); });

    wassert(actual(f->send_buffer("testbuf", 7)) == stream::SendResult());
    wassert(actual(f->send_buffer("testbuf", 4)) == stream::SendResult());

    wassert(actual(f->streamed_contents()) == "start\ntestbuftest");

    bool fired = false;
    f->set_data_start_callback([&](StreamOutput& out) { fired=true; return stream::SendResult(); });
    wassert(actual(f->send_buffer("testline", 0)) == stream::SendResult());
    wassert_false(fired);
});

add_method("data_start_send_file_segment", [&] {
    auto f = make_fixture();
    f->set_data_start_callback([](StreamOutput& out) {
        sys::Tempfile t;
        t.write_all_or_throw("start", 5);
        t.lseek(0);
        auto res = out.send_from_pipe(t);
        res.second.flags = res.second.flags & ~stream::SendResult::SEND_PIPE_EOF_SOURCE;
        return res.second;
    });

    sys::Tempfile tf1;
    tf1.write_all_or_throw(std::string("testfile"));

    wassert(actual(f->send_file_segment(tf1, 1, 6)) == stream::SendResult());
    wassert(actual(f->send_file_segment(tf1, 5, 1)) == stream::SendResult());
    wassert(actual(f->send_file_segment(tf1, 0, 4)) == stream::SendResult());
    // Send past the end of file
    wassert_throws(std::runtime_error, actual(f->send_file_segment(tf1, 6, 4)));

    wassert(actual(f->streamed_contents()) == "startestfilitestle");

    bool fired = false;
    f->set_data_start_callback([&](StreamOutput& out) { fired=true; return stream::SendResult(); });
    wassert(actual(f->send_file_segment(tf1, 3, 0)) == stream::SendResult());
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
    wassert(actual(f->send_from_pipe(tf1)) == std::make_pair(7lu, stream::SendResult(stream::SendResult::SEND_PIPE_EOF_SOURCE)));

    tf1.lseek(2);
    wassert(actual(f->send_from_pipe(tf1)) == std::make_pair(6lu, stream::SendResult(stream::SendResult::SEND_PIPE_EOF_SOURCE)));

    // Pipe from end of file
    tf1.lseek(0, SEEK_END);
    wassert(actual(f->send_from_pipe(tf1)) == std::make_pair(0lu, stream::SendResult(stream::SendResult::SEND_PIPE_EOF_SOURCE)));

    wassert(actual(f->streamed_contents()) == "tarestfilestfile");

    bool fired = false;
    f->set_data_start_callback([&](StreamOutput& out) { fired=true; return stream::SendResult(); });
    tf1.lseek(0, SEEK_END);
    wassert(actual(f->send_from_pipe(tf1)) == std::make_pair(0lu, stream::SendResult(stream::SendResult::SEND_PIPE_EOF_SOURCE)));
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
    wassert(actual(f->send_from_pipe(child.get_stdout())) == std::make_pair(8lu, stream::SendResult(stream::SendResult::SEND_PIPE_EOF_SOURCE)));
    wassert(actual(f->send_from_pipe(child.get_stdout())) == std::make_pair(0lu, stream::SendResult(stream::SendResult::SEND_PIPE_EOF_SOURCE)));

    // Pipe from an empty pipe
    PipeSource child1("");
    wassert(actual(f->send_from_pipe(child1.get_stdout())) == std::make_pair(0lu, stream::SendResult(stream::SendResult::SEND_PIPE_EOF_SOURCE)));

    wassert(actual(f->streamed_contents()) == "tartestfile");

    bool fired = false;
    f->set_data_start_callback([&](StreamOutput& out) { fired=true; return stream::SendResult(); });
    PipeSource child2("");
    wassert(actual(f->send_from_pipe(child2.get_stdout())) == std::make_pair(0lu, stream::SendResult(stream::SendResult::SEND_PIPE_EOF_SOURCE)));
    wassert_false(fired);
});

add_method("large_send_line", [&] {
    auto f = make_fixture();
    std::vector<uint8_t> buf(stream::TransferBuffer::size * 3);

    wassert(actual(f->send_line(buf.data(), buf.size())) == stream::SendResult());

    wassert(actual(f->streamed_contents().size()) == buf.size() + 1);
});

add_method("large_send_buffer", [&] {
    auto f = make_fixture();
    std::vector<uint8_t> buf(stream::TransferBuffer::size * 3);

    wassert(actual(f->send_buffer(buf.data(), buf.size())) == stream::SendResult());

    wassert(actual(f->streamed_contents().size()) == buf.size());
});

add_method("large_send_file_segment", [&] {
    auto f = make_fixture();
    std::vector<uint8_t> buf(stream::TransferBuffer::size * 3);

    sys::Tempfile tf1;
    tf1.write_all_or_retry(buf);

    wassert(actual(f->send_file_segment(tf1, 0, buf.size())) == stream::SendResult());

    wassert(actual(f->streamed_contents().size()) == buf.size());
});

add_method("large_send_from_pipe_read", [&] {
    auto f = make_fixture();
    std::vector<uint8_t> buf(stream::TransferBuffer::size * 3);

    // Neither the source not the target is a pipe, so this won't trigger splice
    sys::Tempfile tf1;
    tf1.write_all_or_retry(buf);
    tf1.lseek(0);

    wassert(actual(f->send_from_pipe(tf1)) == std::make_pair(buf.size(), stream::SendResult(stream::SendResult::SEND_PIPE_EOF_SOURCE)));

    wassert(actual(f->streamed_contents().size()) == buf.size());
});

add_method("large_send_from_pipe_splice", [&] {
    auto f = make_fixture();
    size_t size = stream::TransferBuffer::size * 3;

    // Source is a pipe and it should trigger splice
    PipeSource child(std::string(size, 0));
    wassert(actual(f->send_from_pipe(child.get_stdout())) == std::make_pair(size, stream::SendResult(stream::SendResult::SEND_PIPE_EOF_SOURCE)));

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
    wassert(actual(f->send_from_pipe(child.get_stdout())) == std::make_pair(size, stream::SendResult(stream::SendResult::SEND_PIPE_EOF_SOURCE)));

    wassert(actual(f->streamed_contents()).startswith("foo\n        "));
});

}

namespace {

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

}

void ConcreteStreamTests::register_tests() {
using namespace arki::tests;
StreamTests::register_tests();

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

add_method("closed_pipe_send_pipe_read", [&] {
    ClosedPipe cp;
    auto f = make_concrete_fixture(cp.fd);

    sys::Tempfile tf;
    tf.write_all_or_retry("test", 4);
    tf.lseek(0);
    wassert(actual(f->send_from_pipe(tf)) == std::make_pair(0lu, stream::SendResult(stream::SendResult::SEND_PIPE_EOF_DEST)));
});

add_method("closed_pipe_send_pipe_splice", [&] {
    ClosedPipe cp;
    auto f = make_concrete_fixture(cp.fd);
    PipeSource child("test");
    wassert(actual(f->send_from_pipe(child.get_stdout())) == std::make_pair(0lu, stream::SendResult(stream::SendResult::SEND_PIPE_EOF_DEST)));
});

add_method("read_eof", [&] {
    struct ReadEof : DisableSendfileSplice
    {
        size_t read_pos = 0;
        size_t available;

        ReadEof(size_t lead_size=0)
            : available(lead_size)
        {
            ConcreteTestingBackend::read = [this](int fd, void *buf, size_t count) -> ssize_t {
                if (read_pos < available) {
                    count = std::min(count, available - read_pos);
                    memset(buf, 0, count);
                    read_pos += count;
                    return count;
                } else {
                    return 0;
                }
            };
            ConcreteTestingBackend::pread = [this](int fd, void *buf, size_t count, off_t offset) -> ssize_t {
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


// stream::SendResult send_line(const void* data, size_t size);
// stream::SendResult send_buffer(const void* data, size_t size);
// stream::SendResult send_file_segment(arki::core::NamedFileDescriptor& fd, off_t offset, size_t size);
// stream::SendResult send_from_pipe(int fd);

}

}
}
