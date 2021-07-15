#include "tests.h"
#include "base.h"
#include "concrete.h"
#include "filter.h"
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

MockConcreteSyscalls::MockConcreteSyscalls()
    : orig_read(TestingBackend::read),
      orig_write(TestingBackend::write),
      orig_writev(TestingBackend::writev),
      orig_sendfile(TestingBackend::sendfile),
      orig_splice(TestingBackend::splice),
      orig_poll(TestingBackend::poll),
      orig_pread(TestingBackend::pread)
{
}

MockConcreteSyscalls::~MockConcreteSyscalls()
{
    TestingBackend::read = orig_read;
    TestingBackend::write = orig_write;
    TestingBackend::writev = orig_writev;
    TestingBackend::sendfile = orig_sendfile;
    TestingBackend::splice = orig_splice;
    TestingBackend::poll = orig_poll;
    TestingBackend::pread = orig_pread;
}

DisableSendfileSplice::DisableSendfileSplice()
{
    TestingBackend::sendfile = [](int out_fd, int in_fd, off_t *offset, size_t count) -> ssize_t {
        errno = EINVAL;
        return -1;
    };
    TestingBackend::splice = [](int fd_in, loff_t *off_in, int fd_out,
                                        loff_t *off_out, size_t len, unsigned int flags) -> ssize_t {
        errno = EINVAL;
        return -1;
    };
}

ExpectedSyscalls::ExpectedSyscalls(std::vector<ExpectedSyscallMatch*> expected) : expected(expected)
{
    TestingBackend::read = [this](int fd, void *buf, size_t count) { return on_read(fd, buf, count); };
    TestingBackend::write = [this](int fd, const void *buf, size_t count) { return on_write(fd, buf, count); };
    TestingBackend::writev = [this](int fd, const struct iovec *iov, int iovcnt) { return on_writev(fd, iov, iovcnt); };
    TestingBackend::sendfile = [this](int out_fd, int in_fd, off_t *offset, size_t count) { return on_sendfile(out_fd, in_fd, offset, count); };
    TestingBackend::splice = [this](int fd_in, loff_t *off_in, int fd_out,
              loff_t *off_out, size_t len, unsigned int flags) { return on_splice(fd_in, off_in, fd_out, off_out, len, flags); };
    TestingBackend::poll = [this](struct pollfd *fds, nfds_t nfds, int timeout) { return on_poll(fds, nfds, timeout); };
    TestingBackend::pread = [this](int fd, void *buf, size_t count, off_t offset) { return on_pread(fd, buf, count, offset); };

    for (size_t i = 0; i < expected.size(); ++i)
        expected[i]->index = i + 1;
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
    // fprintf(stderr, "ExpectedSyscalls::pop %s\n", res->tag().c_str());
    return res;
}

void ExpectedSyscalls::check_not_called()
{
    if (!expected.empty())
        wfail_test("function unexpectedly stopped before syscall " + expected.front()->tag());
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
        f->stream().start_filter({"wc", "-l"});
        wassert(actual(f->send_line("testline1", 9)) == stream::SendResult());
        auto flt = f->stream().stop_filter();
        wassert(actual(flt->size_stdin) == 10u);
        wassert(actual(flt->size_stdout) == 2lu);
        wassert(actual(flt->errors.str()) == "");
        wassert(actual(flt->cmd.raw_returncode()) == 0);
    }

    wassert(actual(f->streamed_contents()) == "testline\n1\n");
});

add_method("send_line_filtered_stderr", [&] {
    auto f = make_fixture();

    wassert(actual(f->send_line("testline", 8)) == stream::SendResult());

    {
        f->stream().start_filter({"postproc/error"});
        wassert(actual(f->send_line("testline1", 9)) == stream::SendResult());
        auto flt = f->stream().stop_filter();
        wassert(actual(flt->size_stdin) == 10u);
        wassert(actual(flt->size_stdout) == 0lu);
        wassert(actual(flt->errors.str()) == "FAIL\n");
        wassert(actual(flt->cmd.returncode()) == 1);
        wassert_throws(std::runtime_error, flt->check_for_errors());
    }

    wassert(actual(f->streamed_contents()) == "testline\n");
});

add_method("send_line_filtered_exit", [&] {
    auto f = make_fixture();

    wassert(actual(f->send_line("testline", 8)) == stream::SendResult());

    {
        f->stream().start_filter({"postproc/exit"});
        wassert(actual(f->send_line("testline1", 9)) == stream::SendResult());
        auto flt = f->stream().stop_filter();
        wassert(actual(flt->size_stdin) == 10u);
        wassert(actual(flt->size_stdout) == 0lu);
        wassert(actual(flt->errors.str()) == "");
        wassert(actual(flt->cmd.returncode()) == 0);
    }

    wassert(actual(f->streamed_contents()) == "testline\n");
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
        f->stream().start_filter({"wc", "-c"});
        wassert(actual(f->send_buffer("testbuf", 4)) == stream::SendResult());
        auto flt = f->stream().stop_filter();
        wassert(actual(flt->size_stdin) == 4u);
        wassert(actual(flt->size_stdout) == 2lu);
        wassert(actual(flt->errors.str()) == "");
        wassert(actual(flt->cmd.raw_returncode()) == 0);
    }

    wassert(actual(f->streamed_contents()) == "testbuf4\n");
});

add_method("send_buffer_filtered_stderr", [&] {
    auto f = make_fixture();

    wassert(actual(f->send_buffer("testbuf", 7)) == stream::SendResult());

    {
        f->stream().start_filter({"postproc/error"});
        wassert(actual(f->send_buffer("testbuf", 4)) == stream::SendResult());
        auto flt = f->stream().stop_filter();
        wassert(actual(flt->size_stdin) == 4u);
        wassert(actual(flt->size_stdout) == 0lu);
        wassert(actual(flt->errors.str()) == "FAIL\n");
        wassert(actual(flt->cmd.returncode()) == 1);
        wassert_throws(std::runtime_error, flt->check_for_errors());
    }

    wassert(actual(f->streamed_contents()) == "testbuf");
});

add_method("send_buffer_filtered_exit", [&] {
    auto f = make_fixture();

    wassert(actual(f->send_buffer("testbuf", 7)) == stream::SendResult());

    {
        f->stream().start_filter({"postproc/exit"});
        wassert(actual(f->send_buffer("testbuf", 4)) == stream::SendResult());
        auto flt = f->stream().stop_filter();
        wassert(actual(flt->size_stdin) == 4u);
        wassert(actual(flt->size_stdout) == 0lu);
        wassert(actual(flt->errors.str()) == "");
        wassert(actual(flt->cmd.returncode()) == 0);
    }

    wassert(actual(f->streamed_contents()) == "testbuf");
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
        f->stream().start_filter({"wc", "-c"});
        wassert(actual(f->send_file_segment(tf1, 5, 1)) == stream::SendResult());
        wassert(actual(f->send_file_segment(tf1, 0, 4)) == stream::SendResult());
        auto flt = f->stream().stop_filter();
        wassert(actual(flt->size_stdin) == 5u);
        wassert(actual(flt->size_stdout) == 2lu);
        wassert(actual(flt->errors.str()) == "");
        wassert(actual(flt->cmd.raw_returncode()) == 0);
    }

    wassert(actual(f->streamed_contents()) == "estfil5\n");
});

add_method("send_file_segment_filtered_stderr", [&] {
    auto f = make_fixture();

    sys::Tempfile tf1;
    tf1.write_all_or_throw(std::string("testfile"));

    wassert(actual(f->send_file_segment(tf1, 1, 6)) == stream::SendResult());

    {
        f->stream().start_filter({"postproc/error"});
        wassert(actual(f->send_file_segment(tf1, 5, 1)) == stream::SendResult());
        wassert(actual(f->send_file_segment(tf1, 0, 4)) == stream::SendResult());
        auto flt = f->stream().stop_filter();
        wassert(actual(flt->size_stdin) == 5u);
        wassert(actual(flt->size_stdout) == 0lu);
        wassert(actual(flt->errors.str()) == "FAIL\n");
        wassert(actual(flt->cmd.returncode()) == 1);
        wassert_throws(std::runtime_error, flt->check_for_errors());
    }

    wassert(actual(f->streamed_contents()) == "estfil");
});

add_method("send_file_segment_filtered_exit", [&] {
    auto f = make_fixture();

    sys::Tempfile tf1;
    tf1.write_all_or_throw(std::string("testfile"));

    wassert(actual(f->send_file_segment(tf1, 1, 6)) == stream::SendResult());

    {
        f->stream().start_filter({"postproc/exit"});
        wassert(actual(f->send_file_segment(tf1, 5, 1)) == stream::SendResult());
        wassert(actual(f->send_file_segment(tf1, 0, 4)) == stream::SendResult());
        auto flt = f->stream().stop_filter();
        wassert(actual(flt->size_stdin) == 5u);
        wassert(actual(flt->size_stdout) == 0lu);
        wassert(actual(flt->errors.str()) == "");
        wassert(actual(flt->cmd.returncode()) == 0);
    }

    wassert(actual(f->streamed_contents()) == "estfil");
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

}

}
}
