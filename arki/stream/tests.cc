#include "tests.h"
#include "arki/utils/sys.h"
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


void StreamTests::register_tests() {
using namespace arki::tests;

add_method("send_line", [&] {
    auto f = make_fixture();

    f->output->send_line("testline", 8);
    f->output->send_line("testline1", 9);

    wassert(actual(f->streamed_contents()) == "testline\ntestline1\n");
});

add_method("send_buffer", [&] {
    auto f = make_fixture();

    f->output->send_buffer("testbuf", 7);
    f->output->send_buffer("testbuf", 4);

    wassert(actual(f->streamed_contents()) == "testbuftest");
});

add_method("send_file_segment", [&] {
    auto f = make_fixture();

    sys::Tempfile tf1;
    tf1.write_all_or_throw(std::string("testfile"));

    f->output->send_file_segment(tf1, 1, 6);
    f->output->send_file_segment(tf1, 5, 1);
    f->output->send_file_segment(tf1, 0, 4);

    wassert(actual(f->streamed_contents()) == "estfilitest");
});

add_method("send_from_pipe", [&] {
    auto f = make_fixture();

    sys::Tempfile tf1;
    tf1.write_all_or_throw(std::string("testfile"));
    tf1.lseek(1);

    f->output->send_from_pipe(tf1);

    wassert(actual(f->streamed_contents()) == "estfile");
});

add_method("data_start_send_line", [&] {
    auto f = make_fixture();
    f->output->set_data_start_callback([](StreamOutput& out) { out.send_buffer("start", 5); });

    f->output->send_line("testline", 8);
    f->output->send_line("testline1", 9);

    wassert(actual(f->streamed_contents()) == "starttestline\ntestline1\n");
});

add_method("data_start_send_buffer", [&] {
    auto f = make_fixture();
    f->output->set_data_start_callback([](StreamOutput& out) { out.send_line("start", 5); });

    f->output->send_buffer("testbuf", 7);
    f->output->send_buffer("testbuf", 4);

    wassert(actual(f->streamed_contents()) == "start\ntestbuftest");
});

add_method("data_start_send_file_segment", [&] {
    auto f = make_fixture();
    f->output->set_data_start_callback([](StreamOutput& out) {
        sys::Tempfile t;
        t.write_all_or_throw("start", 5);
        t.lseek(0);
        out.send_from_pipe(t);
    });

    sys::Tempfile tf1;
    tf1.write_all_or_throw(std::string("testfile"));

    f->output->send_file_segment(tf1, 1, 6);
    f->output->send_file_segment(tf1, 5, 1);
    f->output->send_file_segment(tf1, 0, 4);

    wassert(actual(f->streamed_contents()) == "startestfilitest");
});

add_method("data_start_send_from_pipe", [&] {
    auto f = make_fixture();
    f->output->set_data_start_callback([](StreamOutput& out) {
        sys::Tempfile t;
        t.write_all_or_throw("start", 5);
        out.send_file_segment(t, 1, 3);
    });

    sys::Tempfile tf1;
    tf1.write_all_or_throw(std::string("testfile"));
    tf1.lseek(1);
    f->output->send_from_pipe(tf1);

    tf1.lseek(2);
    f->output->send_from_pipe(tf1);

    wassert(actual(f->streamed_contents()) == "tarestfilestfile");
});

}

}
}
