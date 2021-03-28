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


void StreamTestsFixture::set_output(std::unique_ptr<StreamOutput>&& output)
{
    this->output = std::move(output);

    this->output->set_progress_callback([&](size_t written) {
        cb_log.push_back(written);
    });
}

size_t StreamTestsFixture::send_line(const void* data, size_t size)
{
    cb_log.clear();
    return output->send_line(data, size);
}

size_t StreamTestsFixture::send_file_segment(arki::core::NamedFileDescriptor& fd, off_t offset, size_t size)
{
    cb_log.clear();
    return output->send_file_segment(fd, offset, size);
}

size_t StreamTestsFixture::send_buffer(const void* data, size_t size)
{
    cb_log.clear();
    return output->send_buffer(data, size);
}

size_t StreamTestsFixture::send_from_pipe(int fd)
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


void StreamTests::register_tests() {
using namespace arki::tests;

add_method("send_line", [&] {
    auto f = make_fixture();

    wassert(actual(f->send_line("testline", 8)) == 9u);
    wassert(actual(f->cb_log) == std::vector<size_t>{9u});
    wassert(actual(f->send_line("testline1", 9)) == 10u);
    wassert(actual(f->cb_log) == std::vector<size_t>{10u});

    wassert(actual(f->streamed_contents()) == "testline\ntestline1\n");
});

add_method("send_buffer", [&] {
    auto f = make_fixture();

    wassert(actual(f->send_buffer("testbuf", 7)) == 7u);
    wassert(actual(f->cb_log) == std::vector<size_t>{7u});
    wassert(actual(f->send_buffer("testbuf", 4)) == 4u);
    wassert(actual(f->cb_log) == std::vector<size_t>{4u});

    wassert(actual(f->streamed_contents()) == "testbuftest");
});

add_method("send_file_segment", [&] {
    auto f = make_fixture();

    sys::Tempfile tf1;
    tf1.write_all_or_throw(std::string("testfile"));

    wassert(actual(f->send_file_segment(tf1, 1, 6)) == 6u);
    wassert(actual(f->cb_log) == std::vector<size_t>{6});
    wassert(actual(f->send_file_segment(tf1, 5, 1)) == 1u);
    wassert(actual(f->cb_log) == std::vector<size_t>{1});
    wassert(actual(f->send_file_segment(tf1, 0, 4)) == 4u);
    wassert(actual(f->cb_log) == std::vector<size_t>{4});

    wassert(actual(f->streamed_contents()) == "estfilitest");
});

add_method("send_from_pipe", [&] {
    auto f = make_fixture();

    sys::Tempfile tf1;
    tf1.write_all_or_throw(std::string("testfile"));
    tf1.lseek(1);

    wassert(actual(f->send_from_pipe(tf1)) == 7u);
    wassert(actual(f->cb_log) == std::vector<size_t>{7});

    wassert(actual(f->streamed_contents()) == "estfile");
});

add_method("data_start_send_line", [&] {
    auto f = make_fixture();
    f->set_data_start_callback([](StreamOutput& out) { return out.send_buffer("start", 5); });

    wassert(actual(f->send_line("testline", 8)) == 9u + 5u);
    wassert(actual(f->cb_log) == std::vector<size_t>{5, 9});
    wassert(actual(f->send_line("testline1", 9)) == 10u);
    wassert(actual(f->cb_log) == std::vector<size_t>{10});

    wassert(actual(f->streamed_contents()) == "starttestline\ntestline1\n");

    bool fired = false;
    f->set_data_start_callback([&](StreamOutput& out) { fired=true; return 0u; });
    wassert(actual(f->send_line("testline", 0)) == 0u);
    wassert(actual(f->cb_log) == std::vector<size_t>());
    wassert_false(fired);
});

add_method("data_start_send_buffer", [&] {
    auto f = make_fixture();
    f->set_data_start_callback([](StreamOutput& out) { return out.send_line("start", 5); });

    wassert(actual(f->send_buffer("testbuf", 7)) == 7u + 6u);
    wassert(actual(f->cb_log) == std::vector<size_t>{6, 7});
    wassert(actual(f->send_buffer("testbuf", 4)) == 4u);
    wassert(actual(f->cb_log) == std::vector<size_t>{4});

    wassert(actual(f->streamed_contents()) == "start\ntestbuftest");

    bool fired = false;
    f->set_data_start_callback([&](StreamOutput& out) { fired=true; return 0u; });
    wassert(actual(f->send_buffer("testline", 0)) == 0u);
    wassert(actual(f->cb_log) == std::vector<size_t>());
    wassert_false(fired);
});

add_method("data_start_send_file_segment", [&] {
    auto f = make_fixture();
    f->set_data_start_callback([](StreamOutput& out) {
        sys::Tempfile t;
        t.write_all_or_throw("start", 5);
        t.lseek(0);
        return out.send_from_pipe(t);
    });

    sys::Tempfile tf1;
    tf1.write_all_or_throw(std::string("testfile"));

    wassert(actual(f->send_file_segment(tf1, 1, 6)) == 6u + 5u);
    wassert(actual(f->cb_log) == std::vector<size_t>{5, 6});
    wassert(actual(f->send_file_segment(tf1, 5, 1)) == 1u);
    wassert(actual(f->cb_log) == std::vector<size_t>{1});
    wassert(actual(f->send_file_segment(tf1, 0, 4)) == 4u);
    wassert(actual(f->cb_log) == std::vector<size_t>{4});
    // Send past the end of file
    wassert(actual(f->send_file_segment(tf1, 6, 4)) == 2u);
    wassert(actual(f->cb_log) == std::vector<size_t>{2});

    wassert(actual(f->streamed_contents()) == "startestfilitestle");

    bool fired = false;
    f->set_data_start_callback([&](StreamOutput& out) { fired=true; return 0u; });
    wassert(actual(f->send_file_segment(tf1, 3, 0)) == 0u);
    wassert(actual(f->cb_log) == std::vector<size_t>());
    wassert_false(fired);

    fired = false;
    f->set_data_start_callback([&](StreamOutput& out) { fired=true; return 0u; });
    wassert(actual(f->send_file_segment(tf1, 8, 1)) == 0u);
    wassert(actual(f->cb_log) == std::vector<size_t>());
    wassert_false(fired);
});

add_method("data_start_send_from_pipe", [&] {
    auto f = make_fixture();
    f->set_data_start_callback([](StreamOutput& out) {
        sys::Tempfile t;
        t.write_all_or_throw("start", 5);
        return out.send_file_segment(t, 1, 3);
    });

    sys::Tempfile tf1;
    tf1.write_all_or_throw(std::string("testfile"));
    tf1.lseek(1);
    wassert(actual(f->send_from_pipe(tf1)) == 7u + 3u);
    wassert(actual(f->cb_log) == std::vector<size_t>{3, 7});

    tf1.lseek(2);
    wassert(actual(f->send_from_pipe(tf1)) == 6u);
    wassert(actual(f->cb_log) == std::vector<size_t>{6});

    // Pipe from end of file
    tf1.lseek(0, SEEK_END);
    wassert(actual(f->send_from_pipe(tf1)) == 0u);
    wassert(actual(f->cb_log) == std::vector<size_t>());

    wassert(actual(f->streamed_contents()) == "tarestfilestfile");

    bool fired = false;
    f->set_data_start_callback([&](StreamOutput& out) { fired=true; return 0u; });
    tf1.lseek(0, SEEK_END);
    wassert(actual(f->send_from_pipe(tf1)) == 0u);
    wassert(actual(f->cb_log) == std::vector<size_t>());
    wassert_false(fired);
});

}

}
}
