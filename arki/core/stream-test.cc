#include "arki/tests/tests.h"
#include "arki/utils/sys.h"
#include "stream.h"
#include "file.h"

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
    std::unique_ptr<sys::NamedFileDescriptor> outfd;

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

        outfd.reset(new sys::NamedFileDescriptor(fds[1], "blocking test pipe"));
    }

    ~BlockingSink()
    {
        close(fds[0]);
        close(fds[1]);
    }

    sys::NamedFileDescriptor& fd() { return *outfd; }
    size_t pipe_size() const { return pipe_sz; }

    /// Flush the pipe buffer until it's completely empty
    void empty_buffer()
    {
        char buf[4096];
        while (outfd->read(buf, 4096))
            ;
    }
};


class Tests : public TestCase
{
    using TestCase::TestCase;

    void register_tests() override;
};

Tests test("arki_core_stream");

void Tests::register_tests() {

add_method("concrete", [] {
    sys::Tempfile tf;
    auto writer = StreamOutput::create(tf);
    writer->send_line("testline", 8);
    writer->send_buffer("testbuf", 7);
    sys::Tempfile tf1;
    tf1.write_all_or_throw(std::string("testfile"));
    writer->send_file_segment(tf1, 1, 6);
    tf1.lseek(0);
    writer->send_from_pipe(tf1);

    tf.lseek(0);

    char buf[256];
    wassert(actual(tf.read(buf, 256)) == 22u);
    buf[22] = 0;
    wassert(actual(buf) == "testline\ntestbufestfiltestfile");
});

add_method("concrete_timeout", [] {
    sys::Tempfile tf;
    auto writer = StreamOutput::create(tf, 1000);
    writer->send_line("testline", 8);
    writer->send_buffer("testbuf", 7);
    sys::Tempfile tf1;
    tf1.write_all_or_throw(std::string("testfile"));
    writer->send_file_segment(tf1, 1, 6);
    tf1.lseek(0);
    writer->send_from_pipe(tf1);

    tf.lseek(0);

    char buf[256];
    wassert(actual(tf.read(buf, 256)) == 22u);
    buf[22] = 0;
    wassert(actual(buf) == "testline\ntestbufestfiltestfile");
});

add_method("concrete_timeout1", [] {
    BlockingSink sink;
    std::vector<uint8_t> filler(sink.pipe_size());

    auto writer = StreamOutput::create(sink.fd(), 1);

    // This won't block
    writer->send_buffer(filler.data(), filler.size());
    // This times out
    wassert_throws(std::runtime_error, writer->send_buffer(" ", 1));

    sink.empty_buffer();
    wassert_throws(std::runtime_error, writer->send_line(filler.data(), filler.size()));

    sink.empty_buffer();
    writer->send_buffer(filler.data(), filler.size());
    {
        sys::Tempfile tf1;
        tf1.write_all_or_throw(std::string("testfile"));
        wassert_throws(std::runtime_error, writer->send_file_segment(tf1, 1, 6));
    }

    sink.empty_buffer();
    writer->send_buffer(filler.data(), filler.size());
    {
        sys::Tempfile tf1;
        tf1.write_all_or_throw(std::string("testfile"));
        wassert_throws(std::runtime_error, writer->send_from_pipe(tf1));
    }
});

add_method("abstract", [] {
    std::vector<uint8_t> buffer;
    BufferOutputFile out(buffer, "memory buffer");
    auto writer = StreamOutput::create(out);
    writer->send_line("testline", 8);
    writer->send_buffer("testbuf", 7);
    sys::Tempfile tf1;
    tf1.write_all_or_throw(std::string("testfile"));
    writer->send_file_segment(tf1, 1, 6);
    tf1.lseek(0);
    writer->send_from_pipe(tf1);

    std::string written(buffer.begin(), buffer.end());
    wassert(actual(written) == "testline\ntestbufestfiltestfile");
});

}

}
