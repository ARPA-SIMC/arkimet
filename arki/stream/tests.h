#include "arki/tests/tests.h"
#include "arki/stream.h"
#include "arki/stream/base.h"
#include "arki/core/fwd.h"

namespace arki {
namespace stream {

struct StreamTestsFixture
{
protected:
    std::unique_ptr<StreamOutput> output;

public:
    std::vector<size_t> cb_log;

    virtual ~StreamTestsFixture() {}

    void set_output(std::unique_ptr<StreamOutput>&& output);

    virtual std::string streamed_contents();

    void set_data_start_callback(std::function<stream::SendResult(StreamOutput&)> f) { output->set_data_start_callback(f); }
    stream::SendResult send_line(const void* data, size_t size);
    stream::SendResult send_file_segment(arki::core::NamedFileDescriptor& fd, off_t offset, size_t size);
    stream::SendResult send_buffer(const void* data, size_t size);
    stream::SendResult send_from_pipe(int fd);
};


struct StreamTests : public tests::TestCase
{
    using TestCase::TestCase;

    void register_tests() override;

    virtual std::unique_ptr<StreamTestsFixture> make_fixture() = 0;
};


struct ConcreteStreamTests : public StreamTests
{
    using StreamTests::StreamTests;

    void register_tests() override;

    virtual std::unique_ptr<StreamTestsFixture> make_concrete_fixture(std::shared_ptr<core::NamedFileDescriptor> out) = 0;
};

/**
 * RAII mocking of syscalls for concrete stream implementations
 */
struct MockConcreteSyscalls
{
    std::function<ssize_t(int fd, void *buf, size_t count)> orig_read;
    std::function<ssize_t(int fd, const void *buf, size_t count)> orig_write;
    std::function<ssize_t(int fd, const struct iovec *iov, int iovcnt)> orig_writev;
    std::function<ssize_t(int out_fd, int in_fd, off_t *offset, size_t count)> orig_sendfile;
    std::function<ssize_t(int fd_in, loff_t *off_in, int fd_out,
                                 loff_t *off_out, size_t len, unsigned int flags)> orig_splice;
    std::function<int(struct pollfd *fds, nfds_t nfds, int timeout)> orig_poll;
    std::function<ssize_t(int fd, void *buf, size_t count, off_t offset)> orig_pread;

    MockConcreteSyscalls();
    ~MockConcreteSyscalls();
};

}
}
