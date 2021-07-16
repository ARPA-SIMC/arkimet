#include "arki/tests/tests.h"
#include "arki/stream.h"
#include "arki/stream/base.h"
#include "arki/core/fwd.h"
#include <cstring>

namespace arki {
namespace stream {

struct StreamTestsFixture
{
protected:
    std::unique_ptr<StreamOutput> output;

public:
    virtual ~StreamTestsFixture() {}

    StreamOutput& stream() { return *output; }

    void set_output(std::unique_ptr<StreamOutput>&& output);

    virtual std::string streamed_contents();

    stream::SendResult send_line(const void* data, size_t size);
    stream::SendResult send_file_segment(arki::core::NamedFileDescriptor& fd, off_t offset, size_t size);
    stream::SendResult send_buffer(const void* data, size_t size);
};


struct StreamTests : public tests::TestCase
{
    using TestCase::TestCase;

    void register_tests() override;

    virtual std::unique_ptr<StreamTestsFixture> make_fixture() = 0;
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

/**
 * Mock sendfile and splice as if they weren't available on this system
 */
struct DisableSendfileSplice : public MockConcreteSyscalls
{
    DisableSendfileSplice();
};


struct ExpectedSyscallMatch
{
    std::string name;
    size_t index = 0;

    ExpectedSyscallMatch(const std::string& name)
         : name(name) {}
    virtual ~ExpectedSyscallMatch() {}

    std::string tag() const { return std::to_string(index) + ":" + name; }

    [[noreturn]] void fail_mismatch(const char* called)
    {
        std::string msg = tag();
        msg += " was expected but ";
        msg += called;
        msg += " was called instead";
        throw arki::utils::tests::TestFailed((msg));
    }

    virtual ssize_t on_read(int fd, void *buf, size_t count) { fail_mismatch("read"); }
    virtual ssize_t on_write(int fd, const void *buf, size_t count) { fail_mismatch("write"); }
    virtual ssize_t on_writev(int fd, const struct iovec *iov, int iovcnt) { fail_mismatch("writev"); }
    virtual ssize_t on_sendfile(int out_fd, int in_fd, off_t *offset, size_t count) { fail_mismatch("sendfile"); }
    virtual ssize_t on_splice(int fd_in, loff_t *off_in, int fd_out,
                              loff_t *off_out, size_t len, unsigned int flags) { fail_mismatch("splice"); }
    virtual int on_poll(struct pollfd *fds, nfds_t nfds, int timeout) { fail_mismatch("poll"); }
    virtual ssize_t on_pread(int fd, void *buf, size_t count, off_t offset) { fail_mismatch("pread"); }
};

struct ExpectedRead : public ExpectedSyscallMatch
{
    int fd;
    std::string result;
    size_t count;
    ssize_t res;
    int errno_val;

    ExpectedRead(int fd, const std::string& result, size_t count, ssize_t res, int errno_val=0)
        : ExpectedSyscallMatch("read"), fd(fd), result(result), count(count), res(res), errno_val(errno_val)
    {
    }

    ssize_t on_read(int fd, void *buf, size_t count) override
    {
        ARKI_UTILS_TEST_INFO(info);
        info << tag();
        using arki::utils::tests::actual;
        wassert(actual(fd) == this->fd);
        wassert(actual(count) == this->count);
        memcpy(buf, result.data(), result.size());
        errno = errno_val;
        return res;
    }
};

struct ExpectedPread : public ExpectedSyscallMatch
{
    int fd;
    off_t offset;
    size_t count;
    std::string result;
    ssize_t res;
    int errno_val;

    ExpectedPread(int fd, off_t offset, size_t count, const std::string& result, ssize_t res, int errno_val=0)
        : ExpectedSyscallMatch("pread"), fd(fd), offset(offset), count(count), result(result), res(res), errno_val(errno_val)
    {
    }

    ssize_t on_pread(int fd, void *buf, size_t count, off_t offset) override
    {
        ARKI_UTILS_TEST_INFO(info);
        info << tag();
        using arki::utils::tests::actual;
        wassert(actual(fd) == this->fd);
        wassert(actual(offset) == this->offset);
        wassert(actual(count) == this->count);
        memcpy(buf, result.data(), result.size());
        errno = errno_val;
        return res;
    }
};

struct ExpectedWrite : public ExpectedSyscallMatch
{
    int fd;
    std::string expected;
    ssize_t res;
    int errno_val;

    ExpectedWrite(int fd, const std::string& expected, ssize_t res, int errno_val=0)
        : ExpectedSyscallMatch("write"), fd(fd), expected(expected), res(res), errno_val(errno_val)
    {
    }

    ssize_t on_write(int fd, const void *buf, size_t count) override
    {
        ARKI_UTILS_TEST_INFO(info);
        info << tag();
        using arki::utils::tests::actual;
        wassert(actual(fd) == this->fd);
        wassert(actual(std::string((const char*)buf, count)) == expected);
        errno = errno_val;
        return res;
    }
};

struct ExpectedWritev : public ExpectedSyscallMatch
{
    int fd;
    std::vector<std::string> expected;
    ssize_t res;
    int errno_val;

    ExpectedWritev(int fd, const std::vector<std::string>& expected, ssize_t res, int errno_val=0)
        : ExpectedSyscallMatch("writev"), fd(fd), expected(expected), res(res), errno_val(errno_val)
    {
    }

    ssize_t on_writev(int fd, const struct iovec *iov, int iovcnt) override
    {
        ARKI_UTILS_TEST_INFO(info);
        info << tag();
        using arki::utils::tests::actual;
        wassert(actual(fd) == this->fd);
        wassert(actual((unsigned)iovcnt) == expected.size());
        for (unsigned i = 0; i < (unsigned)iovcnt; ++i)
            wassert(actual(std::string((const char*)iov[i].iov_base, iov[i].iov_len)) == expected[i]);
        errno = errno_val;
        return res;
    }
};

struct ExpectedPoll : public ExpectedSyscallMatch
{
    int fd;
    short events;
    int timeout;
    short revents;
    ssize_t res;
    int errno_val;

    ExpectedPoll(int fd, short events, int timeout, short revents, ssize_t res, int errno_val=0)
        : ExpectedSyscallMatch("poll"), fd(fd), events(events), timeout(timeout), revents(revents), res(res), errno_val(errno_val)
    {
    }

    int on_poll(struct pollfd *fds, nfds_t nfds, int timeout) override
    {
        ARKI_UTILS_TEST_INFO(info);
        info << tag();
        using arki::utils::tests::actual;

        struct pollfd* current = nullptr;
        for (unsigned i = 0; i < nfds; ++i)
        {
            if (fds[i].fd == -1)
                continue;
            fds[i].revents = 0;
            if (fds[i].fd == fd)
                current = &fds[i];
        }

        if (!current)
            wfail_test("expected file descriptor not found in input struct pollfd's");

        wassert(actual(current->events) == events);
        wassert(actual(timeout) == this->timeout);
        current->revents = revents;
        errno = errno_val;
        return res;
    }
};

struct ExpectedSendfile : public ExpectedSyscallMatch
{
    int out_fd;
    int in_fd;
    off_t offset;
    size_t count;
    off_t roffset;
    ssize_t res;
    int errno_val;

    ExpectedSendfile(int out_fd, int in_fd, off_t offset, size_t count, off_t roffset, ssize_t res, int errno_val=0)
        : ExpectedSyscallMatch("sendfile"), out_fd(out_fd), in_fd(in_fd),
          offset(offset), count(count), roffset(roffset), res(res),
          errno_val(errno_val)
    {
    }

    ssize_t on_sendfile(int out_fd, int in_fd, off_t *offset, size_t count) override
    {
        ARKI_UTILS_TEST_INFO(info);
        info << tag();
        using arki::utils::tests::actual;
        wassert(actual(out_fd) == this->out_fd);
        wassert(actual(in_fd) == this->in_fd);
        wassert(actual(*offset) == this->offset);
        wassert(actual(count) == this->count);

        *offset = roffset;
        errno = errno_val;
        return res;
    }
};

struct ExpectedSplice : public ExpectedSyscallMatch
{
    int fd_in;
    int fd_out;
    size_t len;
    unsigned int flags;
    ssize_t res;
    int errno_val;

    ExpectedSplice(int fd_in, int fd_out, size_t len, unsigned int flags, ssize_t res, int errno_val=0)
        : ExpectedSyscallMatch("splice"), fd_in(fd_in), fd_out(fd_out),
          len(len), flags(flags), res(res), errno_val(errno_val)
    {
    }

    ssize_t on_splice(int fd_in, loff_t *off_in, int fd_out,
                              loff_t *off_out, size_t len, unsigned int flags) override
    {
        ARKI_UTILS_TEST_INFO(info);
        info << tag();
        using arki::utils::tests::actual;
        wassert(actual(fd_in) == this->fd_in);
        wassert_false(off_in);
        wassert(actual(fd_out) == this->fd_out);
        wassert_false(off_out);
        wassert(actual(len) == this->len);
        wassert(actual(flags) == this->flags);

        errno = errno_val;
        return res;
    }
};

struct ExpectedSyscalls : public stream::MockConcreteSyscalls
{
    std::vector<ExpectedSyscallMatch*> expected;

    std::unique_ptr<ExpectedSyscallMatch> pop(const char* name);

    ExpectedSyscalls(std::vector<ExpectedSyscallMatch*> expected);
    ~ExpectedSyscalls();

    ssize_t on_read(int fd, void *buf, size_t count);
    ssize_t on_write(int fd, const void *buf, size_t count);
    ssize_t on_writev(int fd, const struct iovec *iov, int iovcnt);
    ssize_t on_sendfile(int out_fd, int in_fd, off_t *offset, size_t count);
    ssize_t on_splice(int fd_in, loff_t *off_in, int fd_out,
                      loff_t *off_out, size_t len, unsigned int flags);
    int on_poll(struct pollfd *fds, nfds_t nfds, int timeout);
    ssize_t on_pread(int fd, void *buf, size_t count, off_t offset);

    void check_not_called();
};


}
}
