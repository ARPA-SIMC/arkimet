#include "arki/tests/tests.h"
#include "arki/stream.h"
#include "arki/core/fwd.h"

namespace arki {
namespace stream {

class BlockingSink
{
    int fds[2];
    int pipe_sz;
    std::unique_ptr<core::NamedFileDescriptor> outfd;
    std::vector<uint8_t> filler;

public:
    BlockingSink();
    ~BlockingSink();

    core::NamedFileDescriptor& fd() { return *outfd; }
    size_t pipe_size() const { return pipe_sz; }

    /**
     * Return how much data is currently present in the pipe buffer
     */
    size_t data_in_pipe();

    /**
     * If size is positive, send size bytes to the pipe.
     *
     * If size is negative, send pipe_size - size bytes to the pipe.
     */
    void fill(int size);

    /// Flush the pipe buffer until it's completely empty
    void empty_buffer();
};


struct WriteTest
{
    StreamOutput& writer;
    BlockingSink& sink;
    std::vector<size_t> cb_log;
    size_t total_written = 0;

    WriteTest(StreamOutput& writer, BlockingSink& sink, int prefill);
    ~WriteTest();
};


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

    virtual std::unique_ptr<StreamTestsFixture> make_concrete_fixture(core::NamedFileDescriptor& out) = 0;
};

}
}
