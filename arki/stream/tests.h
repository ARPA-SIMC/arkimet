#include "arki/tests/tests.h"
#include "arki/stream.h"
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

    virtual std::unique_ptr<StreamTestsFixture> make_concrete_fixture(core::NamedFileDescriptor& out) = 0;
};

}
}
