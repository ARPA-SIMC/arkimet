#include "tests.h"
#include "concrete.h"
#include "arki/utils/sys.h"

using namespace std;
using namespace arki;
using namespace arki::core;
using namespace arki::utils;
using namespace arki::tests;

namespace {

struct CommonTestFixture : public stream::StreamTestsFixture
{
    std::shared_ptr<sys::Tempfile> tf;

    CommonTestFixture()
        : tf(std::make_shared<sys::Tempfile>())
    {
        set_output(StreamOutput::create(tf));
    }

    std::string streamed_contents() override
    {
        std::string res;
        tf->lseek(0);

        char buf[4096];
        while (size_t sz = tf->read(buf, 4096))
            res.append(buf, sz);

        return res;
    }
};

struct ConcreteTestFixture : public stream::StreamTestsFixture
{
    std::shared_ptr<core::NamedFileDescriptor> out;

    ConcreteTestFixture(std::shared_ptr<core::NamedFileDescriptor> out, int timeout_ms=-1)
        : out(out)
    {
        set_output(
                std::unique_ptr<arki::StreamOutput>(
                    new stream::ConcreteStreamOutputBase<stream::ConcreteTestingBackend>(out, timeout_ms)));
    }
};

class ConcreteFallbackTestFixture : public ConcreteTestFixture
{
    using ConcreteTestFixture::ConcreteTestFixture;

    stream::DisableSendfileSplice no_sendfile_and_splice;
};

template<typename Fixture>
class Tests : public stream::ConcreteStreamTests
{
    using ConcreteStreamTests::ConcreteStreamTests;

    void register_tests() override;

    std::unique_ptr<stream::StreamTestsFixture> make_fixture() override
    {
        return std::unique_ptr<stream::StreamTestsFixture>(new CommonTestFixture);
    }

    std::unique_ptr<stream::StreamTestsFixture> make_concrete_fixture(std::shared_ptr<core::NamedFileDescriptor> out, int timeout_ms=-1) override
    {
        return std::unique_ptr<stream::StreamTestsFixture>(new Fixture(out, timeout_ms));
    }
};

Tests<ConcreteTestFixture> test1("arki_stream_concrete");
Tests<ConcreteFallbackTestFixture> test2("arki_stream_concrete_fallback");

template<typename Fixture>
void Tests<Fixture>::register_tests() {
ConcreteStreamTests::register_tests();

add_method("empty", [] {
});

}

}
