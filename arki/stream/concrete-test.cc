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

    ConcreteTestFixture(std::shared_ptr<core::NamedFileDescriptor> out)
        : out(out)
    {
        set_output(
                std::unique_ptr<arki::StreamOutput>(
                    new stream::ConcreteStreamOutputBase<stream::ConcreteTestingBackend>(out)));
    }
};

class Tests : public stream::ConcreteStreamTests
{
    using ConcreteStreamTests::ConcreteStreamTests;

    void register_tests() override;

    std::unique_ptr<stream::StreamTestsFixture> make_fixture() override
    {
        return std::unique_ptr<stream::StreamTestsFixture>(new CommonTestFixture);
    }

    std::unique_ptr<stream::StreamTestsFixture> make_concrete_fixture(std::shared_ptr<core::NamedFileDescriptor> out) override
    {
        return std::unique_ptr<stream::StreamTestsFixture>(new ConcreteTestFixture(out));
    }
};

Tests test("arki_stream_concrete");

void Tests::register_tests() {
ConcreteStreamTests::register_tests();

add_method("empty", [] {
});

}

}
