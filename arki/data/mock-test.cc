#include "arki/metadata/tests.h"
#include "mock.h"
#include <thread>

namespace {
using namespace std;
using namespace arki;
using namespace arki::tests;
using namespace arki::utils;

class Tests : public TestCase
{
    using TestCase::TestCase;
    void register_tests() override;
} test("arki_data_mock");

struct EngineUseTest : public TestSubprocess
{
    int main() noexcept override
    {
        try
        {
            std::shared_ptr<Metadata> md = data::MockEngine::get().by_checksum(
                "c6813ad551ced8a788310f177b3485652ea0b3b7c2a"
                "ca25956a2448f59268391");
            wassert_true(md.get());
            return 0;
        }
        catch (std::exception& e)
        {
            fprintf(stderr, "EngineUseTest: %s\n", e.what());
            return 1;
        }
    }
};

void Tests::register_tests()
{
    add_method("engine_threading", []() {
        // Multithreading is ok
        data::MockEngine& engine      = data::MockEngine::get();
        std::shared_ptr<Metadata> md1 = engine.by_checksum(
            "c6813ad551ced8a788310f177b3485652ea0b3b7c2aca25956a2448f59268391");
        wassert_true(md1.get());

        std::thread subthread([&]() {
            std::shared_ptr<Metadata> md2 =
                engine.by_checksum("c6813ad551ced8a788310f177b3485652ea0b3b7c2a"
                                   "ca25956a2448f59268391");
            wassert_true(md2.get());
        });
        subthread.join();

        std::shared_ptr<Metadata> md3 = engine.by_checksum(
            "c6813ad551ced8a788310f177b3485652ea0b3b7c2aca25956a2448f59268391");
        wassert_true(md3.get());
    });

    add_method("engine_multiprocessing", []() {
        // Multiprocessing needs to use MockEngine::get()
        data::MockEngine& engine      = data::MockEngine::get();
        std::shared_ptr<Metadata> md1 = engine.by_checksum(
            "c6813ad551ced8a788310f177b3485652ea0b3b7c2aca25956a2448f59268391");
        wassert_true(md1.get());

        EngineUseTest eut;
        eut.start();
        eut.wait();

        std::shared_ptr<Metadata> md3 = engine.by_checksum(
            "c6813ad551ced8a788310f177b3485652ea0b3b7c2aca25956a2448f59268391");
        wassert_true(md3.get());
    });
}

} // namespace
