#include "arki/metadata/tests.h"
#include "arki/types/source/blob.h"
#include "arki/utils/sys.h"
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
    add_method("scan_file", [] {
        auto basedir =
            std::filesystem::current_path() / "inbound" / "fixture.odimh5";
        auto src = basedir / "00.odimh5";
        arki::data::MockScanner scanner(DataFormat::ODIMH5);
        std::vector<std::shared_ptr<Metadata>> mds;
        wassert_true(scanner.scan_file(src, [&](std::shared_ptr<Metadata> md_) {
            mds.emplace_back(md_);
            return true;
        }));
        wassert(actual(mds.size()) == 1u);

        wassert(actual_type(mds[0]->source())
                    .is_source_blob(DataFormat::ODIMH5, basedir, "00.odimh5", 0,
                                    49057));
        wassert_true(mds[0]->has_cached_data());
        // read_data fails as the blob is not associated with a reader
        wassert_throws(std::runtime_error, mds[0]->sourceBlob().read_data());
    });

    add_method("scan_pipe", [] {
        auto basedir =
            std::filesystem::current_path() / "inbound" / "fixture.odimh5";
        auto src = basedir / "00.odimh5";
        arki::data::MockScanner scanner(DataFormat::ODIMH5);
        std::vector<std::shared_ptr<Metadata>> mds;
        core::File in(src, O_RDONLY);
        wassert_true(scanner.scan_pipe(in, [&](std::shared_ptr<Metadata> md) {
            mds.emplace_back(md);
            return true;
        }));
        wassert(actual(mds.size()) == 1u);

        wassert(actual_type(mds[0]->source())
                    .is_source_inline(DataFormat::ODIMH5, 49057));
        wassert_true(mds[0]->has_cached_data());
    });

    add_method("scan_data", [] {
        auto basedir         = std::filesystem::current_path() / "inbound";
        auto src             = basedir / "fixture.odimh5" / "00.odimh5";
        std::string data_str = sys::read_file(src);
        std::vector<uint8_t> data(data_str.begin(), data_str.end());

        arki::data::MockScanner scanner(DataFormat::ODIMH5);
        std::shared_ptr<Metadata> md = scanner.scan_data(data);
        wassert_true(md.get());
        wassert(actual_type(md->source())
                    .is_source_inline(DataFormat::ODIMH5, data_str.size()));
        wassert_true(md->has_cached_data());
    });

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
