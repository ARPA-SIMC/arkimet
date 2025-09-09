#include "arki/core/binary.h"
#include "arki/core/cfg.h"
#include "arki/core/lock.h"
#include "arki/metadata.h"
#include "arki/metadata/data.h"
#include "arki/metadata/reader.h"
#include "arki/segment.h"
#include "arki/stream.h"
#include "arki/stream/filter.h"
#include "arki/tests/tests.h"
#include "arki/types/source/blob.h"
#include "arki/utils/sys.h"
#include "postprocess.h"
#include <csetjmp>
#include <signal.h>
#include <unistd.h>

#ifndef STDERR_FILENO
#define STDERR_FILENO 2
#endif

namespace {
using namespace std;
using namespace arki;
using namespace arki::core;
using namespace arki::tests;
using namespace arki::utils;
using namespace arki::metadata;

bool process(const std::string source, const std::string& command,
             const core::cfg::Section& cfg, StreamOutput& out,
             unsigned repeat = 1)
{
    metadata::DataManager& data_manager = metadata::DataManager::get();
    std::vector<std::string> args =
        metadata::Postprocess::validate_command(command, cfg);
    out.start_filter(args);
    try
    {
        for (unsigned i = 0; i < repeat; ++i)
        {
            core::File in(source, O_RDONLY);
            metadata::BinaryReader md_reader(in);
            if (!md_reader.read_all([&](std::shared_ptr<Metadata> md) {
                    md->set_source_inline(
                        DataFormat::BUFR,
                        data_manager.to_data(
                            DataFormat::BUFR,
                            std::vector<uint8_t>(md->sourceBlob().size)));
                    return metadata::Postprocess::send(md, out);
                }))
                return false;
        }
    }
    catch (...)
    {
        out.abort_filter();
        throw;
    }
    auto flt = out.stop_filter();
    flt->check_for_errors();
    return true;
}

bool process(const std::string source, const std::string& command,
             StreamOutput& out, unsigned repeat = 1)
{
    core::cfg::Section cfg;
    return process(source, command, cfg, out, repeat);
}

class Tests : public TestCase
{
    using TestCase::TestCase;
    void register_tests() override;
} test("arki_metadata_postprocess");

void Tests::register_tests()
{

    // See if the postprocess makes a difference
    add_method("null_validate", [] {
        string conf = "type = test\n"
                      "step = daily\n"
                      "filter = origin: GRIB1\n"
                      "index = origin, reftime\n"
                      "postprocess = null\n"
                      "name = testall\n"
                      "path = testall\n";
        auto config = core::cfg::Section::parse(conf, "(memory)");

        std::vector<uint8_t> out;
        auto stream = StreamOutput::create(out);
        process("inbound/test.grib1.arkimet", "null", *config, *stream);
        wassert_true(out.empty());
    });

    // Check that it works without validation, too
    add_method("null", [] {
        std::vector<uint8_t> out;
        auto stream = StreamOutput::create(out);
        process("inbound/test.grib1.arkimet", "null", *stream);
        wassert_true(out.empty());
    });

    add_method("countbytes", [] {
        auto out    = std::make_shared<sys::Tempfile>();
        auto stream = StreamOutput::create(out);
        process("inbound/test.grib1.arkimet", "countbytes", *stream);
        wassert(actual(sys::read_file(out->path())) == "44937\n");
    });

    add_method("cat", [] {
        auto session = std::make_shared<segment::Session>("inbound");
        auto segment = session->segment_from_relpath_and_format(
            "test.grib1", DataFormat::GRIB);
        auto reader =
            segment->reader(std::make_shared<core::lock::NullReadLock>());

        // Get the normal data
        vector<uint8_t> plain;
        {
            core::BinaryEncoder enc(plain);
            reader->read_all([&](std::shared_ptr<Metadata> md) {
                md->makeInline();
                md->encodeBinary(enc);
                const auto& data = md->get_data().read();
                enc.add_raw(data);
                return true;
            });
        }

        // Get the postprocessed data
        auto out    = std::make_shared<sys::Tempfile>();
        auto stream = StreamOutput::create(out);
        process("inbound/test.grib1.arkimet", "cat", *stream);
        std::string postprocessed = sys::read_file(out->path());
        wassert(actual(vector<uint8_t>(postprocessed.begin(),
                                       postprocessed.end()) == plain));
    });

    // Try to shift a sizeable chunk of data to the postprocessor
    add_method("countbytes_large", [] {
        auto out    = std::make_shared<sys::Tempfile>();
        auto stream = StreamOutput::create(out);
        process("inbound/test.grib1.arkimet", "countbytes", *stream, 128);
        wassert(actual(out->path()).contents_equal("5751936\n"));
    });

    add_method("zeroes_arg", [] {
        const char* fname = "postprocess_output";
        stringstream str;

        auto fd = std::make_shared<sys::File>(
            fname, O_WRONLY | O_CREAT | O_NOCTTY, 0666);
        auto stream = StreamOutput::create(fd);
        process("inbound/test.grib1.arkimet", "zeroes 4096", *stream);
        wassert(actual(sys::size(fname)) == 4096 * 1024u);

        sys::unlink(fname);
    });

    add_method("zeroes_arg_large", [] {
        const char* fname = "postprocess_output";
        stringstream str;
        auto fd = std::make_shared<sys::File>(
            fname, O_WRONLY | O_CREAT | O_NOCTTY, 0666);
        auto stream = StreamOutput::create(fd);
        process("inbound/test.grib1.arkimet", "zeroes 4096", *stream, 128);
        wassert(actual(sys::size(fname)) == 4096 * 1024u);

        sys::unlink(fname);
    });

    add_method("issue209", [] {
        auto fd     = std::make_shared<sys::File>("/dev/null", O_WRONLY);
        auto stream = StreamOutput::create(fd);
        process("inbound/issue209.arkimet", "cat", *stream);

        // wassert(actual(p.flush()) == stream::SendResult(24482266));
    });

    add_method("partialread", [] {
        auto fd     = std::make_shared<sys::File>("/dev/null", O_WRONLY);
        auto stream = StreamOutput::create(fd);
        try
        {
            process("inbound/issue209.arkimet", "partialread", *stream);
        }
        catch (std::runtime_error& e)
        {
            wassert(actual(e.what()).matches(
                R"(cannot run postprocessing filter: postprocess command \".+partialread\" exited with code 1; stderr: test: simulating stopping read with an error)"));
        }

        // wassert(actual(stream_result) == stream::SendResult(0));
    });
}

} // namespace
