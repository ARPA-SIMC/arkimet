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

class Tests : public TestCase
{
    using TestCase::TestCase;

    void register_tests() override;
};

Tests test("arki_core_stream");

static std::vector<uint8_t> mkbuf(const std::string& data)
{
    return std::vector<uint8_t>(data.begin(), data.end());
}

void Tests::register_tests() {

add_method("concrete", [] {
    sys::Tempfile tf;
    auto writer = StreamOutput::create(tf);
    writer->send_vm2_line(mkbuf("testline"));
    writer->send_buffer("testbuf", 7);
    sys::Tempfile tf1;
    tf1.write_all_or_throw(std::string("testfile"));
    writer->send_file_segment(tf1, 1, 6);

    tf.lseek(0);

    char buf[256];
    wassert(actual(tf.read(buf, 256)) == 22u);
    buf[22] = 0;
    wassert(actual(buf) == "testline\ntestbufestfil");
    // virtual size_t send_vm2_line(const std::vector<uint8_t>& line) = 0;
    // virtual size_t send_file_segment(arki::core::NamedFileDescriptor& fd, off_t offset, size_t size) = 0;
    // virtual size_t send_buffer(const std::vector<uint8_t>& buf) = 0;

    // static std::unique_ptr<StreamOutput> create(NamedFileDescriptor& out);
    // static std::unique_ptr<StreamOutput> create(AbstractOutputFile& out);
});

add_method("abstract", [] {
    std::vector<uint8_t> buffer;
    BufferOutputFile out(buffer, "memory buffer");
    auto writer = StreamOutput::create(out);
    writer->send_vm2_line(mkbuf("testline"));
    writer->send_buffer("testbuf", 7);
    sys::Tempfile tf1;
    tf1.write_all_or_throw(std::string("testfile"));
    writer->send_file_segment(tf1, 1, 6);

    std::string written(buffer.begin(), buffer.end());
    wassert(actual(written) == "testline\ntestbufestfil");
});

}

}
