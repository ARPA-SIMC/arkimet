#include "arki/tests/tests.h"
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

Tests test("arki_file");

void Tests::register_tests() {

add_method("bufferedreader", []() {
    // Generate a sample file
    std::string buf;
    for (unsigned i = 0; i < 65536; ++i)
        buf += (char)(i % 256);
    for (unsigned i = 0; i < 32768; ++i)
        buf += (char)((i + 1) % 256);

    // Read as a file
    {
        sys::write_file("testfile", buf);

        File in("testfile", O_RDONLY);
        auto reader(BufferedReader::from_fd(in));
        for (unsigned i = 0; i < 65536; ++i)
        {
            ARKI_UTILS_TEST_INFO(info);
            std::string expected = "offset:" + std::to_string(i);
            info() << expected;
            wassert(actual((unsigned)reader->peek()) == i % 256);
            wassert(actual((unsigned)reader->get()) == i % 256);
        }
        for (unsigned i = 0; i < 32768; ++i)
        {
            ARKI_UTILS_TEST_INFO(info);
            std::string expected = "offset:" + std::to_string(65536 + i);
            info() << expected;
            wassert(actual((unsigned)reader->peek()) == (i + 1) % 256);
            wassert(actual((unsigned)reader->get()) == (i + 1) % 256);
        }
        wassert(actual(reader->peek()) == EOF);
        wassert(actual(reader->get()) == EOF);

        sys::unlink("testfile");
    }

    // Read as a string
    {
        auto reader(BufferedReader::from_string(buf));
        for (unsigned i = 0; i < 65536; ++i)
        {
            ARKI_UTILS_TEST_INFO(info);
            std::string expected = "offset:" + std::to_string(i);
            info() << expected;
            wassert(actual((unsigned)reader->peek()) == i % 256);
            wassert(actual((unsigned)reader->get()) == i % 256);
        }
        for (unsigned i = 0; i < 32768; ++i)
        {
            ARKI_UTILS_TEST_INFO(info);
            std::string expected = "offset:" + std::to_string(65536 + i);
            info() << expected;
            wassert(actual((unsigned)reader->peek()) == (i + 1) % 256);
            wassert(actual((unsigned)reader->get()) == (i + 1) % 256);
        }
        wassert(actual(reader->peek()) == EOF);
        wassert(actual(reader->get()) == EOF);
    }
});

add_method("linereader", []() {
    // Generate a sample file
    stringstream ss;
    for (unsigned i = 0; i < 10000; ++i)
        ss << "line" << i << endl;
    sys::write_file("testfile", ss.str());

    File in("testfile", O_RDONLY);
    auto reader(LineReader::from_fd(in));
    string line;
    for (unsigned i = 0; i < 10000; ++i)
    {
        ARKI_UTILS_TEST_INFO(info);
        string expected = "line" + std::to_string(i);
        info() << expected;
        wassert(actual(reader->eof()).isfalse());
        wassert(actual(reader->getline(line)));
        wassert(actual(line) == expected);
    }
    wassert(actual(reader->eof()).isfalse());
    wassert(actual(reader->getline(line)).isfalse());
    wassert(actual(reader->eof()));

    sys::unlink("testfile");
});

}

}
