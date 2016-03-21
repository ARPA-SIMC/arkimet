#include "config.h"
#include <arki/tests/tests.h>
#include <arki/file.h>
#include <sstream>
#include <iostream>
#include <fcntl.h>

using namespace std;
using namespace arki;
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


