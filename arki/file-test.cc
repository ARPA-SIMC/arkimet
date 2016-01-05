#include "config.h"
#include <arki/tests/tests.h>
#include <arki/binary.h>
#include <sstream>
#include <iostream>

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
});

}

}


