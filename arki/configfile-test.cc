#include "config.h"

#include <arki/tests/tests.h>
#include <arki/configfile.h>

#include <sstream>
#include <iostream>

namespace {
using namespace std;
using namespace arki;
using namespace arki::tests;

class Tests : public TestCase
{
    using TestCase::TestCase;
    void register_tests() override;
} test("arki_configfile");

void Tests::register_tests() {

add_method("parse", [] {
    // Check that simple key = val items are parsed correctly
    string test =
        "\n" // Empty line
        "#\n" // Empty comment
        "# Comment at the beginning\n"
        "   # Comment after some spaces\n"
        "; Comment at the beginning\n"
        "   ; Comment after some spaces\n"
        "zero = 0\n"
        " uno = 1  \n"
        "due=2\n"
        "  t r e  = 3\n"
        "\n";
    ConfigFile conf;
    conf.parse(test, "(memory)");

    size_t count = 0;
    for (ConfigFile::const_iterator i = conf.begin(); i != conf.end(); ++i)
        ++count;
    wassert(actual(count) == 4u);

    wassert(actual(conf.value("zero")) == "0");
    wassert(actual(conf.value("uno")) == "1");
    wassert(actual(conf.value("due")) == "2");
    wassert(actual(conf.value("t r e")) == "3");
    conf.setValue("due", "DUE");
    wassert(actual(conf.value("due")) == "DUE");
});

}

}
