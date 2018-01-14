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
	ensure_equals(count, 4u);

    ensure_equals(conf.value("zero"), "0");
    const configfile::Position* info = conf.valueInfo("zero");
    ensure(info != 0);
    ensure_equals(info->pathname, "(memory)");
    wassert(actual(info->lineno) == 7u);

	ensure_equals(conf.value("uno"), "1");
	info = conf.valueInfo("uno");
	ensure(info != 0);
	ensure_equals(info->pathname, "(memory)");
    wassert(actual(info->lineno) == 8u);

	ensure_equals(conf.value("due"), "2");
	info = conf.valueInfo("due");
	ensure(info != 0);
	ensure_equals(info->pathname, "(memory)");
    wassert(actual(info->lineno) == 9u);

	ensure_equals(conf.value("t r e"), "3");
	info = conf.valueInfo("t r e");
	ensure(info != 0);
	ensure_equals(info->pathname, "(memory)");
    wassert(actual(info->lineno) == 10u);

	conf.setValue("due", "DUE");
	ensure_equals(conf.value("due"), "DUE");
	ensure_equals(conf.valueInfo("due"), (void*)NULL);
});

}

}
