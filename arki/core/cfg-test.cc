#include "arki/tests/tests.h"
#include "cfg.h"

namespace {
using namespace arki;
using namespace arki::tests;
using namespace arki::core;

class Tests : public TestCase
{
    using TestCase::TestCase;
    void register_tests() override;
} test("arki_core_cfg");

void Tests::register_tests() {

add_method("parse_section", [] {
    // Check that simple key = val items are parsed correctly
    std::string test =
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
    cfg::Section conf = cfg::Section::parse(test, "(memory)");

    size_t count = 0;
    for (const auto& v: conf)
        ++count;
    wassert(actual(count) == 4u);

    wassert(actual(conf.value("zero")) == "0");
    wassert(actual(conf.value("uno")) == "1");
    wassert(actual(conf.value("due")) == "2");
    wassert(actual(conf.value("t r e")) == "3");
    conf.set("due", "DUE");
    wassert(actual(conf.value("due")) == "DUE");
});

add_method("parse_sections", [] {
    // Check that simple key = val items are parsed correctly
    std::string test =
        "\n" // Empty line
        "#\n" // Empty comment
        "# Comment at the beginning\n"
        "   # Comment after some spaces\n"
        "; Comment at the beginning\n"
        "   ; Comment after some spaces\n"
        "[first]\n"
        " uno = 1  \n"
        "[second]\n"
        "due=2\n"
        "  t r e  = 3\n"
        "\n";
    auto conf = cfg::Sections::parse(test, "(memory)");

    size_t count = 0;
    for (const auto& v: conf)
        ++count;
    wassert(actual(count) == 2u);

    wassert_true(conf.section("first"));
    wassert_true(conf.section("second"));
    wassert_false(conf.section("third"));

    const auto* s = conf.section("first");
    wassert(actual(s->value("zero")) == "");
    wassert(actual(s->value("uno")) == "1");
    wassert(actual(s->value("t r e")) == "");
    s = conf.section("second");
    wassert(actual(s->value("uno")) == "");
    wassert(actual(s->value("due")) == "2");
    wassert(actual(s->value("t r e")) == "3");
});

}

}

