#include "arki/tests/tests.h"
#include "memory.h"

namespace {
using namespace std;
using namespace arki;
using namespace arki::tests;
using namespace arki::emitter;

class Tests : public TestCase
{
    using TestCase::TestCase;
    void register_tests() override;
} test("arki_emitter_memory");

void Tests::register_tests() {

// null value
add_method("null", [] {
    Memory m;
    m.add_null();

    wassert(actual(m.root().tag()) == "null");
    ensure(m.root().is_null());
});

// bool value
add_method("bool", [] {
    {
        Memory m;
        m.add(true);
        wassert(actual(m.root().tag()) == "bool");
        ensure(m.root().is_bool());
        ensure_equals(m.root().get_bool(), true);
    }
    {
        Memory m;
        m.add(false);
        wassert(actual(m.root().tag()) == "bool");
        ensure(m.root().is_bool());
        ensure_equals(m.root().get_bool(), false);
    }
});

// int value
add_method("int", [] {
    Memory m;
    m.add(42);
    wassert(actual(m.root().tag()) == "int");
    ensure(m.root().is_int());
    ensure_equals(m.root().get_int(), 42);
});

// double value
add_method("double", [] {
    {
        Memory m;
        m.add(1.0);
        wassert(actual(m.root().tag()) == "double");
        ensure(m.root().is_double());
        ensure_equals(m.root().get_double(), 1.0);
    }

    {
        Memory m;
        m.add(0.1);
        wassert(actual(m.root().tag()) == "double");
        ensure(m.root().is_double());
        ensure_equals(m.root().get_double(), 0.1);
    }
});

// string value
add_method("string", [] {
    {
        Memory m;
        m.add_string("");
        wassert(actual(m.root().tag()) == "string");
        ensure(m.root().is_string());
        ensure_equals(m.root().get_string(), "");
    }

    {
        Memory m;
        m.add("antani");
        wassert(actual(m.root().tag()) == "string");
        ensure(m.root().is_string());
        ensure_equals(m.root().get_string(), "antani");
    }
});

// list value
add_method("list", [] {
    Memory m;
    m.start_list();
      m.add("antani");
      m.start_list();
        m.add("blinda");
        m.add("supercazzola");
      m.end_list();
      m.add("tapioca");
    m.end_list();

    wassert(actual(m.root().tag()) == "list");
    ensure(m.root().is_list());
    const memory::List& l = m.root().get_list();

    ensure_equals(l.empty(), false);
    ensure_equals(l.size(), 3u);
    ensure_equals(l[0].is_string(), true);
    ensure_equals(l[0].get_string(), "antani");
    ensure_equals(l[1].is_list(), true);
    ensure_equals(l[1].get_list().empty(), false);
    ensure_equals(l[1].get_list().size(), 2u);
    ensure_equals(l[1].get_list()[0].get_string(), "blinda");
    ensure_equals(l[1].get_list()[1].get_string(), "supercazzola");
    ensure_equals(l[2].get_string(), "tapioca");
});

// mapping value
add_method("mapping", [] {
    Memory m;
    m.start_mapping();
      m.add("antani");
      m.start_list();
        m.add("blinda");
        m.add("supercazzola");
      m.end_list();
      m.add("tapioca");
      m.start_mapping();
        m.add("blinda");
        m.add("supercazzola");
      m.end_mapping();
    m.end_mapping();

    wassert(actual(m.root().tag()) == "mapping");
    ensure(m.root().is_mapping());
    const memory::Mapping& i = m.root().get_mapping();

    ensure_equals(i.empty(), false);
    ensure_equals(i.size(), 2u);
    ensure_equals(i["antani"].is_list(), true);
    ensure_equals(i["antani"].get_list()[0].get_string(), "blinda");
    ensure_equals(i["antani"].get_list()[1].get_string(), "supercazzola");
    ensure_equals(i["tapioca"].is_mapping(), true);
    ensure_equals(i["tapioca"].get_mapping().size(), 1u);
    ensure_equals(i["tapioca"].get_mapping()["blinda"].get_string(), "supercazzola");
});

}

}
