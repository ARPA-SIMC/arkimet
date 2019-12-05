#include "arki/tests/tests.h"
#include "memory.h"

namespace {
using namespace std;
using namespace arki;
using namespace arki::tests;
using namespace arki::structured;

class Tests : public TestCase
{
    using TestCase::TestCase;
    void register_tests() override;
} test("arki_structured_memory");

void Tests::register_tests() {

// null value
add_method("null", [] {
    Memory m;
    m.add_null();

    wassert(actual(m.root().tag()) == "null");
    wassert(actual(m.root().type()) = NodeType::NONE);
});

// bool value
add_method("bool", [] {
    {
        Memory m;
        m.add(true);
        wassert(actual(m.root().tag()) == "bool");
        wassert(actual(m.root().type()) == NodeType::BOOL);
        wassert_true(m.root().as_bool("test"));
    }
    {
        Memory m;
        m.add(false);
        wassert(actual(m.root().tag()) == "bool");
        wassert(actual(m.root().type()) == NodeType::BOOL);
        wassert_false(m.root().as_bool("test"));
    }
});

// int value
add_method("int", [] {
    Memory m;
    m.add(42);
    wassert(actual(m.root().tag()) == "int");
    wassert(actual(m.root().type()) == NodeType::INT);
    wassert(actual(m.root().as_int("test")) == 42);
});

// double value
add_method("double", [] {
    {
        Memory m;
        m.add(1.0);
        wassert(actual(m.root().tag()) == "double");
        wassert(actual(m.root().type()) == NodeType::DOUBLE);
        wassert(actual(m.root().as_double("test")) == 1.0);
    }

    {
        Memory m;
        m.add(0.1);
        wassert(actual(m.root().tag()) == "double");
        wassert(actual(m.root().type()) == NodeType::DOUBLE);
        wassert(actual(m.root().as_double("test")) == 0.1);
    }
});

// string value
add_method("string", [] {
    {
        Memory m;
        m.add_string("");
        wassert(actual(m.root().tag()) == "string");
        wassert(actual(m.root().type()) == NodeType::STRING);
        wassert(actual(m.root().as_string("test")) == "");
    }

    {
        Memory m;
        m.add("antani");
        wassert(actual(m.root().tag()) == "string");
        wassert(actual(m.root().type()) == NodeType::STRING);
        wassert(actual(m.root().as_string("test")) == "antani");
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
    wassert(actual(m.root().type()) == NodeType::LIST);
    wassert(actual(m.root().list_size("test")) == 3u);
    wassert(actual(m.root().as_string(0, "test")) == "antani");

    m.root().sub(1, "test", [&](const Reader& reader) {
        wassert(actual(reader.type()) == NodeType::LIST);
        wassert(actual(reader.list_size("test")) == 2u);
        wassert(actual(reader.as_string(0, "test")) == "blinda");
        wassert(actual(reader.as_string(1, "test")) == "supercazzola");
    });

    wassert(actual(m.root().as_string(2, "test")) == "tapioca");
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
    wassert(actual(m.root().type()) == NodeType::MAPPING);

    m.root().sub("antani", "test", [&](const Reader& reader) {
        wassert(actual(reader.type()) == NodeType::LIST);
        wassert(actual(reader.as_string(0, "test")) == "blinda");
        wassert(actual(reader.as_string(1, "test")) == "supercazzola");
    });

    m.root().sub("tapioca", "test", [&](const Reader& reader) {
        wassert(actual(reader.type()) == NodeType::MAPPING);
        wassert(actual(reader.as_string("blinda", "test")) == "supercazzola");
    });
});

}

}
