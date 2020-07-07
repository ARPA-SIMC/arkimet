#include "arki/tests/tests.h"
#include "arki/core/file.h"
#include "json.h"
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
} test("arki_structured_json");

void Tests::register_tests() {

// null value
add_method("null", [] {
    stringstream str;
    JSON json(str);
    json.add_null();
    wassert(actual(str.str()) == "null");

    Memory m;
    JSON::parse(str.str(), m);
    wassert(actual(m.root().type()) = NodeType::NONE);
});

// bool value
add_method("bool", [] {
    {
        stringstream str;
        JSON json(str);
        json.add(true);
        wassert(actual(str.str()) == "true");

        Memory m;
        JSON::parse(str.str(), m);
        wassert(actual(m.root().type()) == NodeType::BOOL);
    }

    {
        stringstream str;
        JSON json(str);
        json.add(false);
        wassert(actual(str.str()) == "false");

        Memory m;
        JSON::parse(str.str(), m);
        wassert(actual(m.root().type()) == NodeType::BOOL);
    }
});

// int value
add_method("int", [] {
    {
        stringstream str;
        JSON json(str);
        json.add(1);
        wassert(actual(str.str()) == "1");

        Memory m;
        JSON::parse(str.str(), m);
        wassert(actual(m.root().type()) == NodeType::INT);
        wassert(actual(m.root().as_int("test")) == 1);
    }

    {
        stringstream str;
        JSON json(str);
        json.add(-1234567);
        wassert(actual(str.str()) == "-1234567");

        Memory m;
        JSON::parse(str.str(), m);
        wassert(actual(m.root().type()) == NodeType::INT);
        wassert(actual(m.root().as_int("test")) == -1234567);
    }
});

// double value
add_method("double", [] {
    {
        stringstream str;
        JSON json(str);
        json.add(1.1);
        wassert(actual(str.str()) == "1.1");

        Memory m;
        JSON::parse(str.str(), m);
        wassert(actual(m.root().type()) == NodeType::DOUBLE);
        wassert(actual(m.root().as_double("test")) == 1.1);
    }

    {
        stringstream str;
        JSON json(str);
        json.add(-1.1);
        wassert(actual(str.str()) == "-1.1");

        Memory m;
        JSON::parse(str.str(), m);
        wassert(actual(m.root().type()) == NodeType::DOUBLE);
        wassert(actual(m.root().as_double("test")) == -1.1);
    }

    {
        stringstream str;
        JSON json(str);
        json.add(1.0);
        wassert(actual(str.str()) == "1.0");

        Memory m;
        JSON::parse(str.str(), m);
        wassert(actual(m.root().type()) == NodeType::DOUBLE);
        wassert(actual(m.root().as_double("test")) == 1.0);
    }

    {
        stringstream str;
        JSON json(str);
        json.add(-1.0);
        wassert(actual(str.str()) == "-1.0");

        Memory m;
        JSON::parse(str.str(), m);
        wassert(actual(m.root().type()) == NodeType::DOUBLE);
        wassert(actual(m.root().as_double("test")) == -1.0);
    }
});

// string value
add_method("string", [] {
    {
        stringstream str;
        JSON json(str);
        json.add("");
        wassert(actual(str.str()) == "\"\"");

        Memory m;
        JSON::parse(str.str(), m);
        wassert(actual(m.root().type()) == NodeType::STRING);
        wassert(actual(m.root().as_string("test")) == "");
    }

    {
        stringstream str;
        JSON json(str);
        json.add("antani");
        wassert(actual(str.str()) == "\"antani\"");

        Memory m;
        JSON::parse(str.str(), m);
        wassert(actual(m.root().type()) == NodeType::STRING);
        wassert(actual(m.root().as_string("test")) == "antani");
    }
});

// list value
add_method("list", [] {
    stringstream str;
    JSON json(str);
    json.start_list();
    json.add("");
    json.add(1);
    json.add(1.0);
    json.end_list();
    wassert(actual(str.str()) == "[\"\",1,1.0]");

    Memory m;
    JSON::parse(str.str(), m);
    wassert(actual(m.root().type()) == NodeType::LIST);
    // wassert(actual(m.root().get_list()[0].is_string(), true);
    wassert(actual(m.root().as_string(0, "test")) == "");
    // wassert(actual(m.root().get_list()[1].is_int(), true);
    wassert(actual(m.root().as_int(1, "test")) == 1);
    // wassert(actual(m.root().get_list()[2].is_double(), true);
    wassert(actual(m.root().as_double(2, "test")) == 1.0);
});

// mapping value
add_method("mapping", [] {
    stringstream str;
    JSON json(str);
    json.start_mapping();
    json.add("", 1);
    json.add("antani", 1.0);
    json.end_mapping();
    wassert(actual(str.str()) == "{\"\":1,\"antani\":1.0}");

    Memory m;
    JSON::parse(str.str(), m);
    wassert(actual(m.root().type()) == NodeType::MAPPING);
    // ensure_equals(m.root().get_mapping()[""].is_int(), true);
    wassert(actual(m.root().as_int("", "test")) == 1);
    // ensure_equals(m.root().get_mapping()["antani"].is_double(), true);
    wassert(actual(m.root().as_double("antani", "test")) == 1.0);
});

}

}
