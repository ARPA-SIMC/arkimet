#include "arki/tests/tests.h"
#include "json.h"
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
} test("arki_emitter_json");

void Tests::register_tests() {

// null value
add_method("null", [] {
    stringstream str;
    JSON json(str);
    json.add_null();
    ensure_equals(str.str(), "null");

    str.seekg(0);
    Memory m;
    JSON::parse(str, m);
    ensure_equals(m.root().is_null(), true);
});

// bool value
add_method("bool", [] {
    {
        stringstream str;
        JSON json(str);
        json.add(true);
        ensure_equals(str.str(), "true");

        str.seekg(0);
        Memory m;
        JSON::parse(str, m);
        ensure_equals(m.root().is_bool(), true);
        ensure_equals(m.root().get_bool(), true);
    }

    {
        stringstream str;
        JSON json(str);
        json.add(false);
        ensure_equals(str.str(), "false");

        str.seekg(0);
        Memory m;
        JSON::parse(str, m);
        ensure_equals(m.root().is_bool(), true);
        ensure_equals(m.root().get_bool(), false);
    }
});

// int value
add_method("int", [] {
    {
        stringstream str;
        JSON json(str);
        json.add(1);
        ensure_equals(str.str(), "1");

        str.seekg(0);
        Memory m;
        JSON::parse(str, m);
        ensure_equals(m.root().is_int(), true);
        ensure_equals(m.root().get_int(), 1);
    }

    {
        stringstream str;
        JSON json(str);
        json.add(-1234567);
        ensure_equals(str.str(), "-1234567");

        str.seekg(0);
        Memory m;
        JSON::parse(str, m);
        ensure_equals(m.root().is_int(), true);
        ensure_equals(m.root().get_int(), -1234567);
    }
});

// double value
add_method("double", [] {
    {
        stringstream str;
        JSON json(str);
        json.add(1.1);
        ensure_equals(str.str(), "1.1");

        str.seekg(0);
        Memory m;
        JSON::parse(str, m);
        ensure_equals(m.root().is_double(), true);
        ensure_equals(m.root().get_double(), 1.1);
    }

    {
        stringstream str;
        JSON json(str);
        json.add(-1.1);
        ensure_equals(str.str(), "-1.1");

        str.seekg(0);
        Memory m;
        JSON::parse(str, m);
        ensure_equals(m.root().is_double(), true);
        ensure_equals(m.root().get_double(), -1.1);
    }

    {
        stringstream str;
        JSON json(str);
        json.add(1.0);
        ensure_equals(str.str(), "1.0");

        str.seekg(0);
        Memory m;
        JSON::parse(str, m);
        ensure_equals(m.root().is_double(), true);
        ensure_equals(m.root().get_double(), 1.0);
    }

    {
        stringstream str;
        JSON json(str);
        json.add(-1.0);
        ensure_equals(str.str(), "-1.0");

        str.seekg(0);
        Memory m;
        JSON::parse(str, m);
        ensure_equals(m.root().is_double(), true);
        ensure_equals(m.root().get_double(), -1.0);
    }
});

// string value
add_method("string", [] {
    {
        stringstream str;
        JSON json(str);
        json.add("");
        ensure_equals(str.str(), "\"\"");

        str.seekg(0);
        Memory m;
        JSON::parse(str, m);
        ensure_equals(m.root().is_string(), true);
        ensure_equals(m.root().get_string(), "");
    }

    {
        stringstream str;
        JSON json(str);
        json.add("antani");
        ensure_equals(str.str(), "\"antani\"");

        str.seekg(0);
        Memory m;
        JSON::parse(str, m);
        ensure_equals(m.root().is_string(), true);
        ensure_equals(m.root().get_string(), "antani");
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
    ensure_equals(str.str(), "[\"\",1,1.0]");

    str.seekg(0);
    Memory m;
    JSON::parse(str, m);
    ensure_equals(m.root().is_list(), true);
    ensure_equals(m.root().get_list()[0].is_string(), true);
    ensure_equals(m.root().get_list()[0].get_string(), "");
    ensure_equals(m.root().get_list()[1].is_int(), true);
    ensure_equals(m.root().get_list()[1].get_int(), 1);
    ensure_equals(m.root().get_list()[2].is_double(), true);
    ensure_equals(m.root().get_list()[2].get_double(), 1.0);
});

// mapping value
add_method("mapping", [] {
    stringstream str;
    JSON json(str);
    json.start_mapping();
    json.add("", 1);
    json.add("antani", 1.0);
    json.end_mapping();
    ensure_equals(str.str(), "{\"\":1,\"antani\":1.0}");

    str.seekg(0);
    Memory m;
    JSON::parse(str, m);
    ensure_equals(m.root().is_mapping(), true);
    ensure_equals(m.root().get_mapping()[""].is_int(), true);
    ensure_equals(m.root().get_mapping()[""].get_int(), 1);
    ensure_equals(m.root().get_mapping()["antani"].is_double(), true);
    ensure_equals(m.root().get_mapping()["antani"].get_double(), 1.0);
});

}

}
