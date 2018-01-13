#include "arki/tests/tests.h"
#include "arki/core/file.h"
#include "yaml.h"

using namespace std;
using namespace arki;
using namespace arki::core;
using namespace arki::utils;
using namespace arki::tests;

namespace {

class Tests : public TestCase
{
    using TestCase::TestCase;

    void register_tests() override
    {
        add_method("parse", []() {
            string data = 
                "Name: value\n"
                "Multiline: value1\n"
                "  value2\n"
                "   value3\n"
                "Multifield:\n"
                "  Field1: val1\n"
                "  Field2: val2\n"
                "   continue val2\n"
                "\n"
                "Name: second record\n";
            auto reader = LineReader::from_chars(data.data(), data.size());
            YamlStream yamlStream;
            YamlStream::const_iterator i = yamlStream.begin(*reader);
            wassert(actual(i != yamlStream.end()));
            wassert(actual(i->first) == "Name");
            wassert(actual(i->second) == "value");

            ++i;
            wassert(actual(i != yamlStream.end()));
            wassert(actual(i->first) == "Multiline");
            wassert(actual(i->second) ==
                "value1\n"
                "value2\n"
                " value3\n");

            ++i;
            wassert(actual(i != yamlStream.end()));
            wassert(actual(i->first) == "Multifield");
            wassert(actual(i->second) ==
                "Field1: val1\n"
                "Field2: val2\n"
                " continue val2\n");

            ++i;
            wassert(actual(i == yamlStream.end()));

            i = yamlStream.begin(*reader);
            wassert(actual(i != yamlStream.end()));
            wassert(actual(i->first) == "Name");
            wassert(actual(i->second) == "second record");

            ++i;
            wassert(actual(i == yamlStream.end()));

            i = yamlStream.begin(*reader);
            wassert(actual(i == yamlStream.end()));
        });
        add_method("comments", []() {
            string data =
                "# comment\n"
                "Name: value # comment\n"
                "# comment\n"
                "Multiline: value1          #   comment \n"
                "  value2 # a\n"
                "   value3#b\n"
                "\n"
                "# comment\n"
                "\n"
                "Name: second record\n";
            auto reader = LineReader::from_chars(data.data(), data.size());
            YamlStream yamlStream;
            YamlStream::const_iterator i = yamlStream.begin(*reader);
            wassert(actual(i != yamlStream.end()));
            wassert(actual(i->first) == "Name");
            wassert(actual(i->second) == "value");

            ++i;
            wassert(actual(i != yamlStream.end()));
            wassert(actual(i->first) == "Multiline");
            wassert(actual(i->second) ==
                "value1\n"
                "value2 # a\n"
                " value3#b\n");

            ++i;
            wassert(actual(i == yamlStream.end()));

            i = yamlStream.begin(*reader);
            wassert(actual(i != yamlStream.end()));
            wassert(actual(i->first) == "Name");
            wassert(actual(i->second) == "second record");

            ++i;
            wassert(actual(i == yamlStream.end()));

            i = yamlStream.begin(*reader);
            wassert(actual(i == yamlStream.end()));
        });
    }
} test("arki_utils_yaml");

}
