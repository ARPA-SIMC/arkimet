#include <arki/metadata/tests.h>
#include <arki/formatter.h>
#include <arki/metadata.h>
#include "arki/core/file.h"
#include <memory>

namespace {
using namespace std;
using namespace arki;
using namespace arki::core;
using namespace arki::tests;
using namespace arki::utils;

class Tests : public TypeTestCase<types::Source>
{
    using TypeTestCase::TypeTestCase;
    void register_tests() override;
} test("arki_formatter");

void Tests::register_tests() {

// See if the formatter makes a difference
add_method("yaml", [] {
    Metadata md;
    arki::tests::fill(md);

    unique_ptr<Formatter> formatter(Formatter::create());

    std::string str1 = md.to_yaml();
    std::string str2 = md.to_yaml(formatter.get());

    // They must be different
    wassert(actual(str1) != str2);

    // str2 contains annotations, so it should be longer
    wassert(actual(str1.size()) < str2.size());

    // Read back the two metadatas
    Metadata md1;
    {
        string s(str1);
        auto reader = LineReader::from_chars(s.data(), s.size());
        md1.readYaml(*reader, "(test memory buffer)");
    }
    Metadata md2;
    {
        string s(str2);
        auto reader = LineReader::from_chars(s.data(), s.size());
        md2.readYaml(*reader, "(test memory buffer)");
    }

    // Once reparsed, they should have the same content
    wassert(actual(md) == md1);
    wassert(actual(md) == md2);
});

}

}
