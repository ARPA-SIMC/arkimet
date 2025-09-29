#include "arki/core/file.h"
#include <arki/formatter.h>
#include <arki/metadata.h>
#include <arki/metadata/reader.h>
#include <arki/metadata/tests.h>
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

void Tests::register_tests()
{

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
        std::shared_ptr<Metadata> md1;
        {
            arki::core::MemoryFile in(str1.data(), str1.size(),
                                      "(memory buffer)");
            metadata::YamlReader reader(in);
            md1 = reader.read();
        }
        std::shared_ptr<Metadata> md2;
        {
            arki::core::MemoryFile in(str2.data(), str2.size(),
                                      "(memory buffer)");
            metadata::YamlReader reader(in);
            md2 = reader.read();
        }

        // Once reparsed, they should have the same content
        wassert(actual(md) == md1);
        wassert(actual(md) == md2);
    });
}

} // namespace
