#include "arki/tests/tests.h"
#include "regexp.h"

using namespace std;
using namespace arki::utils;
using namespace arki::tests;

namespace {

class Tests : public TestCase
{
    using TestCase::TestCase;

    void register_tests() override;
} test("arki_utils_regexp");

void Tests::register_tests() {

add_method("basic_match", [] {
    Regexp re("^fo\\+bar()$");
    wassert(actual(re.match("fobar()")));
    wassert(actual(re.match("foobar()")));
    wassert(actual(re.match("fooobar()")));
    wassert(actual(!re.match("fbar()")));
    wassert(actual(!re.match(" foobar()")));
    wassert(actual(!re.match("foobar() ")));
});

add_method("extended_match", [] {
    ERegexp re("^fo+bar()$");
    wassert(actual(re.match("fobar")));
    wassert(actual(re.match("foobar")));
    wassert(actual(re.match("fooobar")));
    wassert(actual(!re.match("fbar")));
    wassert(actual(!re.match(" foobar")));
    wassert(actual(!re.match("foobar ")));
});

add_method("capture", [] {
    ERegexp re("^f(o+)bar([0-9]*)$", 3);
    wassert(actual(re.match("fobar")));
    wassert(actual(re[0]) == "fobar");
    wassert(actual(re[1]) == "o");
    wassert(actual(re[2]) == "");
    wassert(actual(re.matchStart(0)) == 0u);
    wassert(actual(re.matchEnd(0)) == 5u);
    wassert(actual(re.matchLength(0)) == 5u);
    wassert(actual(re.matchStart(1)) == 1u);
    wassert(actual(re.matchEnd(1)) == 2u);
    wassert(actual(re.matchLength(1)) == 1u);

    wassert(re.match("foobar42"));
    wassert(actual(re[0]) == "foobar42");
    wassert(actual(re[1]) == "oo");
    wassert(actual(re[2]) == "42");
});

add_method("tokenize", [] {
    string str("antani blinda la supercazzola!");
    Tokenizer tok(str, "[a-z]+", REG_EXTENDED);
    Tokenizer::const_iterator i = tok.begin();

    wassert(actual(i != tok.end()));
    wassert(actual(*i) == "antani");
    ++i;
    wassert(actual(i != tok.end()));
    wassert(actual(*i) == "blinda");
    ++i;
    wassert(actual(i != tok.end()));
    wassert(actual(*i) == "la");
    ++i;
    wassert(actual(i != tok.end()));
    wassert(actual(*i) == "supercazzola");
    ++i;
    wassert(actual(i == tok.end()));
});

add_method("splitter", [] {
    Splitter splitter("[ \t]+or[ \t]+", REG_EXTENDED | REG_ICASE);
    Splitter::const_iterator i = splitter.begin("a or b OR c   or     dadada");
    wassert(actual(*i) == "a");
    wassert(actual(i->size()) == 1u);
    ++i;
    wassert(actual(*i) == "b");
    wassert(actual(i->size()) == 1u);
    ++i;
    wassert(actual(*i) == "c");
    wassert(actual(i->size()) == 1u);
    ++i;
    wassert(actual(*i) == "dadada");
    wassert(actual(i->size()) == 6u);
    ++i;
    wassert(i == splitter.end());
});

add_method("splitter_empty", [] {
    Splitter splitter("Z*", REG_EXTENDED | REG_ICASE);
    Splitter::const_iterator i = splitter.begin("ciao");
    wassert(actual(*i) == "c");
    wassert(actual(i->size()) == 1u);
    ++i;
    wassert(actual(*i) == "i");
    wassert(actual(i->size()) == 1u);
    ++i;
    wassert(actual(*i) == "a");
    wassert(actual(i->size()) == 1u);
    ++i;
    wassert(actual(*i) == "o");
    wassert(actual(i->size()) == 1u);
    ++i;
    wassert(i == splitter.end());
});

}

}
