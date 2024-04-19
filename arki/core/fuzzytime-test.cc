#include "arki/core/tests.h"
#include "fuzzytime.h"

namespace {
using namespace std;
using namespace arki;
using namespace arki::tests;
using arki::core::FuzzyTime;

class Tests : public TestCase
{
    using TestCase::TestCase;
    void register_tests() override;
} test("arki_core_fuzzytime");

static void parses(int ye, int mo=-1, int da=-1, int ho=-1, int mi=-1, int se=-1)
{
    FuzzyTime t(ye, mo, da, ho, mi, se);
    try {
        t.validate();
    } catch (std::invalid_argument& e) {
        std::stringstream ss;
        ss << t.to_string() << " did not validate: "
           << e.what();
        throw TestFailed(ss.str());
    }
}

static void fails(const char* what, int ye, int mo=-1, int da=-1, int ho=-1, int mi=-1, int se=-1)
{
    FuzzyTime t(ye, mo, da, ho, mi, se);

    try {
        t.validate();
    } catch (std::invalid_argument& e) {
        wassert(actual(e.what()) == what);
        return;
    }
    std::stringstream ss;
    ss << t.to_string() << " unexpectedly validated correctly";
    throw TestFailed(ss.str());
}

void Tests::register_tests() {

add_method("validate", [] {
    wassert(parses(2024));
    wassert(parses(2024, 1));
    wassert(parses(2024, 1, 12));
    wassert(parses(2024, 1, 12, 0));
    wassert(parses(2024, 1, 12, 0, 0));
    wassert(parses(2024, 12, 1, 12, 0, 0));
    wassert(fails("month must be between 1 and 12", 2024, 0, 1, 12, 0, 0));
    wassert(fails("day must be between 1 and 31", 2024, 1, 0, 12, 0, 0));
    wassert(fails("month must be between 1 and 12", 2024, 13, 1, 12, 0, 0));
    wassert(parses(2024, 2, 1, 12, 0, 0));
    wassert(parses(2024, 2, 29, 12, 0, 0));
    wassert(fails("day must be between 1 and 29", 2024, 2, 30, 12, 0, 0));
    wassert(fails("day must be between 1 and 28", 2023, 2, 29, 12, 0, 0));
    wassert(parses(2024, 2, 1, 0, 0, 0));
    wassert(fails("hour must be between 0 and 24", 2024, 2, 1, 25, 0, 0));
    wassert(parses(2024, 2, 1, 24, 0, 0));
    wassert(fails("on hour 24, minute must be zero", 2024, 2, 1, 24, 1, 0));
    wassert(fails("on hour 24, second must be zero", 2024, 2, 1, 24, 0, 1));
    wassert(fails("minute must be between 0 and 59", 2024, 2, 1, 23, 60, 0));
    wassert(parses(2024, 2, 1, 23, 59, 60));
    wassert(fails("second must be between 0 and 60", 2024, 2, 1, 23, 59, 61));
});

}

}
