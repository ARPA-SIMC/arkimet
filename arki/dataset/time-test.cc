#include "arki/tests/tests.h"
#include "arki/dataset/time.h"
#include <ctime>

using namespace std;
using namespace arki::utils;
using namespace arki::tests;

namespace {

class Tests : public TestCase
{
    using TestCase::TestCase;

    void register_tests() override;
} test("arki_dataset_time");

void Tests::register_tests() {

add_method("default", []() {
    time_t now = time(nullptr);
    time_t val = arki::dataset::SessionTime::get().now();
    wassert(actual(val) >= now);
});

add_method("override", []() {
    time_t now = time(nullptr);
    time_t val = arki::dataset::SessionTime::get().now();
    wassert(actual(val) >= now);

    {
        auto t = arki::dataset::SessionTime::local_override(12345);
        time_t val = arki::dataset::SessionTime::get().now();
        wassert(actual(val) == 12345);
    }

    val = arki::dataset::SessionTime::get().now();
    wassert(actual(val) >= now);
});

}

}
