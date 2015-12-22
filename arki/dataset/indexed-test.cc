#include "tests.h"
#include "indexed.h"

namespace {
using namespace std;
using namespace arki;
using namespace arki::tests;
using namespace arki::utils;

class Tests : public TestCase
{
    using TestCase::TestCase;

    void register_tests() override
    {
        add_method("empty", [] {
        });
    }
} test("arki_dataset_indexed");

}

