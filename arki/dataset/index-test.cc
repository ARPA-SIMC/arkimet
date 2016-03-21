#include "config.h"
#include "arki/dataset/tests.h"
#include "index.h"

using namespace std;
using namespace arki;
using namespace arki::utils;
using namespace arki::tests;

namespace {

class Tests : public TestCase
{
    using TestCase::TestCase;

    void register_tests() override
    {
        add_method("empty", []() {
        });
    }
} test("arki_dataset_index");

}
