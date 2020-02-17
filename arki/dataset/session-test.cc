#include "arki/dataset/tests.h"
#include "arki/dataset/session.h"

using namespace std;
using namespace arki;
using namespace arki::utils;
using namespace arki::tests;

namespace {

class Tests : public TestCase
{
    using TestCase::TestCase;

    void register_tests() override;
} test("arki_dataset_session");

void Tests::register_tests() {

add_method("empty", [] {
});

}

}
