#include "config.h"

#include <arki/tests/tests.h>
#include <arki/runtime.h>

namespace {
using namespace std;
using namespace arki;
using namespace arki::tests;

def_tests(arki_runtime);

void Tests::register_tests() {

add_method("empty", [] {
});

}

}
