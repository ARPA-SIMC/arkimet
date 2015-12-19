#include "config.h"

#include <arki/tests/tests.h>
#include <arki/iotrace.h>

namespace {
using namespace std;
using namespace arki;
using namespace arki::tests;

def_tests(arki_iotrace);

void Tests::register_tests() {

add_method("empty", [] {
});

}

}
