#include "config.h"

#include <arki/tests/tests.h>
#include <arki/transaction.h>

#include <sstream>
#include <fstream>
#include <iostream>

namespace {
using namespace std;
using namespace arki;
using namespace arki::tests;

def_tests(arki_dsindex_pending);

void Tests::register_tests() {

add_method("empty", [] {
});

}

}
