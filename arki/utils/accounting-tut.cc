#include "config.h"

#include <arki/tests/tests.h>
#include <arki/utils/accounting.h>

#include <sstream>
#include <iostream>

namespace tut {
using namespace std;
using namespace arki;
using namespace arki::tests;

struct arki_utils_accounting_shar {
	utils::acct::Counter counter;
	
	arki_utils_accounting_shar() : counter("foo") {}
};
TESTGRP(arki_utils_accounting);

// Simple counter test
def_test(1)
{
    wassert(actual(counter.name()) == "foo");
    wassert(actual(counter.val()) == 0u);
    counter.incr();
    wassert(actual(counter.val()) == 1u);
    counter.incr(10);
    wassert(actual(counter.val()) == 11u);
    counter.reset();
    wassert(actual(counter.val()) == 0u);
}

}

// vim:set ts=4 sw=4:
