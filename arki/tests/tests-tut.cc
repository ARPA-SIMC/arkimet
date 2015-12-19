#include "config.h"

#include <arki/tests/tests.h>
#include <arki/utils.h>
#include <arki/utils/files.h>
#include <arki/utils/sys.h>
#include <sstream>
#include <iostream>
#include <sys/types.h>
#include <sys/stat.h>
#include <fcntl.h>

namespace tut {
using namespace std;
using namespace arki;
using namespace arki::utils;
using namespace arki::tests;

struct arki_tests_shar {
};
TESTGRP(arki_tests);

// Check compareMaps
def_test(1)
{
    wassert(actual(true).istrue());
    wassert(actual(false).isfalse());
    wassert(actual(3) == 3);
    wassert(actual(3) != 4);
    wassert(actual(3) < 4);
    wassert(actual(3) <= 4);
    wassert(actual(3) <= 3);
    wassert(actual(4) > 3);
    wassert(actual(4) >= 3);
    wassert(actual(3) >= 3);
    wassert(actual("ciao").startswith("ci"));
    wassert(actual("ciao").endswith("ao"));
    wassert(actual("ciao").contains("ci"));
    wassert(actual("ciao").contains("ia"));
    wassert(actual("ciao").contains("ao"));
    wassert(actual("ciao").matches("[ia]+"));
}

}
