#include "config.h"

#include <arki/tests/tests.h>
#include <arki/runtime/io.h>
#include <arki/utils/sys.h>

namespace tut {
using namespace std;
using namespace arki;
using namespace arki::runtime;
using namespace arki::utils;
using namespace arki::tests;

struct arki_runtime_io_shar {
};
TESTGRP(arki_runtime_io);

def_test(1)
{
    string name;
    {
        Tempfile foo;
        name = foo.name();
        ensure(sys::exists(name));
    }
    ensure(!sys::exists(name));
}

}
