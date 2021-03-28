#include "stream/tests.h"
#include "stream.h"
#include "arki/utils/sys.h"
#include "arki/core/file.h"
#include <sys/ioctl.h>


using namespace std;
using namespace arki;
using namespace arki::core;
using namespace arki::utils;
using namespace arki::tests;

namespace {

class Tests : public TestCase
{
    using TestCase::TestCase;

    void register_tests() override;
};

Tests test("arki_stream");

void Tests::register_tests() {

add_method("empty", [] {
});

}

}
