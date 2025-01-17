#include "arki/segment/tests.h"
#include "arki/core/lock.h"
#include "arki/utils/sys.h"
#include "arki/utils/string.h"
#include "segment.h"
#include "segment/metadata.h"

namespace {
using namespace std;
using namespace arki;
using namespace arki::utils;
using namespace arki::tests;

class Tests : public TestCase
{
    using TestCase::TestCase;
    void register_tests() override;
} test("arki_segment");

void Tests::register_tests() {

}

}

