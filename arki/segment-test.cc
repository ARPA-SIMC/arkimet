#include "arki/dataset/tests.h"
#include "arki/dataset/reporter.h"
#include "arki/configfile.h"
#include "arki/metadata.h"
#include "arki/types/source/blob.h"
#include "arki/utils/sys.h"
#include "arki/utils/string.h"
#include "arki/scan/any.h"
#include "segment.h"
#include "segment/lines.h"
#include <algorithm>

namespace {
using namespace std;
using namespace arki;
using namespace arki::dataset;
using namespace arki::types;
using namespace arki::utils;
using namespace arki::tests;

class Tests : public TestCase
{
    using TestCase::TestCase;
    void register_tests() override;
} test("arki_segment");

void Tests::register_tests() {

add_method("empty", [] {
});

}

}
