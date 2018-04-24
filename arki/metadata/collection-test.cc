#include "arki/libconfig.h"
#include "arki/types/tests.h"
#include "arki/segment.h"
#include "arki/metadata.h"
#include "arki/metadata/collection.h"
#include "arki/utils.h"
#include "arki/utils/accounting.h"
#include "arki/utils/sys.h"
#include "arki/types/source.h"
#include "arki/types/source/blob.h"
#include "arki/summary.h"
#include "arki/scan/any.h"
#include "arki/runtime/io.h"
#include <cstring>
#include <sstream>
#include <iostream>

namespace {
using namespace std;
using namespace arki;
using namespace arki::utils;
using namespace arki::tests;

class Tests : public TestCase
{
    using TestCase::TestCase;
    void register_tests() override;
} test("arki_metadata_collection");

void Tests::register_tests() {

add_method("empty", [] {
});

}

}
