#include "config.h"
#include "arki/tests/tests.h"
#include "arki/dataset/test-scenario.h"
#include "arki/utils/sys.h"
#include "arki/utils/string.h"

namespace {
using namespace std;
using namespace arki;
using namespace arki::utils;
using namespace arki::tests;

def_tests(arki_dataset_test_scenario);

void Tests::register_tests() {

// Simple generation
add_method("archived", [] {
    using namespace dataset;
    /* const test::Scenario& scen = */ wcallchecked(test::Scenario::get("ondisk2-archived"));
});

add_method("manyarchivestates", [] {
    using namespace dataset;
    const test::Scenario& scen = wcallchecked(test::Scenario::get("ondisk2-manyarchivestates"));
    ensure(sys::exists(str::joinpath(scen.path, ".archive/offline.summary")));
    ensure(!sys::exists(str::joinpath(scen.path, ".archive/offline")));
    ensure(sys::exists(str::joinpath(scen.path, ".archive/wrongro.summary")));
    ensure(sys::exists(str::joinpath(scen.path, ".archive/ro.summary")));
});

}
}
