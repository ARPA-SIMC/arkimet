#include "config.h"
#include "arki/tests/tests.h"
#include "arki/dataset/test-scenario.h"
#include "arki/utils/sys.h"
#include "arki/utils/string.h"

namespace tut {
using namespace std;
using namespace arki;
using namespace arki::utils;
using namespace arki::tests;

struct arki_dataset_test_scenario_shar {
    arki_dataset_test_scenario_shar()
    {
    }
};
TESTGRP(arki_dataset_test_scenario);

// Simple generation
template<> template<>
void to::test<1>()
{
    using namespace dataset;
    /* const test::Scenario& scen = */ test::Scenario::get("ondisk2-archived");
}

template<> template<>
void to::test<2>()
{
    using namespace dataset;
    const test::Scenario& scen = test::Scenario::get("ondisk2-manyarchivestates");
    ensure(sys::exists(str::joinpath(scen.path, ".archive/offline.summary")));
    ensure(!sys::exists(str::joinpath(scen.path, ".archive/offline")));
    ensure(sys::exists(str::joinpath(scen.path, ".archive/wrongro.summary")));
    ensure(sys::exists(str::joinpath(scen.path, ".archive/ro.summary")));
}

}
