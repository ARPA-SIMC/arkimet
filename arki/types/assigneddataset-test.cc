#include "tests.h"
#include "assigneddataset.h"

namespace {
using namespace std;
using namespace arki;
using namespace arki::tests;

class Tests : public TypeTestCase<types::AssignedDataset>
{
    using TypeTestCase::TypeTestCase;
    void register_tests() override;
} test("arki_types_assigneddataset");

void Tests::register_tests() {

// Check AssignedDataset, created with current time
add_generic_test("assigneddataset",
        { "pestname as testid imported on 2015-01-03T00:00:00", "testname as pestid imported on 2015-01-03T00:00:00", },
        {
            "testname as testid imported on 2015-01-03T00:00:00",
            // Comparison should not care about the attribution time
            "testname as testid imported on 2015-01-02T00:00:00",
            "testname as testid imported on 2015-01-04T00:00:00",
        },
        {
            "zestname as testid imported on 2015-01-03T00:00:00",
            "testname as zestid imported on 2015-01-03T00:00:00",
        });

}

}
