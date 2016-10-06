#include "config.h"
#include "tests/tests.h"
#include "datasets.h"
#include "configfile.h"

namespace {
using namespace std;
using namespace arki;
using namespace arki::tests;

static const char* sample_config = R"(
[test200]
type = ondisk2
step = daily
filter = origin: GRIB1,200
index = origin, reftime
name = test200
path = test200

[test80]
type = ondisk2
step = daily
filter = origin: GRIB1,80
index = origin, reftime
name = test80
path = test80

[error]
type = error
step = daily
name = error
path = error
)";

class Tests : public TestCase
{
    using TestCase::TestCase;
    void register_tests() override;
} test("arki_dataset_datasets");

void Tests::register_tests() {

add_method("instantiate", [] {
    // In-memory dataset configuration
    ConfigFile config(sample_config, "(memory)");

    WriterPool pool(config);
    wassert(actual(pool.get("error")).istrue());
    wassert(actual(pool.get("test200")).istrue());
    wassert(actual(pool.get("test80")).istrue());
    wassert(actual(pool.get("duplicates")).isfalse());
});

}

}
