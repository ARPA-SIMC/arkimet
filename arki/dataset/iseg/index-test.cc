#include "arki/dataset/tests.h"
#include "arki/dataset/iseg.h"
#include "arki/dataset/iseg/index.h"
#include "arki/utils/sys.h"

namespace {
using namespace std;
using namespace arki;
using namespace arki::utils;
using namespace arki::tests;
using namespace arki::dataset;

class Tests : public TestCase
{
    using TestCase::TestCase;
    void register_tests() override;
} test("arki_dataset_iseg_index");

void Tests::register_tests() {

add_method("locks", [] {
    // We cannot rely on sqlite transactions to protect readers from a repack
#if 0
    std::shared_ptr<const iseg::Config> cfg = iseg::Config::create(ConfigFile(R"(
type=iseg
format=grib
step=daily
path=testds
    )"));

    std::filesystem::remove("testindex");
    if (std::filesystem::is_directory("testds"))
        sys::rmtree("testds");
    std::filesystem::create_directory("testds");

    // Create the database
    {
        iseg::WIndex idx(cfg, "testindex");
    }

    iseg::RIndex ridx(cfg, "testindex");
    iseg::WIndex widx(cfg, "testindex");

    Pending p1 = ridx.begin_transaction();
    ridx.db().exec("SELECT * from md");

    Pending p2 = widx.begin_exclusive_transaction();
    p1.commit();
    p2.commit();
#endif
});

}

}
