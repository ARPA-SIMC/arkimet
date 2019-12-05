#include "arki/dataset/tests.h"
#include "arki/dataset.h"
#include "arki/utils/sys.h"

using namespace std;
using namespace arki;
using namespace arki::utils;
using namespace arki::tests;

namespace {

class Tests : public TestCase
{
    using TestCase::TestCase;

    void register_tests() override;
} test("arki_dataset");

void write_test_config()
{
    sys::rmtree_ifexists("testds");
    sys::makedirs("testds");
    sys::write_file("testds/config", R"(
type = iseg
step = daily
filter = origin: GRIB1,200
index = origin, reftime
)");
}

void Tests::register_tests() {

add_method("read_config", [] {
    auto cfg = dataset::Reader::read_config("inbound/test.grib1");
    wassert(actual(cfg.value("name")) == "inbound/test.grib1");

    cfg = dataset::Reader::read_config("grib:inbound/test.grib1");
    wassert(actual(cfg.value("name")) == "inbound/test.grib1");

    write_test_config();
    cfg = dataset::Reader::read_config("testds");
    wassert(actual(cfg.value("name")) == "testds");
});

add_method("read_configs", [] {
    auto cfg = dataset::Reader::read_configs("inbound/test.grib1");
    core::cfg::Section* sec = cfg.section("inbound/test.grib1");
    wassert_true(sec);
    wassert(actual(sec->value("name")) == "inbound/test.grib1");

    cfg = dataset::Reader::read_configs("grib:inbound/test.grib1");
    sec = cfg.section("inbound/test.grib1");
    wassert_true(sec);
    wassert(actual(sec->value("name")) == "inbound/test.grib1");

    write_test_config();
    cfg = dataset::Reader::read_configs("testds");
    sec = cfg.section("testds");
    wassert_true(sec);
    wassert(actual(sec->value("name")) == "testds");
});

}


struct Fixture : public DatasetTest
{
    using DatasetTest::DatasetTest;

    void test_setup()
    {
        DatasetTest::test_setup(R"(
            step = daily
        )");
    }
};

class InstantiateTests : public FixtureTestCase<Fixture>
{
    using FixtureTestCase::FixtureTestCase;

    void register_tests() override;
};

InstantiateTests test_simple("arki_dataset_instantiate_simple", "type=simple\n_reader=simple\n_writer=simple\n_checker=simple\n");
InstantiateTests test_ondisk2("arki_dataset_instantiate_ondisk2", "type=ondisk2\n_reader=ondisk2\n_writer=ondisk2\n_checker=ondisk2\n");
InstantiateTests test_iseg("arki_dataset_instantiate_iseg", "type=iseg\nformat=grib\n_reader=iseg\n_writer=iseg\n_checker=iseg\n");
InstantiateTests test_error("arki_dataset_instantiate_error", "type=error\n_reader=simple\n_writer=simple\n_checker=simple\n");
InstantiateTests test_duplicates("arki_dataset_instantiate_duplicates", "type=duplicates\n_reader=simple\n_writer=simple\n_checker=simple\n");
InstantiateTests test_outbound("arki_dataset_instantiate_outbound", "type=outbound\n_reader=empty\n_writer=outbound\n");
InstantiateTests test_discard("arki_dataset_instantiate_discard", "type=discard\n_reader=empty\n_writer=discard\n_checker=empty\n");

void InstantiateTests::register_tests() {

add_method("instantiate", [](Fixture& f) {
    std::string type;

    type = f.cfg.value("_reader");
    if (!type.empty())
    {
        auto reader = f.config().create_reader();
        wassert(actual(reader->type()) == type);
    }

    type = f.cfg.value("_writer");
    if (!type.empty())
    {
        auto writer = f.config().create_writer();
        wassert(actual(writer->type()) == type);
    }

    type = f.cfg.value("_checker");
    if (!type.empty())
    {
        auto checker = f.config().create_checker();
        wassert(actual(checker->type()) == type);
    }
});

}

}
