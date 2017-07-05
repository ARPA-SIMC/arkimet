#include "arki/dataset/tests.h"
#include "arki/dataset.h"

using namespace std;
using namespace arki;
using namespace arki::utils;
using namespace arki::tests;

namespace {

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

class Tests : public FixtureTestCase<Fixture>
{
    using FixtureTestCase::FixtureTestCase;

    void register_tests() override;
};

Tests test_simple("arki_dataset_instantiate_simple", "type=simple\n_reader=simple\n_writer=simple\n_checker=simple\n");
Tests test_ondisk2("arki_dataset_instantiate_ondisk2", "type=ondisk2\n_reader=ondisk2\n_writer=ondisk2\n_checker=ondisk2\n");
Tests test_iseg("arki_dataset_instantiate_iseg", "type=iseg\nformat=grib\n_reader=iseg\n_writer=iseg\n_checker=iseg\n");
Tests test_error("arki_dataset_instantiate_error", "type=error\n_reader=simple\n_writer=simple\n_checker=simple\n");
Tests test_duplicates("arki_dataset_instantiate_duplicates", "type=duplicates\n_reader=simple\n_writer=simple\n_checker=simple\n");
Tests test_outbound("arki_dataset_instantiate_outbound", "type=outbound\n_reader=empty\n_writer=outbound\n");
Tests test_discard("arki_dataset_instantiate_discard", "type=discard\n_reader=empty\n_writer=discard\n_checker=empty\n");

void Tests::register_tests() {

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
