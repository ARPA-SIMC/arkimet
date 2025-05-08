#include "arki/dataset/tests.h"
#include "arki/dataset.h"
#include "arki/dataset/session.h"
#include "arki/utils/sys.h"

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

class InstantiateTests : public FixtureTestCase<Fixture>
{
    using FixtureTestCase::FixtureTestCase;

    void register_tests() override;
};

InstantiateTests test_simple("arki_dataset_instantiate_simple", "type=simple\n_reader=simple\n_writer=simple\n_checker=simple\n");
InstantiateTests test_iseg("arki_dataset_instantiate_iseg", "type=iseg\nformat=grib\n_reader=iseg\n_writer=iseg\n_checker=iseg\n");
InstantiateTests test_error("arki_dataset_instantiate_error", "type=error\n_reader=simple\n_writer=simple\n_checker=simple\n");
InstantiateTests test_duplicates("arki_dataset_instantiate_duplicates", "type=duplicates\n_reader=simple\n_writer=simple\n_checker=simple\n");
InstantiateTests test_outbound("arki_dataset_instantiate_outbound", "type=outbound\n_reader=empty\n_writer=outbound\n");
InstantiateTests test_discard("arki_dataset_instantiate_discard", "type=discard\n_reader=empty\n_writer=discard\n_checker=empty\n");

void InstantiateTests::register_tests() {

add_method("instantiate", [](Fixture& f) {
    std::string type;

    type = f.cfg->value("_reader");
    if (!type.empty())
    {
        auto reader = f.config().create_reader();
        wassert(actual(reader->type()) == type);
    }

    type = f.cfg->value("_writer");
    if (!type.empty())
    {
        auto writer = f.config().create_writer();
        wassert(actual(writer->type()) == type);
    }

    type = f.cfg->value("_checker");
    if (!type.empty())
    {
        auto checker = f.config().create_checker();
        wassert(actual(checker->type()) == type);
    }
});

}

class Tests : public TestCase
{
    using TestCase::TestCase;

    void register_tests() override;
} test("arki_dataset");

std::shared_ptr<dataset::Dataset> ds(const std::string& config)
{
    auto session = std::make_shared<dataset::Session>(false);
    auto cfg = core::cfg::Section::parse(config);
    cfg->set("step", "daily");
    cfg->set("format", "grib");
    return session->dataset(*cfg);
}

DatasetUse use(const std::string& config)
{
    return ds(config)->use();
}

void throws(const std::string& config, const std::string& msg)
{
    auto e = wassert_throws(std::runtime_error, ds(config));
    wassert(actual(e.what()) == msg);
}

void Tests::register_tests() {

add_method("use", [] {
    wassert(actual(use("name=test\ntype=simple\n")) == DatasetUse::DEFAULT);

    wassert(actual(use("name=error\ntype=simple\n")) == DatasetUse::ERRORS);
    wassert(actual(use("name=errors\ntype=simple\n")) == DatasetUse::ERRORS);
    wassert(actual(use("name=test\ntype=error\n")) == DatasetUse::ERRORS);
    wassert(actual(use("name=test\ntype=simple\nuse=error\n")) == DatasetUse::ERRORS);
    wassert(actual(use("name=test\ntype=simple\nuse=errors\n")) == DatasetUse::ERRORS);
    wassert(actual(use("name=test\ntype=error\nuse=error\n")) == DatasetUse::ERRORS);
    wassert(actual(use("name=test\ntype=error\nuse=errors\n")) == DatasetUse::ERRORS);
    wassert(actual(use("name=error\ntype=simple\nuse=error\n")) == DatasetUse::ERRORS);
    wassert(actual(use("name=errors\ntype=simple\nuse=errors\n")) == DatasetUse::ERRORS);
    wassert(actual(use("name=error\ntype=error\nuse=error\n")) == DatasetUse::ERRORS);
    wassert(actual(use("name=errors\ntype=error\nuse=errors\n")) == DatasetUse::ERRORS);

    wassert(actual(use("name=duplicates\ntype=simple\n")) == DatasetUse::DUPLICATES);
    wassert(actual(use("name=test\ntype=duplicates\n")) == DatasetUse::DUPLICATES);
    wassert(actual(use("name=test\ntype=simple\nuse=duplicates\n")) == DatasetUse::DUPLICATES);
    wassert(actual(use("name=test\ntype=duplicates\nuse=duplicates\n")) == DatasetUse::DUPLICATES);
    wassert(actual(use("name=duplicates\ntype=simple\nuse=duplicates\n")) == DatasetUse::DUPLICATES);
    wassert(actual(use("name=duplicates\ntype=duplicates\nuse=duplicates\n")) == DatasetUse::DUPLICATES);

    wassert(throws("name=duplicates\ntype=error\n", "dataset with type=error cannot be called duplicates"));
    wassert(throws("name=duplicates\ntype=simple\nuse=errors\n", "dataset with use=errors cannot be called duplicates"));
    wassert(throws("name=duplicates\ntype=error\nuse=errors\n", "dataset with use=errors cannot be called duplicates"));
    wassert(throws("name=test\ntype=duplicates\nuse=errors\n", "dataset with use=errors cannot have type=duplicates"));
    wassert(throws("name=test\ntype=error\nuse=duplicates\n", "dataset with use=duplicates cannot have type=error"));
    wassert(throws("name=errors\ntype=simple\nuse=duplicates\n", "dataset with use=duplicates cannot be called errors"));
    wassert(throws("name=errors\ntype=duplicates\n", "dataset with type=duplicates cannot be called errors"));
    wassert(throws("name=errors\ntype=duplicates\nuse=duplicates\n", "dataset with use=duplicates cannot be called errors"));
    wassert(throws("name=errors\ntype=duplicates\nuse=errors\n", "dataset with use=errors cannot have type=duplicates"));
});

}

}
