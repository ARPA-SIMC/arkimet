#include "arki/core/tests.h"
#include "arki/dataset.h"
#include "arki/dataset/session.h"
#include "arki/dispatcher.h"
#include "arki/matcher.h"
#include "arki/matcher/parser.h"
#include "arki/metadata.h"
#include "arki/metadata/collection.h"
#include "arki/metadata/data.h"
#include "arki/metadata/validator.h"
#include "arki/types/source/blob.h"
#include "arki/utils/accounting.h"
#include "arki/utils/string.h"
#include "arki/utils/sys.h"

namespace {
using namespace std;
using namespace arki;
using namespace arki::tests;
using namespace arki::utils;

static std::shared_ptr<core::cfg::Sections> setup1(const std::string& format)
{
    static const char* config1 = R"(
[test200]
type = iseg
format = grib
step = daily
filter = origin: GRIB1,200
index = origin, reftime
name = test200
path = test200

[test80]
type = iseg
format = grib
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

    sys::rmtree_ifexists("test200");
    sys::rmtree_ifexists("test80");
    sys::rmtree_ifexists("testerror");
    auto res = core::cfg::Sections::parse(config1);

    auto test200 = res->section("test200");
    test200->set("format", format);
    auto test80 = res->section("test80");
    test200->set("format", format);

    return res;
}

class Tests : public TestCase
{
    using TestCase::TestCase;
    void register_tests() override;
} test("arki_dispatcher");

void Tests::register_tests()
{

    // Test simple dispatching
    add_method("simple", [] {
        using namespace arki::utils::acct;

        auto session = std::make_shared<dataset::Session>();
        auto pool    = std::make_shared<dataset::Pool>(session);
        auto cfg     = setup1("grib");
        for (const auto& i : *cfg)
            pool->add_dataset(*i.second);

        metadata::TrackedData tracked_data(metadata::DataManager::get());
        metadata::TestCollection mdc("inbound/test.grib1", true);
        plain_data_read_count.reset();

        RealDispatcher dispatcher(pool);
        auto batch = mdc.make_batch();
        wassert(dispatcher.dispatch(batch, false));
        wassert(actual(batch[0]->destination) == "test200");
        wassert(actual(batch[0]->result) == metadata::Inbound::Result::OK);
        wassert(actual(batch[1]->destination) == "test80");
        wassert(actual(batch[1]->result) == metadata::Inbound::Result::OK);
        wassert(actual(batch[2]->destination) == "error");
        wassert(actual(batch[2]->result) == metadata::Inbound::Result::OK);
        dispatcher.flush();

        wassert(actual(plain_data_read_count.val()) == 0u);

        wassert(actual(tracked_data.count_used()) == 3u);
    });

    add_method("drop_cached_data", [] {
        using namespace arki::utils::acct;

        auto session = std::make_shared<dataset::Session>();
        auto pool    = std::make_shared<dataset::Pool>(session);
        auto cfg     = setup1("grib");
        for (const auto& i : *cfg)
            pool->add_dataset(*i.second);

        metadata::TrackedData tracked_data(metadata::DataManager::get());
        metadata::TestCollection mdc("inbound/test.grib1", true);
        plain_data_read_count.reset();

        RealDispatcher dispatcher(pool);
        auto batch = mdc.make_batch();
        wassert(dispatcher.dispatch(batch, true));
        wassert(actual(batch[0]->destination) == "test200");
        wassert(actual(batch[0]->result) == metadata::Inbound::Result::OK);
        wassert(actual(batch[1]->destination) == "test80");
        wassert(actual(batch[1]->result) == metadata::Inbound::Result::OK);
        wassert(actual(batch[2]->destination) == "error");
        wassert(actual(batch[2]->result) == metadata::Inbound::Result::OK);
        dispatcher.flush();

        wassert(actual(plain_data_read_count.val()) == 0u);

        wassert(actual(tracked_data.count_used()) == 0u);
    });

    // Test a case where dispatch is known to fail
    add_method("regression01", [] {
        skip_unless_bufr();
        matcher::Parser parser;
        // In-memory dataset configuration
        sys::rmtree_ifexists("lami_temp");
        sys::rmtree_ifexists("error");
        string conf = "[lami_temp]\n"
                      "filter = origin:BUFR,200; product:BUFR:t=temp\n"
                      "name = lami_temp\n"
                      "type = discard\n"
                      "\n"
                      "[error]\n"
                      "type = discard\n"
                      "name = error\n";
        auto config = core::cfg::Sections::parse(conf);

        auto session = std::make_shared<dataset::Session>();
        auto pool    = std::make_shared<dataset::Pool>(session);
        for (const auto& i : *config)
            pool->add_dataset(*i.second);

        metadata::TestCollection source("inbound/tempforecast.bufr", true);
        wassert(actual(source.size()) == 1u);

        Matcher matcher = parser.parse("origin:BUFR,200; product:BUFR:t=temp");
        wassert_true(matcher(source[0]));

        RealDispatcher dispatcher(pool);
        auto batch = source.make_batch();
        wassert(dispatcher.dispatch(batch, false));
        wassert(actual(batch[0]->destination) == "lami_temp");
        wassert(actual(batch[0]->result) == metadata::Inbound::Result::OK);
        dispatcher.flush();
    });

    // Test dispatch to error datasets after validation errors
    add_method("validation", [] {
        auto session = std::make_shared<dataset::Session>();
        auto pool    = std::make_shared<dataset::Pool>(session);
        auto cfg     = setup1("grib");
        for (const auto& i : *cfg)
            pool->add_dataset(*i.second);
        RealDispatcher dispatcher(pool);
        metadata::validators::FailAlways fail_always;
        dispatcher.add_validator(fail_always);
        metadata::TestCollection mdc("inbound/test.grib1", true);
        auto batch = mdc.make_batch();
        wassert(dispatcher.dispatch(batch, false));
        wassert(actual(batch[0]->destination) == "error");
        wassert(actual(batch[0]->result) == metadata::Inbound::Result::OK);
        wassert(actual(batch[1]->destination) == "error");
        wassert(actual(batch[1]->result) == metadata::Inbound::Result::OK);
        wassert(actual(batch[2]->destination) == "error");
        wassert(actual(batch[2]->result) == metadata::Inbound::Result::OK);
        dispatcher.flush();
    });

    // Test dispatching files with no reftime, they should end up in the error
    // dataset
    add_method("missing_reftime", [] {
        auto session = std::make_shared<dataset::Session>();
        auto pool    = std::make_shared<dataset::Pool>(session);
        auto cfg     = setup1("bufr");
        for (const auto& i : *cfg)
            pool->add_dataset(*i.second);
        metadata::TestCollection source("inbound/wrongdate.bufr", true);
        wassert(actual(source.size()) == 6u);

        RealDispatcher dispatcher(pool);
        auto batch = source.make_batch();
        wassert(dispatcher.dispatch(batch, false));
        wassert(actual(batch[0]->destination) == "error");
        wassert(actual(batch[0]->result) == metadata::Inbound::Result::OK);
        wassert(actual(batch[1]->destination) == "error");
        wassert(actual(batch[1]->result) == metadata::Inbound::Result::OK);
        wassert(actual(batch[2]->destination) == "error");
        wassert(actual(batch[2]->result) == metadata::Inbound::Result::OK);
        wassert(actual(batch[3]->destination) == "error");
        wassert(actual(batch[3]->result) == metadata::Inbound::Result::OK);
        wassert(actual(batch[4]->destination) == "error");
        wassert(actual(batch[4]->result) == metadata::Inbound::Result::OK);
        wassert(actual(batch[5]->destination) == "error");
        wassert(actual(batch[5]->result) == metadata::Inbound::Result::OK);
        dispatcher.flush();
    });
}

} // namespace
