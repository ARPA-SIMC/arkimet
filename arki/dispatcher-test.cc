#include "arki/core/tests.h"
#include "arki/dispatcher.h"
#include "arki/dataset.h"
#include "arki/configfile.h"
#include "arki/metadata.h"
#include "arki/metadata/collection.h"
#include "arki/matcher.h"
#include "arki/types/source/blob.h"
#include "arki/utils/accounting.h"
#include "arki/utils/string.h"
#include "arki/utils/sys.h"
#include "arki/validator.h"
#include <iostream>

namespace {
using namespace std;
using namespace arki;
using namespace arki::tests;
using namespace arki::utils;

inline std::string dsname(const Metadata& md)
{
    if (!md.has_source_blob()) return "(md source is not a blob source)";
    return str::basename(md.sourceBlob().basedir);
}

static const char* config1 = R"(
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

static ConfigFile setup1()
{
    sys::rmtree_ifexists("test200");
    sys::rmtree_ifexists("test80");
    sys::rmtree_ifexists("testerror");
    ConfigFile cfg;
    cfg.parse(config1);
    return cfg;
}

class Tests : public TestCase
{
    using TestCase::TestCase;
    void register_tests() override;
} test("arki_dispatcher");

void Tests::register_tests() {

// Test simple dispatching
add_method("simple", [] {
    using namespace arki::utils::acct;

    ConfigFile config = setup1();

    plain_data_read_count.reset();
    metadata::TrackedData tracked_data(metadata::DataTracker::get());

    metadata::TestCollection mdc("inbound/test.grib1", true);
    RealDispatcher dispatcher(config);
    auto batch = mdc.make_import_batch();
    wassert(dispatcher.dispatch(batch));
    wassert(actual(batch[0]->dataset_name) == "test200");
    wassert(actual(batch[0]->result) == dataset::ACQ_OK);
    wassert(actual(batch[1]->dataset_name) == "test80");
    wassert(actual(batch[1]->result) == dataset::ACQ_OK);
    wassert(actual(batch[2]->dataset_name) == "error");
    wassert(actual(batch[2]->result) == dataset::ACQ_OK);
    dispatcher.flush();

    wassert(actual(plain_data_read_count.val()) == 0u);

    // wassert(actual(tracked_data.count_used()) == 0u);
});

// Test a case where dispatch is known to fail
add_method("regression01", [] {
    skip_unless_bufr();
    // In-memory dataset configuration
    sys::rmtree_ifexists("lami_temp");
    sys::rmtree_ifexists("error");
    string conf =
        "[lami_temp]\n"
        "filter = origin:BUFR,200; product:BUFR:t=temp\n"
        "name = lami_temp\n"
        "type = discard\n"
        "\n"
        "[error]\n"
        "type = discard\n"
        "name = error\n";
    ConfigFile config;
    config.parse(conf);

    metadata::TestCollection source("inbound/tempforecast.bufr", true);
    ensure_equals(source.size(), 1u);

    Matcher matcher = Matcher::parse("origin:BUFR,200; product:BUFR:t=temp");
    ensure(matcher(source[0]));

    RealDispatcher dispatcher(config);
    auto batch = source.make_import_batch();
    wassert(dispatcher.dispatch(batch));
    wassert(actual(batch[0]->dataset_name) == "lami_temp");
    wassert(actual(batch[0]->result) == dataset::ACQ_OK);
    dispatcher.flush();
});

// Test dispatch to error datasets after validation errors
add_method("validation", [] {
    ConfigFile config = setup1();
    RealDispatcher dispatcher(config);
    validators::FailAlways fail_always;
    dispatcher.add_validator(fail_always);
    metadata::TestCollection mdc("inbound/test.grib1", true);
    auto batch = mdc.make_import_batch();
    wassert(dispatcher.dispatch(batch));
    wassert(actual(batch[0]->dataset_name) == "error");
    wassert(actual(batch[0]->result) == dataset::ACQ_OK);
    wassert(actual(batch[1]->dataset_name) == "error");
    wassert(actual(batch[1]->result) == dataset::ACQ_OK);
    wassert(actual(batch[2]->dataset_name) == "error");
    wassert(actual(batch[2]->result) == dataset::ACQ_OK);
    dispatcher.flush();
});

// Test dispatching files with no reftime, they should end up in the error dataset
add_method("missing_reftime", [] {
    ConfigFile config = setup1();
    metadata::TestCollection source("inbound/wrongdate.bufr", true);
    wassert(actual(source.size()) == 6u);

    RealDispatcher dispatcher(config);
    auto batch = source.make_import_batch();
    wassert(dispatcher.dispatch(batch));
    wassert(actual(batch[0]->dataset_name) == "error");
    wassert(actual(batch[0]->result) == dataset::ACQ_OK);
    wassert(actual(batch[1]->dataset_name) == "error");
    wassert(actual(batch[1]->result) == dataset::ACQ_OK);
    wassert(actual(batch[2]->dataset_name) == "error");
    wassert(actual(batch[2]->result) == dataset::ACQ_OK);
    wassert(actual(batch[3]->dataset_name) == "error");
    wassert(actual(batch[3]->result) == dataset::ACQ_OK);
    wassert(actual(batch[4]->dataset_name) == "error");
    wassert(actual(batch[4]->result) == dataset::ACQ_OK);
    wassert(actual(batch[5]->dataset_name) == "error");
    wassert(actual(batch[5]->result) == dataset::ACQ_OK);
    dispatcher.flush();
});

}

}
