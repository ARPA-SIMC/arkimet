#include "arki/exceptions.h"
#include "arki/tests/tests.h"
#include "arki/dispatcher.h"
#include "arki/dataset.h"
#include "arki/configfile.h"
#include "arki/metadata.h"
#include "arki/metadata/collection.h"
#include "arki/matcher.h"
#include "arki/types/source/blob.h"
#include "arki/scan/grib.h"
#include "arki/scan/any.h"
#include "arki/utils/accounting.h"
#include "arki/utils/string.h"
#include "arki/utils/sys.h"
#include "arki/validator.h"
#include <sys/resource.h>
#include <time.h>

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

    Metadata md;
    scan::Grib scanner;
    RealDispatcher dispatcher(config);
    scanner.open("inbound/test.grib1");
    ensure(scanner.next(md));
    wassert(actual(dispatcher.dispatch(md)) == Dispatcher::DISP_OK);
    wassert(actual(dsname(md)) == "test200");
    ensure(scanner.next(md));
    wassert(actual(dispatcher.dispatch(md)) == Dispatcher::DISP_OK);
    wassert(actual(dsname(md)) == "test80");
    ensure(scanner.next(md));
    wassert(actual(dispatcher.dispatch(md)) == Dispatcher::DISP_ERROR);
    wassert(actual(dsname(md)) == "error");
    ensure(!scanner.next(md));

    dispatcher.flush();

    ensure_equals(plain_data_read_count.val(), 0u);
});

// Test a case where dispatch is known to fail
add_method("regression01", [] {
#ifdef HAVE_DBALLE
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

    metadata::Collection source("inbound/tempforecast.bufr");
    ensure_equals(source.size(), 1u);

    Matcher matcher = Matcher::parse("origin:BUFR,200; product:BUFR:t=temp");
    ensure(matcher(source[0]));

    RealDispatcher dispatcher(config);
    wassert(actual(dispatcher.dispatch(source[0])) == Dispatcher::DISP_OK);
    wassert(actual(source[0].sourceBlob()) == source[0].sourceBlob());
    dispatcher.flush();
#endif
});

// Test dispatch to error datasets after validation errors
add_method("validation", [] {
    ConfigFile config = setup1();
    Metadata md;
    scan::Grib scanner;
    RealDispatcher dispatcher(config);
    validators::FailAlways fail_always;
    dispatcher.add_validator(fail_always);
    scanner.open("inbound/test.grib1");
    ensure(scanner.next(md));
    wassert(actual(dispatcher.dispatch(md)) == Dispatcher::DISP_ERROR);
    wassert(actual(dsname(md)) == "error");
    ensure(scanner.next(md));
    wassert(actual(dispatcher.dispatch(md)) == Dispatcher::DISP_ERROR);
    wassert(actual(dsname(md)) == "error");
    ensure(scanner.next(md));
    wassert(actual(dispatcher.dispatch(md)) == Dispatcher::DISP_ERROR);
    wassert(actual(dsname(md)) == "error");
    ensure(!scanner.next(md));
    dispatcher.flush();
});

// Test dispatching files with no reftime, they should end up in the error dataset
add_method("missing_reftime", [] {
    ConfigFile config = setup1();
    metadata::Collection source("inbound/wrongdate.bufr");
    wassert(actual(source.size()) == 6u);

    RealDispatcher dispatcher(config);
    wassert(actual(dispatcher.dispatch(source[0])) == Dispatcher::DISP_ERROR);
    wassert(actual(dsname(source[0])) == "error");
    wassert(actual(dispatcher.dispatch(source[1])) == Dispatcher::DISP_ERROR);
    wassert(actual(dsname(source[1])) == "error");
    wassert(actual(dispatcher.dispatch(source[2])) == Dispatcher::DISP_ERROR);
    wassert(actual(dsname(source[2])) == "error");
    wassert(actual(dispatcher.dispatch(source[3])) == Dispatcher::DISP_ERROR);
    wassert(actual(dsname(source[3])) == "error");
    wassert(actual(dispatcher.dispatch(source[4])) == Dispatcher::DISP_ERROR);
    wassert(actual(dsname(source[4])) == "error");
    wassert(actual(dispatcher.dispatch(source[5])) == Dispatcher::DISP_ERROR);
    wassert(actual(dsname(source[5])) == "error");
    dispatcher.flush();
});

// Test dispatching to more datasets than the number of open files allowed
add_method("issue103", [] {
    sys::rmtree_ifexists("error");
    sys::rmtree_ifexists("vm2");
    const char* conf_text = R"(
[error]
step = daily
type = error
path = error
[vm2]
filter = area:VM2,
replace = yes
step = daily
type = iseg
format = vm2
path = vm2
smallfiles = yes
unique = reftime, area, product
index = reftime, area, product
)";

    struct rlimit limits;
    if (getrlimit(RLIMIT_NOFILE, &limits) == -1)
        throw_system_error("getrlimit failed");

    File sample("inbound/issue103.vm2", O_CREAT | O_WRONLY);
    time_t base = 1507917600;
    for (unsigned i = 0; i < limits.rlim_max + 1; ++i)
    {
        time_t step = base + i * 86400;
        struct tm t;
        if (gmtime_r(&step, &t) == nullptr)
            throw_system_error("gmtime_r failed");
        char buf[128];
        size_t bufsize = strftime(buf, 128, "%Y%m%d%H%M,1,158,23,,,\n", &t);
        if (bufsize == 0)
            throw_system_error("strftime failed");
        sample.write_all_or_throw(buf, bufsize);
    }
    sample.close();

    ConfigFile config;
    config.parse(conf_text);
    metadata::Collection source("inbound/issue103.vm2");
    wassert(actual(source.size()) == limits.rlim_max + 1);

    RealDispatcher dispatcher(config);
    for (unsigned i = 0; i < source.size(); ++i)
    {
        wassert(actual(dispatcher.dispatch(source[i])) == Dispatcher::DISP_OK);
        wassert(actual(dsname(source[i])) == "vm2");
    }
});

}

}
