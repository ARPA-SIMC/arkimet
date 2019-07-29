#include "config.h"
#include <arki/metadata/tests.h>
#include <arki/validator.h>
#include <arki/metadata.h>
#include <arki/types/reftime.h>
#include <time.h>
#include <memory>

namespace {
using namespace std;
using namespace arki;
using namespace arki::types;
using namespace arki::tests;
using arki::core::Time;

static void dump_errors(const vector<string>& errors)
{
    unsigned count = 1;
    for (const auto& error: errors)
        fprintf(stderr, "Error %u: %s\n", count, error.c_str());
}

class Tests : public TestCase
{
    using TestCase::TestCase;
    void register_tests() override;
} test("arki_validator");

void Tests::register_tests() {

// Test FailAlways
add_method("fail_always", [] {
    Metadata md;
    arki::tests::fill(md);

    validators::FailAlways v;
    vector<string> errors;
    wassert_false(v(md, errors));
    wassert(actual(errors.size()) == 1u);
});

#define ensure_no_errors() \
    do { \
        errors.clear(); \
        bool tmp = v(md, errors); \
        dump_errors(errors); \
        wassert_true(tmp); \
        wassert(actual(errors.size()) == 0u); \
    } while(0)

#define ensure_errors(count) \
    do { \
        errors.clear(); \
        bool tmp = v(md, errors); \
        wassert_false(tmp); \
        wassert(actual(errors.size()) == count); \
    } while(0)


// Test DailyImport
add_method("daily_import", [] {
    Metadata md;
    arki::tests::fill(md);

    time_t ts_now = time(NULL);
    time_t ts = ts_now;
    struct tm t;

    validators::DailyImport v;
    vector<string> errors;
    localtime_r(&ts, &t);
    md.set(Reftime::createPosition(Time(t)));
    ensure_no_errors();

    // 12 hours into the future
    ts = ts_now + 3600 * 12;
    localtime_r(&ts, &t);
    md.set(Reftime::createPosition(Time(t)));
    ensure_no_errors();

    // 6 days into the past
    ts = ts_now - 3600 * 24 * 6;
    localtime_r(&ts, &t);
    md.set(Reftime::createPosition(Time(t)));
    ensure_no_errors();

    // 2 days into the future
    ts = ts_now + 3600 * 24 * 2;
    localtime_r(&ts, &t);
    md.set(Reftime::createPosition(Time(t)));
    ensure_errors(1u);

    // 8 days into the past
    errors.clear();
    ts = ts_now - 3600 * 24 * 8;
    localtime_r(&ts, &t);
    md.set(Reftime::createPosition(Time(t)));
    ensure_errors(1u);
});

}

}
