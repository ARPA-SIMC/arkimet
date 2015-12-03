#include "config.h"
#include <arki/metadata/tests.h>
#include <arki/validator.h>
#include <arki/metadata.h>
#include <arki/types/reftime.h>
#include <time.h>
#include <memory>
#include <sstream>
#include <iostream>

namespace std {
static inline std::ostream& operator<<(std::ostream& o, const arki::Metadata& m)
{
    m.writeYaml(o);
    return o;
}
}

namespace tut {
using namespace std;
using namespace arki;
using namespace arki::types;
using namespace arki::tests;

struct arki_validator_shar {
    arki_validator_shar()
    {
    }
};
TESTGRP(arki_validator);

// Test FailAlways
template<> template<>
void to::test<1>()
{
    Metadata md;
    arki::tests::fill(md);

    validators::FailAlways v;
    vector<string> errors;
    ensure(!v(md, errors));
    ensure_equals(errors.size(), 1u);
}

static void dump_errors(const vector<string>& errors)
{
    unsigned count = 1;
    for (vector<string>::const_iterator i = errors.begin();
            i != errors.end(); ++i, ++count)
    {
        cerr << "Error " << count << ": " << *i << endl;
    }
}

#define ensure_no_errors() \
    do { \
        errors.clear(); \
        bool tmp = v(md, errors); \
        dump_errors(errors); \
        ensure(tmp); \
        ensure_equals(errors.size(), 0u); \
    } while(0)

#define ensure_errors(count) \
    do { \
        errors.clear(); \
        bool tmp = v(md, errors); \
        ensure(!tmp); \
        ensure_equals(errors.size(), count); \
    } while(0)


// Test DailyImport
template<> template<>
void to::test<2>()
{
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
}

}

// vim:set ts=4 sw=4:
