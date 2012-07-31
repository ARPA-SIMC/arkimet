/*
 * Copyright (C) 2012  ARPA-SIM <urpsim@smr.arpa.emr.it>
 *
 * This program is free software; you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation; either version 2 of the License, or
 * (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License along
 * with this program; if not, write to the Free Software Foundation, Inc.,
 * 51 Franklin Street, Fifth Floor, Boston, MA 02110-1301 USA.
 *
 * Author: Enrico Zini <enrico@enricozini.com>
 */

#include "config.h"

#include <arki/matcher/test-utils.h>
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
    md.set(types::reftime::Position::create(types::Time::create(t)));
    ensure_no_errors();

    // 12 hours into the future
    ts = ts_now + 3600 * 12;
    localtime_r(&ts, &t);
    md.set(types::reftime::Position::create(types::Time::create(t)));
    ensure_no_errors();

    // 6 days into the past
    ts = ts_now - 3600 * 24 * 6;
    localtime_r(&ts, &t);
    md.set(types::reftime::Position::create(types::Time::create(t)));
    ensure_no_errors();

    // 2 days into the future
    ts = ts_now + 3600 * 24 * 2;
    localtime_r(&ts, &t);
    md.set(types::reftime::Position::create(types::Time::create(t)));
    ensure_errors(1u);

    // 8 days into the past
    errors.clear();
    ts = ts_now - 3600 * 24 * 8;
    localtime_r(&ts, &t);
    md.set(types::reftime::Position::create(types::Time::create(t)));
    ensure_errors(1u);
}

}

// vim:set ts=4 sw=4:
