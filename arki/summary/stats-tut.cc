#include <arki/types/tests.h>
#include "stats.h"
#include <arki/metadata.h>

namespace tut {
using namespace std;
using namespace arki;
using namespace arki::types;
using namespace wibble::tests;

struct arki_summary_stats_shar {
    Metadata md;

    arki_summary_stats_shar()
    {
        md.set(Reftime::createPosition(Time(2009, 8, 7, 6, 5, 4)));
        md.set_source_inline("grib1", vector<uint8_t>());
    }
};
TESTGRP(arki_summary_stats);

// Basic stats tests
template<> template<>
void to::test<1>()
{
    using namespace arki::summary;

    unique_ptr<Stats> st(new Stats);
    wassert(actual(st->count) == 0);
    wassert(actual(st->size) == 0);
    wassert(actual(*st) == Stats());

    st->merge(md);
    wassert(actual(st->count) == 1);
    wassert(actual(st->size) == 0);

    unique_ptr<Stats> st1(new Stats);
    st1->merge(md);
    st1->merge(md);

    wassert(actual(st).compares(*st1));
}

// Basic encode/decode tests
template<> template<>
void to::test<2>()
{
    using namespace arki::summary;

    unique_ptr<Stats> st(new Stats);
    st->merge(md);
    wassert(actual(st).serializes());
}

// Basic encode/decode tests with large numbers
template<> template<>
void to::test<3>()
{
    using namespace arki::summary;

    unique_ptr<Stats> st(new Stats);
    st->merge(md);
    st->count = 0x7FFFffffUL;
    st->size = 0x7FFFffffFFFFffffUL;
    wassert(actual(st).serializes());
}

}
