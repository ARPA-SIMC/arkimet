#include "arki/types/tests.h"
#include "arki/metadata.h"
#include "arki/metadata/data.h"
#include "stats.h"

namespace {
using namespace std;
using namespace arki;
using namespace arki::tests;
using namespace arki::core;
using namespace arki::types;
using namespace arki::summary;

class Tests : public TestCase
{
    using TestCase::TestCase;
    void register_tests() override;
} test("arki_summary_stats");

void Tests::register_tests() {

// Basic stats tests
add_method("basic", [] {
    Metadata md;
    md.set(Reftime::createPosition(Time(2009, 8, 7, 6, 5, 4)));
    md.set_source_inline("grib", metadata::DataManager::get().to_data("grib", vector<uint8_t>()));

    unique_ptr<Stats> st(new Stats);
    wassert(actual(st->count) == 0u);
    wassert(actual(st->size) == 0u);
    //wassert(actual(*st) == Stats());

    st->merge(md);
    wassert(actual(st->count) == 1u);
    wassert(actual(st->size) == 0u);

    unique_ptr<Stats> st1(new Stats);
    st1->merge(md);
    st1->merge(md);

    //wassert(actual(st).compares(*st1));
});

}

}
