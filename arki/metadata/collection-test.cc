#include "arki/metadata/collection.h"
#include "arki/metadata/tests.h"
#include "arki/types/reftime.h"

namespace {
using namespace std;
using namespace arki;
using namespace arki::utils;
using namespace arki::tests;
using arki::metadata::Collection;

class Tests : public TestCase
{
    using TestCase::TestCase;
    void register_tests() override;
} test("arki_metadata_collection");

std::shared_ptr<Metadata> dupmd(std::shared_ptr<Metadata> md, int marker)
{
    auto res = md->clone();
    res->set(types::Reftime::createPosition(core::Time(2025, 1, marker)));
    return res;
}

void Tests::register_tests()
{

    add_method("without_duplicates_empty_collection", []() {
        Collection mds;
        Collection mds1 =
            mds.without_duplicates(types::parse_code_names("product, level"));
        wassert(actual(mds1.size()) == 0u);
    });

    add_method("without_duplicates_one_item", []() {
        GRIBData td;
        Collection mds;
        mds.acquire(td.mds.get(0));

        Collection mds1 =
            mds.without_duplicates(types::parse_code_names("product, level"));
        wassert(actual(mds1.size()) == 1u);
        wassert(actual(mds1[0]) == mds[0]);
    });

    add_method("without_duplicates_empty_unique", []() {
        GRIBData td;
        Collection mds = td.mds.without_duplicates(std::set<types::Code>());
        wassert_true(mds == td.mds);
    });

    add_method("without_duplicates_no_unique_metadata_items", []() {
        GRIBData td;
        Collection mds = td.mds.without_duplicates(
            types::parse_code_names("task, quantity"));
        wassert_true(mds.empty());
    });

    add_method("without_duplicates_no_duplicates", []() {
        GRIBData td;
        Collection mds = td.mds.without_duplicates(
            types::parse_code_names("product, level"));
        wassert_true(mds == td.mds);
    });

    add_method("without_duplicates_all_duplicates", []() {
        GRIBData td;
        Collection mds;
        mds.acquire(dupmd(td.mds.get(0), 1));
        mds.acquire(dupmd(td.mds.get(0), 2));
        mds.acquire(dupmd(td.mds.get(0), 3));
        auto mds1 =
            mds.without_duplicates(types::parse_code_names("product, level"));
        wassert(actual(mds1.size()) == 1u);
        wassert(actual_type(mds1[0].get(TYPE_REFTIME))
                    .is_reftime_position(core::Time(2025, 1, 3)));
    });

    add_method("without_duplicates", []() {
        GRIBData td;
        Collection mds;
        mds.acquire(dupmd(td.mds.get(0), 1));
        mds.acquire(dupmd(td.mds.get(1), 2));
        mds.acquire(dupmd(td.mds.get(2), 3));
        mds.acquire(dupmd(td.mds.get(1), 4));
        mds.acquire(dupmd(td.mds.get(2), 5));
        mds.acquire(dupmd(td.mds.get(0), 6));
        mds.acquire(dupmd(td.mds.get(0), 7));

        auto mds1 =
            mds.without_duplicates(types::parse_code_names("product, level"));

        wassert(actual(mds1.size()) == 3u);
        wassert(actual_type(mds1[0].get(TYPE_REFTIME))
                    .is_reftime_position(core::Time(2025, 1, 4)));
        wassert(actual_type(mds1[1].get(TYPE_REFTIME))
                    .is_reftime_position(core::Time(2025, 1, 5)));
        wassert(actual_type(mds1[2].get(TYPE_REFTIME))
                    .is_reftime_position(core::Time(2025, 1, 7)));
    });
}

} // namespace
