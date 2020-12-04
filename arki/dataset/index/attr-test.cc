#include "arki/types/tests.h"
#include "arki/types/origin.h"
#include "arki/metadata.h"
#include "arki/matcher.h"
#include "arki/matcher/parser.h"
#include "arki/types.h"
#include "attr.h"

namespace {
using namespace std;
using namespace arki;
using namespace arki::tests;
using namespace arki::types;

class Tests : public TestCase
{
    using TestCase::TestCase;
    void register_tests() override;
} test("arki_dataset_index_attr");

void Tests::register_tests() {

add_method("basic", [] {
    matcher::Parser parser;
    utils::sqlite::SQLiteDB db;
    db.open(":memory:");
    dataset::index::AttrSubIndex(db, TYPE_ORIGIN).initDB();

    Metadata md;
    unique_ptr<Type> origin(Origin::createGRIB1(200, 0, 0));

    dataset::index::AttrSubIndex attr(db, TYPE_ORIGIN);

    // ID is -1 if it is not in the database
    wassert(actual(attr.id(md)) == -1);

    md.test_set(*origin);

    // id() is read-only so it throws NotFound when the item does not exist
    wassert_throws(dataset::index::NotFound, attr.id(md));

    int id = attr.insert(md);
    wassert(actual(id) == 1);

    // Insert again, we should have the same result
    wassert(actual(attr.insert(md)) == 1);

    wassert(actual(attr.id(md)) == 1);

    // Retrieve from the database
    Metadata md1;
    attr.read(1, md1);
    wassert(actual_type(md1.get<types::Origin>()) == origin);

    Matcher m = parser.parse("origin:GRIB1,200");
    auto matcher = m.get(TYPE_ORIGIN);
    wassert_true((bool)matcher);
    vector<int> ids = attr.query(*matcher);
    wassert(actual(ids.size()) == 1u);
    wassert(actual(ids[0]) == 1);
});

// Same as "basic" but instantiates attr every time to always test with a cold cache
add_method("cold_cache", [] {
    matcher::Parser parser;
    utils::sqlite::SQLiteDB db;
    db.open(":memory:");
    dataset::index::AttrSubIndex(db, TYPE_ORIGIN).initDB();

    Metadata md;
    unique_ptr<Type> origin(Origin::createGRIB1(200, 0, 0));

    // ID is -1 if it is not in the database
    wassert(actual(dataset::index::AttrSubIndex(db, TYPE_ORIGIN).id(md)) == -1);

    md.test_set(*origin);

    // id() is read-only so it throws NotFound when the item does not exist
    wassert_throws(dataset::index::NotFound, dataset::index::AttrSubIndex(db, TYPE_ORIGIN).id(md));

    int id = dataset::index::AttrSubIndex(db, TYPE_ORIGIN).insert(md);
    wassert(actual(id) == 1);

    // Insert again, we should have the same result
    wassert(actual(dataset::index::AttrSubIndex(db, TYPE_ORIGIN).insert(md)) == 1);

    wassert(actual(dataset::index::AttrSubIndex(db, TYPE_ORIGIN).id(md)) == 1);

    // Retrieve from the database
    Metadata md1;
    dataset::index::AttrSubIndex(db, TYPE_ORIGIN).read(1, md1);
    wassert(actual_type(md1.get<types::Origin>()) == origin);

    // Query the database
    Matcher m = parser.parse("origin:GRIB1,200");
    auto matcher = m.get(TYPE_ORIGIN);
    wassert_true((bool)matcher);
    vector<int> ids = dataset::index::AttrSubIndex(db, TYPE_ORIGIN).query(*matcher);
    wassert(actual(ids.size()) == 1u);
    wassert(actual(ids[0]) == 1);
});

add_method("obtainids", [] {
    utils::sqlite::SQLiteDB db;
    db.open(":memory:");
    dataset::index::AttrSubIndex(db, TYPE_ORIGIN).initDB();

	set<types::Code> members;
	members.insert(TYPE_ORIGIN);
	members.insert(TYPE_PRODUCT);
	members.insert(TYPE_LEVEL);
	dataset::index::Attrs attrs(db, members);

    Metadata md;
    vector<int> ids = attrs.obtainIDs(md);
    wassert(actual(ids.size()) == 3u);
    wassert(actual(ids[0]) == -1);
    wassert(actual(ids[1]) == -1);
    wassert(actual(ids[2]) == -1);
});

}

}
