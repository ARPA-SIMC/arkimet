#include "arki/types/tests.h"
#include "arki/types/origin.h"
#include "arki/metadata.h"
#include "arki/matcher.h"
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
    utils::sqlite::SQLiteDB db;
    db.open(":memory:");
    dataset::index::AttrSubIndex(db, TYPE_ORIGIN).initDB();

    Metadata md;
    unique_ptr<Type> origin(Origin::createGRIB1(200, 0, 0));

	dataset::index::AttrSubIndex attr(db, TYPE_ORIGIN);

	// ID is -1 if it is not in the database
	ensure_equals(attr.id(md), -1);

    md.set(*origin);

	// id() is read-only so it throws NotFound when the item does not exist
	try {
		attr.id(md);
		ensure(false);
	} catch (dataset::index::NotFound) {
		ensure(true);
	}

	int id = attr.insert(md);
	ensure_equals(id, 1);

	// Insert again, we should have the same result
	ensure_equals(attr.insert(md), 1);

	ensure_equals(attr.id(md), 1);

    // Retrieve from the database
    Metadata md1;
    attr.read(1, md1);
    wassert(actual_type(md1.get<types::Origin>()) == origin);

    Matcher m = Matcher::parse("origin:GRIB1,200");
    auto matcher = m.get(TYPE_ORIGIN);
    ensure((bool)matcher);
    vector<int> ids = attr.query(*matcher);
    ensure_equals(ids.size(), 1u);
    ensure_equals(ids[0], 1);
});

// Same as <1> but instantiates attr every time to always test with a cold cache
add_method("cold_cache", [] {
    utils::sqlite::SQLiteDB db;
    db.open(":memory:");
    dataset::index::AttrSubIndex(db, TYPE_ORIGIN).initDB();

    Metadata md;
    unique_ptr<Type> origin(Origin::createGRIB1(200, 0, 0));

	// ID is -1 if it is not in the database
	ensure_equals(dataset::index::AttrSubIndex(db, TYPE_ORIGIN).id(md), -1);

    md.set(*origin);

	// id() is read-only so it throws NotFound when the item does not exist
	try {
		dataset::index::AttrSubIndex(db, TYPE_ORIGIN).id(md);
		ensure(false);
	} catch (dataset::index::NotFound) {
		ensure(true);
	}

	int id = dataset::index::AttrSubIndex(db, TYPE_ORIGIN).insert(md);
	ensure_equals(id, 1);

	// Insert again, we should have the same result
	ensure_equals(dataset::index::AttrSubIndex(db, TYPE_ORIGIN).insert(md), 1);

	ensure_equals(dataset::index::AttrSubIndex(db, TYPE_ORIGIN).id(md), 1);

    // Retrieve from the database
    Metadata md1;
    dataset::index::AttrSubIndex(db, TYPE_ORIGIN).read(1, md1);
    wassert(actual_type(md1.get<types::Origin>()) == origin);

    // Query the database
    Matcher m = Matcher::parse("origin:GRIB1,200");
    auto matcher = m.get(TYPE_ORIGIN);
    ensure((bool)matcher);
    vector<int> ids = dataset::index::AttrSubIndex(db, TYPE_ORIGIN).query(*matcher);
    ensure_equals(ids.size(), 1u);
    ensure_equals(ids[0], 1);
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
	ensure_equals(ids.size(), 3u);
	ensure_equals(ids[0], -1);
	ensure_equals(ids[1], -1);
	ensure_equals(ids[2], -1);
});

}

}
