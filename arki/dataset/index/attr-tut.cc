#include "arki/types/tests.h"
#include "arki/dataset/index/attr.h"
#include "arki/types.h"
#include "arki/types/origin.h"
#include "arki/metadata.h"
#include "arki/matcher.h"
#include <sstream>
#include <fstream>
#include <iostream>

namespace tut {
using namespace std;
using namespace wibble::tests;
using namespace arki;
using namespace arki::types;

struct arki_dataset_index_attr_shar {
	utils::sqlite::SQLiteDB db;

	arki_dataset_index_attr_shar()
	{
		db.open(":memory:");
		//db.open("/tmp/zaza.sqlite");
		//db.exec("DROP TABLE IF EXISTS sub_origin");
		dataset::index::AttrSubIndex(db, types::TYPE_ORIGIN).initDB();
	}
};
TESTGRP(arki_dataset_index_attr);

template<> template<>
void to::test<1>()
{
    Metadata md;
    unique_ptr<Type> origin(Origin::createGRIB1(200, 0, 0));

	dataset::index::AttrSubIndex attr(db, types::TYPE_ORIGIN);

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
    auto matcher = m.get(types::TYPE_ORIGIN);
    ensure((bool)matcher);
    vector<int> ids = attr.query(*matcher);
    ensure_equals(ids.size(), 1u);
    ensure_equals(ids[0], 1);
}

// Same as <1> but instantiates attr every time to always test with a cold cache
template<> template<>
void to::test<2>()
{
    Metadata md;
    unique_ptr<Type> origin(Origin::createGRIB1(200, 0, 0));

	// ID is -1 if it is not in the database
	ensure_equals(dataset::index::AttrSubIndex(db, types::TYPE_ORIGIN).id(md), -1);

    md.set(*origin);

	// id() is read-only so it throws NotFound when the item does not exist
	try {
		dataset::index::AttrSubIndex(db, types::TYPE_ORIGIN).id(md);
		ensure(false);
	} catch (dataset::index::NotFound) {
		ensure(true);
	}

	int id = dataset::index::AttrSubIndex(db, types::TYPE_ORIGIN).insert(md);
	ensure_equals(id, 1);

	// Insert again, we should have the same result
	ensure_equals(dataset::index::AttrSubIndex(db, types::TYPE_ORIGIN).insert(md), 1);

	ensure_equals(dataset::index::AttrSubIndex(db, types::TYPE_ORIGIN).id(md), 1);

    // Retrieve from the database
    Metadata md1;
    dataset::index::AttrSubIndex(db, types::TYPE_ORIGIN).read(1, md1);
    wassert(actual_type(md1.get<types::Origin>()) == origin);

    // Query the database
    Matcher m = Matcher::parse("origin:GRIB1,200");
    auto matcher = m.get(types::TYPE_ORIGIN);
    ensure((bool)matcher);
    vector<int> ids = dataset::index::AttrSubIndex(db, types::TYPE_ORIGIN).query(*matcher);
    ensure_equals(ids.size(), 1u);
    ensure_equals(ids[0], 1);
}

template<> template<>
void to::test<3>()
{
	set<types::Code> members;
	members.insert(types::TYPE_ORIGIN);
	members.insert(types::TYPE_PRODUCT);
	members.insert(types::TYPE_LEVEL);
	dataset::index::Attrs attrs(db, members);

	Metadata md;
	vector<int> ids = attrs.obtainIDs(md);
	ensure_equals(ids.size(), 3u);
	ensure_equals(ids[0], -1);
	ensure_equals(ids[1], -1);
	ensure_equals(ids[2], -1);
}

}

// vim:set ts=4 sw=4:
