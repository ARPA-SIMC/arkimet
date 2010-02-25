/*
 * Copyright (C) 2007,2008,2009  ARPA-SIM <urpsim@smr.arpa.emr.it>
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

#include <arki/tests/test-utils.h>
#include <arki/dataset/index/attr.h>
#include <arki/types.h>
#include <arki/types/origin.h>
#include <arki/metadata.h>

#include <sstream>
#include <fstream>
#include <iostream>

namespace tut {
using namespace std;
using namespace arki;

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
	Item<types::Origin> origin(types::origin::GRIB1::create(200, 0, 0));

	dataset::index::AttrSubIndex attr(db, types::TYPE_ORIGIN);

	// ID is -1 if it is not in the database
	ensure_equals(attr.id(md), -1);

	md.set(origin);

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
	ensure_equals(md1.get(types::TYPE_ORIGIN).upcast<types::Origin>(), origin);

	Matcher m = Matcher::parse("origin:GRIB1,200");
	const matcher::OR* matcher = m.m_impl->get(types::TYPE_ORIGIN);
	ensure(matcher);
	vector<int> ids = attr.query(*matcher);
	ensure_equals(ids.size(), 1u);
	ensure_equals(ids[0], 1);
}

// Same as <1> but instantiates attr every time to always test with a cold cache
template<> template<>
void to::test<2>()
{
	Metadata md;
	Item<types::Origin> origin(types::origin::GRIB1::create(200, 0, 0));

	// ID is -1 if it is not in the database
	ensure_equals(dataset::index::AttrSubIndex(db, types::TYPE_ORIGIN).id(md), -1);

	md.set(origin);

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
	ensure_equals(md1.get(types::TYPE_ORIGIN).upcast<types::Origin>(), origin);

	// Query the database
	Matcher m = Matcher::parse("origin:GRIB1,200");
	const matcher::OR* matcher = m.m_impl->get(types::TYPE_ORIGIN);
	ensure(matcher);
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
