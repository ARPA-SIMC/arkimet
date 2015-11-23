/*
 * Copyright (C) 2007--2015  ARPA-SIM <urpsim@smr.arpa.emr.it>
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

#include "aggregate.h"
#include <arki/types/tests.h>
#include <arki/types.h>
#include <arki/types/origin.h>
#include <arki/types/product.h>
#include <arki/metadata.h>
#include <arki/matcher.h>

namespace tut {
using namespace std;
using namespace wibble;
using namespace wibble::tests;
using namespace arki;
using namespace arki::dataset::index;
using namespace arki::types;

struct arki_dataset_index_aggregate_shar {
	utils::sqlite::SQLiteDB db;
	arki_dataset_index_aggregate_shar()
	{
		db.open(":memory:");
		//db.open("/tmp/zaza.sqlite");
		//db.exec("DROP TABLE IF EXISTS sub_origin");
	}
};
TESTGRP(arki_dataset_index_aggregate);

template<> template<>
void to::test<1>()
{
	set<types::Code> members;
	members.insert(types::TYPE_ORIGIN);
	members.insert(types::TYPE_PRODUCT);
	members.insert(types::TYPE_LEVEL);
	Aggregate u(db, "test", members);
	u.initDB(members);
	u.initQueries();

    Metadata md;
    unique_ptr<Type> origin(Origin::createGRIB1(200, 0, 0));
    unique_ptr<Type> product(Product::createGRIB1(200, 1, 2));

    ensure_equals(u.get(md), -1);
    md.set(*origin);
    md.set(*product);

	// ID is -1 if it is not in the database
	ensure_equals(u.get(md), -1);

	int id = u.obtain(md);
	ensure_equals(id, 1);

	// Insert again, we should have the same result
	ensure_equals(u.obtain(md), 1);

	ensure_equals(u.get(md), 1);

    // Retrieve from the database
    Metadata md1;
    u.read(1, md1);
    wassert(actual_type(md1.get<types::Origin>()) == origin);
    wassert(actual_type(md1.get<types::Product>()) == product);

	Matcher m = Matcher::parse("origin:GRIB1,200; product:GRIB1,200,1");
	vector<string> constraints;
	ensure_equals(u.add_constraints(m, constraints, "t"), 2);
	ensure_equals(constraints.size(), 2u);
	ensure_equals(constraints[0], "t.origin =1");
	ensure_equals(constraints[1], "t.product =1");
}

}
