/*
 * Copyright (C) 2007,2008  ARPA-SIM <urpsim@smr.arpa.emr.it>
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
#include <arki/dataset/index/sqlite.h>

#include <sstream>
#include <fstream>
#include <iostream>

namespace tut {
using namespace std;
using namespace arki;
using namespace arki::dataset::index;

struct arki_dsindex_sqlite_shar {
	SQLiteDB db;

	arki_dsindex_sqlite_shar()
	{
		db.open(":memory:");

		// Create a simple test database
		db.exec("CREATE TABLE test (id INTEGER PRIMARY KEY, val INTEGER NOT NULL)");
	}
};
TESTGRP(arki_dsindex_sqlite);

// Test running one shot insert queries
template<> template<>
void to::test<1>()
{
	db.exec("INSERT INTO test (val) VALUES (1)");
	int id = db.lastInsertID();

	db.exec("INSERT INTO test (val) VALUES (2)");
	ensure(db.lastInsertID() > id);

	db.exec("INSERT INTO test (val) VALUES (3)");
	ensure(db.lastInsertID() > id);

	db.exec("INSERT INTO test (val) VALUES (-2)");
	ensure(db.lastInsertID() > id);
}

// Test precompile queries
template<> template<>
void to::test<2>()
{
	PrecompiledQuery select("select", db);
	select.compile("SELECT val FROM test WHERE val>? ORDER BY val");

	db.exec("INSERT INTO test (val) VALUES (1)");
	db.exec("INSERT INTO test (val) VALUES (2)");
	db.exec("INSERT INTO test (val) VALUES (3)");

	// Select all items > 1
	select.reset();
	select.bind(1, 1);
	vector<int> results;
	while (select.step())
		results.push_back(select.fetchInt(0));
	ensure_equals(results.size(), 2u);
	ensure_equals(results[0], 2);
	ensure_equals(results[1], 3);

	db.exec("INSERT INTO test (val) VALUES (4)");

	// Select all items > 2
	select.reset();
	select.bind(1, 2);
	results.clear();
	while (select.step())
		results.push_back(select.fetchInt(0));
	ensure_equals(results.size(), 2u);
	ensure_equals(results[0], 3);
	ensure_equals(results[1], 4);
}


}

// vim:set ts=4 sw=4:
