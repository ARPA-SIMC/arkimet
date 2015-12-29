#include "config.h"
#include <arki/tests/tests.h>
#include <arki/utils/sqlite.h>
#include <sstream>

namespace tut {
using namespace std;
using namespace arki;
using namespace arki::tests;
using namespace arki::utils::sqlite;

struct arki_utils_sqlite_shar {
	SQLiteDB db;

	arki_utils_sqlite_shar()
	{
		db.open(":memory:");

		// Create a simple test database
		db.exec("CREATE TABLE test (id INTEGER PRIMARY KEY, val INTEGER NOT NULL)");
	}
};
TESTGRP(arki_utils_sqlite);

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
		results.push_back(select.fetch<int>(0));
	ensure_equals(results.size(), 2u);
	ensure_equals(results[0], 2);
	ensure_equals(results[1], 3);

	db.exec("INSERT INTO test (val) VALUES (4)");

	// Select all items > 2
	select.reset();
	select.bind(1, 2);
	results.clear();
	while (select.step())
		results.push_back(select.fetch<int>(0));
	ensure_equals(results.size(), 2u);
	ensure_equals(results[0], 3);
	ensure_equals(results[1], 4);
}

// Test rollback in the middle of a query
template<> template<>
void to::test<3>()
{
    try {
        Pending p(new SqliteTransaction(db));
        db.exec("INSERT INTO test (val) VALUES (1)");
        db.exec("INSERT INTO test (val) VALUES (2)");
        db.exec("INSERT INTO test (val) VALUES (3)");

        PrecompiledQuery select("select", db);
        select.compile("SELECT * FROM test");
        select.step();
        // Commenting this out works, because the PrecompiledQuery is
        // finalised by its destructor before Pending::rollback is
        // called
        //p.rollback();
        throw std::runtime_error("no problem");
    } catch (std::runtime_error& e) {
        //cerr << e.what() << endl;
        wassert(actual(e.what()).contains("no problem"));
    }
}

// Test inserting 64bit size_t values
template<> template<>
void to::test<4>()
{
	size_t val = 0x42FFFFffffFFFFLLU;
	PrecompiledQuery insert("insert", db);
	insert.compile("INSERT INTO test (val) VALUES (?)");
	insert.bind(1, val);
	while (insert.step())
		;

	PrecompiledQuery select("select", db);
	select.compile("SELECT id, val FROM test ORDER BY id DESC LIMIT 1");

	size_t val1;
	while (select.step())
		val1 = select.fetch<size_t>(1);

	ensure_equals(val1, val);
}

// Test inserting 64bit off_t values
template<> template<>
void to::test<5>()
{
	off_t val = 0x42FFFFffffFFFFLLU;
	PrecompiledQuery insert("insert", db);
	insert.compile("INSERT INTO test (val) VALUES (?)");
	insert.bind(1, val);
	while (insert.step())
		;

	PrecompiledQuery select("select", db);
	select.compile("SELECT id, val FROM test ORDER BY id DESC LIMIT 1");

	off_t val1;
	while (select.step())
		val1 = select.fetch<off_t>(1);

	ensure_equals(val1, val);
}


}

// vim:set ts=4 sw=4:
