#include "arki/tests/tests.h"
#include "sqlite.h"

namespace {
using namespace std;
using namespace arki;
using namespace arki::tests;
using namespace arki::utils::sqlite;

class Tests : public TestCase
{
    using TestCase::TestCase;
    void register_tests() override;
} test("arki_utils_sqlite");

void Tests::register_tests() {

// Test running one shot insert queries
add_method("oneshot", [] {
    SQLiteDB db;
    db.open(":memory:");
    db.exec("CREATE TABLE test (id INTEGER PRIMARY KEY, val INTEGER NOT NULL)");

	db.exec("INSERT INTO test (val) VALUES (1)");
	int id = db.lastInsertID();

    db.exec("INSERT INTO test (val) VALUES (2)");
    wassert(actual(db.lastInsertID()) > id);

    db.exec("INSERT INTO test (val) VALUES (3)");
    wassert(actual(db.lastInsertID()) > id);

    db.exec("INSERT INTO test (val) VALUES (-2)");
    wassert(actual(db.lastInsertID()) > id);
});

// Test precompile queries
add_method("precompile", [] {
    SQLiteDB db;
    db.open(":memory:");
    db.exec("CREATE TABLE test (id INTEGER PRIMARY KEY, val INTEGER NOT NULL)");

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
    wassert(actual(results.size()) == 2u);
    wassert(actual(results[0]) == 2);
    wassert(actual(results[1]) == 3);

    db.exec("INSERT INTO test (val) VALUES (4)");

	// Select all items > 2
	select.reset();
	select.bind(1, 2);
	results.clear();
	while (select.step())
		results.push_back(select.fetch<int>(0));
    wassert(actual(results.size()) == 2u);
    wassert(actual(results[0]) == 3);
    wassert(actual(results[1]) == 4);
});

// Test rollback in the middle of a query
add_method("rollback", [] {
    SQLiteDB db;
    db.open(":memory:");
    db.exec("CREATE TABLE test (id INTEGER PRIMARY KEY, val INTEGER NOT NULL)");

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
        wassert(actual(e.what()).contains("no problem"));
    }
});

// Test inserting 64bit size_t values
add_method("64bit_size_t", [] {
    SQLiteDB db;
    db.open(":memory:");
    db.exec("CREATE TABLE test (id INTEGER PRIMARY KEY, val INTEGER NOT NULL)");

	size_t val = 0x42FFFFffffFFFFLLU;
	PrecompiledQuery insert("insert", db);
	insert.compile("INSERT INTO test (val) VALUES (?)");
	insert.bind(1, val);
	while (insert.step())
		;

	PrecompiledQuery select("select", db);
	select.compile("SELECT id, val FROM test ORDER BY id DESC LIMIT 1");

    size_t val1 = 0xfafef1;
    while (select.step())
        val1 = select.fetch<size_t>(1);

    wassert(actual(val1) == val);
});

// Test inserting 64bit off_t values
add_method("64bit_off_t", [] {
    SQLiteDB db;
    db.open(":memory:");
    db.exec("CREATE TABLE test (id INTEGER PRIMARY KEY, val INTEGER NOT NULL)");

	off_t val = 0x42FFFFffffFFFFLLU;
	PrecompiledQuery insert("insert", db);
	insert.compile("INSERT INTO test (val) VALUES (?)");
	insert.bind(1, val);
	while (insert.step())
		;

	PrecompiledQuery select("select", db);
	select.compile("SELECT id, val FROM test ORDER BY id DESC LIMIT 1");

    off_t val1 = 0xfafef1;
    while (select.step())
        val1 = select.fetch<off_t>(1);

    wassert(actual(val1) == val);
});

}

}
