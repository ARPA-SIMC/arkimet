#ifndef ARKI_UTILS_SQLITE_H
#define ARKI_UTILS_SQLITE_H

/*
 * sqlite - SQLite helpers
 *
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

#include <wibble/exception.h>
#include <arki/transaction.h>
#include <sqlite3.h>
#include <string>
#include <vector>

namespace arki {
namespace utils {
namespace sqlite {

class SQLiteError : public wibble::exception::Generic
{
	std::string m_error;

public:
	// Note: msg will be deallocated using sqlite3_free
	SQLiteError(char* msg, const std::string& context)
		: Generic(context), m_error(msg)
	{
		sqlite3_free(msg);
	}

	SQLiteError(sqlite3* db, const std::string& context)
		: Generic(context), m_error(sqlite3_errmsg(db)) {}

	~SQLiteError() throw () {}

	virtual const char* type() const throw () { return "SQLiteError"; }
	virtual std::string desc() const throw () { return m_error; }
};

class SQLiteDB
{
private:
	sqlite3* m_db;
	sqlite3_stmt* m_last_insert_id;

	// Disallow copy
	SQLiteDB(const SQLiteDB&);
	SQLiteDB& operator=(const SQLiteDB&);

public:
	SQLiteDB() : m_db(0), m_last_insert_id(0) {}
	~SQLiteDB();

	bool isOpen() const { return m_db != 0; }
	void open(const std::string& pathname);

	//operator const sqlite3*() const { return m_db; }
	//operator sqlite3*() { return m_db; }

	/// Prepare a query
	sqlite3_stmt* prepare(const std::string& query) const;

	/**
	 * Run a query that has no return values.
	 *
	 * If it fails with SQLITE_BUSY, wait a bit and retry.
	 */
	void exec(const std::string& query);

	/// Run a SELECT LAST_INSERT_ROWID() statement and return the result
	int lastInsertID();

	/// Throw a SQLiteError exception with the given extra information
	void throwException(const std::string& msg) const;
};

/**
 * Base for classes that work on a sqlite statement
 */
class Query
{
protected:
	// This is just a copy of what is in the main index
	SQLiteDB& m_db;
	// Precompiled statement
	sqlite3_stmt* m_stm;
	// Name of the query, to use in error messages
	std::string name;

public:
	Query(const std::string& name, SQLiteDB& db) : m_db(db), m_stm(0), name(name) {}
	~Query() { if (m_stm) sqlite3_finalize(m_stm); }

	/// Compile the query
	void compile(const std::string& query);

	/// Reset the statement status prior to running the query
	void reset();

	/// Bind a query parameter
	void bind(int idx, const char* str, int len);

	/// Bind a query parameter
	void bind(int idx, const std::string& str);

	/// Bind a query parameter as a Blob
	void bindBlob(int idx, const std::string& str);

	/// Bind a query parameter
	void bind(int idx, int val);

	/// Bind NULL to a query parameter
	void bindNull(int idx);

	/**
	 * Fetch a query row.
	 *
	 * Return true when there is another row to fetch, else false
	 */
	bool step();

	int fetchType(int column)
	{
		return sqlite3_column_type(m_stm, column);
	}

	std::string fetchString(int column)
	{
		return (const char*)sqlite3_column_text(m_stm, column);
	}
	size_t fetchSizeT(int column)
	{
		return sqlite3_column_int(m_stm, column);
	}
	int fetchInt(int column)
	{
		return sqlite3_column_int(m_stm, column);
	}
	const void* fetchBlob(int column)
	{
		return sqlite3_column_blob(m_stm, column);
	}
	int fetchBytes(int column)
	{
		return sqlite3_column_bytes(m_stm, column);
	}
};

/**
 * Simple use of a precompiled query
 */
class PrecompiledQuery : public Query
{
public:
	PrecompiledQuery(const std::string& name, SQLiteDB& db) : Query(name, db) {}
};

/**
 * Precompiled (just not now, but when I'll understand how to do it) query that
 * gets run without passing parameters nor reading results.
 *
 * This is, in fact, currently recompiled every time.  The reason is because
 * this is mainly used to run statements like BEGIN and COMMIT, that I haven't
 * been able to precompile.
 *
 * If the query fails with SQLITE_BUSY, will wait a bit and then retry.
 */
class OneShotQuery : public Query
{
protected:
	std::string m_query;

public:
	OneShotQuery(SQLiteDB& db, const std::string& name, const std::string& query) : Query(name, db), m_query(query) {}

	void initQueries();

	void operator()();
};

/**
 * Holds precompiled (just not yet) begin, commit and rollback statements
 */
struct Committer
{
	OneShotQuery begin;
	OneShotQuery commit;
	OneShotQuery rollback;

	Committer(SQLiteDB& db)
		: begin(db, "begin", "BEGIN EXCLUSIVE"), commit(db, "commit", "COMMIT"), rollback(db, "rollback", "ROLLBACK") {}

	void initQueries()
	{
		begin.initQueries();
		commit.initQueries();
		rollback.initQueries();
	}
};

/**
 * RAII-style transaction
 */
struct SqliteTransaction : public Transaction
{
	int _ref;

	Committer& committer;
	bool fired;

	SqliteTransaction(Committer& c) : _ref(0), committer(c), fired(false)
	{
		committer.begin();
	}
	~SqliteTransaction()
	{
		if (!fired) committer.rollback();
	}

	void commit()
	{
		committer.commit();
		fired = true;
	}

	void rollback()
	{
		committer.rollback();
		fired = true;
	}
};

}
}
}

// vim:set ts=4 sw=4:
#endif
