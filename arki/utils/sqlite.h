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

#include <arki/transaction.h>
#include <arki/types.h>
#include <wibble/exception.h>
#include <wibble/string.h>
#include <sqlite3.h>
#include <string>
#include <vector>
#include <limits>

// Enable compatibility with old sqlite.
// TODO: use #if macros to test sqlite version instead
//#define LEGACY_SQLITE

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
	/**
	 * Open the database and set the given busy timeout
	 *
	 * Note: to open an in-memory database, use ":memory:" as the pathname
	 */
	void open(const std::string& pathname, int timeout_ms = 3600*1000);

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

	/// Get the message describing the last error message
	std::string errmsg() const { return sqlite3_errmsg(m_db); }
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
	template<typename T>
	void bind(int idx, T val)
	{
		using namespace wibble;
		if (std::numeric_limits<T>::is_exact)
		{
			if (sizeof(T) <= 4)
			{
				if (sqlite3_bind_int(m_stm, idx, val) != SQLITE_OK)
					m_db.throwException("binding int to " + name + " query parameter #" + str::fmt(idx));
			}
			else
			{
				if (sqlite3_bind_int64(m_stm, idx, val) != SQLITE_OK)
					m_db.throwException("binding int64 to " + name + " query parameter #" + str::fmt(idx));
			}
		} else {
			if (sqlite3_bind_double(m_stm, idx, val) != SQLITE_OK)
				m_db.throwException("binding double to " + name + " query parameter #" + str::fmt(idx));
		}
	}

	/// Bind a query parameter
	void bind(int idx, const std::string& str);

	/// Bind a string that will not exist until the query is performed
	void bindTransient(int idx, const std::string& str);

	/// Bind a query parameter as a Blob
	void bindBlob(int idx, const std::string& str);

	/// Bind a blob that will not exist until the query is performed
	void bindBlobTransient(int idx, const std::string& str);

	/// Bind NULL to a query parameter
	void bindNull(int idx);

	/// Bind a UItem
	void bindUItem(int idx, const UItem<>& item);

	/// Bind a vector of UItems
	template<typename TYPE>
	void bindItems(int idx, const std::vector< Item<TYPE> >& items)
	{
		if (items.empty())
			bindNull(idx);
		else
		{
			std::string str;
			for (typename std::vector< Item<TYPE> >::const_iterator i = items.begin();
					i != items.end(); ++i)
				str += i->encode();
			bindBlobTransient(idx, str);
		}
	}

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

	template<typename T>
	T fetch(int column)
	{
		if (std::numeric_limits<T>::is_exact)
		{
			if (sizeof(T) <= 4)
				return sqlite3_column_int(m_stm, column);
			else
				return sqlite3_column_int64(m_stm, column);
		} else
			return sqlite3_column_double(m_stm, column);
	}

	std::string fetchString(int column)
	{
		const char* res = (const char*)sqlite3_column_text(m_stm, column);
		if (res == NULL)
			return std::string();
		else
			return res;
	}
	const void* fetchBlob(int column)
	{
		return sqlite3_column_blob(m_stm, column);
	}
	int fetchBytes(int column)
	{
		return sqlite3_column_bytes(m_stm, column);
	}
	UItem<> fetchUItem(int column);
	std::vector< Item<> > fetchItems(int column);

	template<typename TYPE>
	std::vector< Item<TYPE> > fetchItems(int column)
	{
		const unsigned char* buf = (const unsigned char*)fetchBlob(column);
		int len = fetchBytes(column);
		std::vector< Item<TYPE> > res;

		// Parse the various elements
		const unsigned char* end = buf + len;
		for (const unsigned char* cur = buf; cur < end; )
		{
			const unsigned char* el_start = cur;
			size_t el_len = end - cur;
			types::Code el_type = types::decodeEnvelope(el_start, el_len);
			res.push_back(types::decodeInner(el_type, el_start, el_len).upcast<TYPE>());
			cur = el_start + el_len;
		}

		return res;
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

	Committer(SQLiteDB& db, bool exclusive = false)
		: begin(db, "begin", exclusive ? "BEGIN EXCLUSIVE" : "BEGIN"),
	          commit(db, "commit", "COMMIT"),
		  rollback(db, "rollback", "ROLLBACK") {}

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

	Committer committer;
	bool fired;

	SqliteTransaction(SQLiteDB& db, bool exclusive = false)
	       	: _ref(0), committer(db, exclusive), fired(false)
	{
		committer.begin();
	}
	~SqliteTransaction()
	{
		if (!fired) committer.rollback();
	}

	void commit();
	void rollback();
};

}
}
}

// vim:set ts=4 sw=4:
#endif
