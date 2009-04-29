/*
 * dsindex - Dataset index infrastructure
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

#include <arki/utils/sqlite.h>
#include <wibble/string.h>
#include <sstream>
#include <unistd.h>

#define LEGACY_SQLITE

// FIXME: for debugging
//#include <iostream>

using namespace std;
using namespace wibble;

namespace arki {
namespace utils {
namespace sqlite {

SQLiteDB::~SQLiteDB() {
	if (m_last_insert_id) sqlite3_finalize(m_last_insert_id);
	if (m_db)
	{
		// Finalise all pending statements (can't do until sqlite 3.6.0)
		//while (sqlite3_stmt* stm = sqlite3_next_stmt(m_db, 0))
		//	sqlite3_finalize(stm);

		int rc = sqlite3_close(m_db);
		if (rc) throw SQLiteError(m_db, "closing database");
	}
}
void SQLiteDB::open(const std::string& pathname)
{
	if (sqlite3_open(pathname.c_str(), &m_db))
		fprintf(stderr, "WARNING DURING SQLITE DESTRUCTION: %s\n", sqlite3_errmsg(m_db));
}

sqlite3_stmt* SQLiteDB::prepare(const std::string& query) const
{
	sqlite3_stmt* stm_query;
	const char* dummy;
#ifdef LEGACY_SQLITE
	int rc = sqlite3_prepare(m_db, query.data(), query.size(), &stm_query, &dummy);
#else
	int rc = sqlite3_prepare_v2(m_db, query.data(), query.size(), &stm_query, &dummy);
#endif
	if (rc != SQLITE_OK)
		throw SQLiteError(m_db, "compiling query " + query);
	return stm_query;
}

void SQLiteDB::exec(const std::string& query)
{
	while (true)
	{
		char* err;
		int rc = sqlite3_exec(m_db, query.c_str(), 0, 0, &err);
		switch (rc)
		{
			case SQLITE_OK: return;
			case SQLITE_BUSY: usleep(50000); break;
			default:
				throw SQLiteError(err, "executing query " + query);
		}
	}
}

int SQLiteDB::lastInsertID()
{
#if 0
	if (!m_last_insert_id)
		m_last_insert_id = prepare("SELECT LAST_INSERT_ROWID()");

	// Reset the query
	if (sqlite3_reset(m_last_insert_id) != SQLITE_OK)
		throwException("resetting SELECT LAST_INSERT_ROWID query");

	// Run the query
	int res, id = -1;
	while ((res = sqlite3_step(m_last_insert_id)) == SQLITE_ROW)
		id = sqlite3_column_int(m_last_insert_id, 0);
	if (res != SQLITE_DONE)
	{
#ifdef LEGACY_SQLITE
		sqlite3_reset(m_last_insert_id);
#endif
		throwException("executing query SELECT LAST_INSERT_ROWID()");
	}
	return id;
#endif
	return sqlite3_last_insert_rowid(m_db);
}

void SQLiteDB::throwException(const std::string& msg) const
{
	throw SQLiteError(m_db, msg);
}

void Query::compile(const std::string& query)
{
	m_stm = m_db.prepare(query);
}

void Query::reset()
{
	// SELECT file, ofs FROM md WHERE id=id
	if (sqlite3_reset(m_stm) != SQLITE_OK)
		m_db.throwException("resetting " + name + " query");
}

void Query::bind(int idx, const char* str, int len)
{
	// Bind parameters.  We use SQLITE_STATIC even if the pointers will be
	// pointing to invalid memory at exit of this function, because
	// sqlite3_step will not be called again without rebinding parameters.
	if (sqlite3_bind_text(m_stm, idx, str, len, SQLITE_STATIC) != SQLITE_OK)
		m_db.throwException("binding string to " + name + " query parameter #" + str::fmt(idx));
}

void Query::bind(int idx, const std::string& str)
{
	// Bind parameters.  We use SQLITE_STATIC even if the pointers will be
	// pointing to invalid memory at exit of this function, because
	// sqlite3_step will not be called again without rebinding parameters.
	if (sqlite3_bind_text(m_stm, idx, str.data(), str.size(), SQLITE_STATIC) != SQLITE_OK)
		m_db.throwException("binding string to " + name + " query parameter #" + str::fmt(idx));
}

void Query::bindTransient(int idx, const std::string& str)
{
	if (sqlite3_bind_text(m_stm, idx, str.data(), str.size(), SQLITE_TRANSIENT) != SQLITE_OK)
		m_db.throwException("binding string to " + name + " query parameter #" + str::fmt(idx));
}

void Query::bindBlob(int idx, const std::string& str)
{
	// Bind parameters.  We use SQLITE_STATIC even if the pointers will be
	// pointing to invalid memory at exit of this function, because
	// sqlite3_step will not be called again without rebinding parameters.
	if (sqlite3_bind_blob(m_stm, idx, str.data(), str.size(), SQLITE_STATIC) != SQLITE_OK)
		m_db.throwException("binding string to " + name + " query parameter #" + str::fmt(idx));
}

void Query::bind(int idx, int val)
{
	if (sqlite3_bind_int(m_stm, idx, val) != SQLITE_OK)
		m_db.throwException("binding int to " + name + " query parameter #" + str::fmt(idx));
}

void Query::bindNull(int idx)
{
	if (sqlite3_bind_null(m_stm, idx) != SQLITE_OK)
		m_db.throwException("binding NULL to " + name + " query parameter #" + str::fmt(idx));
}

void Query::bindUItem(int idx, const UItem<>& item)
{
	if (item.defined())
		bindTransient(idx, item.encode());
	else
		bindNull(idx);
}

UItem<> Query::fetchUItem(int column)
{
	const unsigned char* buf = (const unsigned char*)fetchBlob(column);
	int len = fetchBytes(column);
	if (len == 0)
		return UItem<>();

	const unsigned char* el_start = buf;
	size_t el_len = len;
	types::Code el_type = types::decodeEnvelope(el_start, el_len);
	return types::decodeInner(el_type, el_start, el_len);
}

std::vector< Item<> > Query::fetchItems(int column)
{
	const unsigned char* buf = (const unsigned char*)fetchBlob(column);
	int len = fetchBytes(column);
	vector< Item<> > res;

	// Parse the various elements
	const unsigned char* end = buf + len;
	for (const unsigned char* cur = buf; cur < end; )
	{
		const unsigned char* el_start = cur;
		size_t el_len = end - cur;
		types::Code el_type = types::decodeEnvelope(el_start, el_len);
		res.push_back(types::decodeInner(el_type, el_start, el_len));
		cur = el_start + el_len;
	}

	return res;
}

bool Query::step()
{
	while (true)
	{
		int rc = sqlite3_step(m_stm);
		switch (rc)
		{
			case SQLITE_DONE:
				return false;
			case SQLITE_ROW:
				return true;
			case SQLITE_BUSY:
				usleep(20000);
			       	break;
			default:
#ifdef LEGACY_SQLITE
				sqlite3_reset(m_stm);
#endif
				m_db.throwException("executing " + name + " query");
		}
	}
}

void OneShotQuery::initQueries()
{
#if 0
	const char* dummy;
#ifdef LEGACY_SQLITE
	int rc = sqlite3_prepare(m_db, m_query.data(), m_query.size(), &m_stm, &dummy);
#else
	int rc = sqlite3_prepare_v2(m_db, m_query.data(), m_query.size(), &m_stm, &dummy);
#endif
	if (rc != SQLITE_OK)
		throw SQLiteError(m_db, "compiling query " + m_query);
#endif
}

void OneShotQuery::operator()()
{
	m_db.exec(m_query);
	
#if 0
	// One day someone will tell me how come I can't precompile a BEGIN


	// Reset the query
	int res = sqlite3_reset(m_stm);
	if (res != SQLITE_OK)
		throw SQLiteError(m_db, "resetting query " + m_query);

	// Run the query
	res = sqlite3_step(m_stm);
	if (res != SQLITE_DONE)
	{
		fprintf(stderr, "BOGH %d\n", res);
#ifdef LEGACY_SQLITE
		sqlite3_reset(m_stm);
#endif
		// TODO: check rc here if we want to handle specially the case of
		// SQLITE_CONSTRAINT (for example, if we want to take special
		// action in case of duplicate entries)
		throw SQLiteError(m_db, "executing query " + m_query);
	}
#endif
}

}
}
}
// vim:set ts=4 sw=4:
