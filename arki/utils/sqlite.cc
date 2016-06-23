#include "config.h"
#include <arki/utils/sqlite.h>
#include <arki/binary.h>
#include <sstream>
#include <unistd.h>

// FIXME: for debugging
//#include <iostream>

using namespace std;

namespace arki {
namespace utils {
namespace sqlite {

// Note: msg will be deallocated using sqlite3_free
SQLiteError::SQLiteError(char* sqlite_msg, const std::string& msg)
    : std::runtime_error(msg + ": " + sqlite_msg)
{
    sqlite3_free(sqlite_msg);
}

SQLiteError::SQLiteError(sqlite3* db, const std::string& msg)
    : std::runtime_error(msg + ": " + sqlite3_errmsg(db))
{
}

DuplicateInsert::DuplicateInsert(const std::string& msg)
    : std::runtime_error(msg + ": duplicate element")
{
}

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
void SQLiteDB::open(const std::string& pathname, int timeout_ms)
{
	int rc;
	rc = sqlite3_open(pathname.c_str(), &m_db);
	if (rc != SQLITE_OK)
	       	throw SQLiteError(m_db, "opening database");

	// Set the busy timeout to an hour (in milliseconds)
	if (timeout_ms > 0)
	{
		rc = sqlite3_busy_timeout(m_db, timeout_ms);
		if (rc != SQLITE_OK)
			throw SQLiteError(m_db, "setting busy timeout");
	}
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
	char* err;
	int rc = sqlite3_exec(m_db, query.c_str(), 0, 0, &err);
	if (rc != SQLITE_OK)
	       	throw SQLiteError(err, "executing query " + query);
}

void SQLiteDB::checkpoint()
{
    int rc = sqlite3_wal_checkpoint_v2(m_db, NULL, SQLITE_CHECKPOINT_PASSIVE, NULL, NULL);
    if (rc != SQLITE_OK)
        throw SQLiteError(m_db, "checkpointing database");
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
		sqlite3_reset(m_last_insert_id);
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

Query::~Query()
{
	if (m_stm) sqlite3_finalize(m_stm);
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
    {
        stringstream ss;
        ss << "cannot bind string to " << name << " query parameter #" << idx;
        m_db.throwException(ss.str());
    }
}

void Query::bind(int idx, const std::string& str)
{
    // Bind parameters.  We use SQLITE_STATIC assuming that the caller will
    // keep the data alive until the end of the query
    if (sqlite3_bind_text(m_stm, idx, str.data(), str.size(), SQLITE_STATIC) != SQLITE_OK)
    {
        stringstream ss;
        ss << "cannot bind string to " << name << " query parameter #" << idx;
        m_db.throwException(ss.str());
    }
}

void Query::bind(int idx, const std::vector<uint8_t>& buf)
{
    // Bind parameters.  We use SQLITE_STATIC assuming that the caller will
    // keep the data alive until the end of the query
    if (sqlite3_bind_blob(m_stm, idx, buf.data(), buf.size(), SQLITE_STATIC) != SQLITE_OK)
    {
        stringstream ss;
        ss << "cannot bind blob to " << name << " query parameter #" << idx;
        m_db.throwException(ss.str());
    }
}

void Query::bindTransient(int idx, const std::vector<uint8_t>& buf)
{
    if (sqlite3_bind_blob(m_stm, idx, buf.data(), buf.size(), SQLITE_TRANSIENT) != SQLITE_OK)
    {
        stringstream ss;
        ss << "cannot bind buffer to " << name << " query parameter #" << idx;
        m_db.throwException(ss.str());
    }
}

void Query::bindTransient(int idx, const std::string& str)
{
    if (sqlite3_bind_text(m_stm, idx, str.data(), str.size(), SQLITE_TRANSIENT) != SQLITE_OK)
    {
        stringstream ss;
        ss << "cannot bind string to " << name << " query parameter #" << idx;
        m_db.throwException(ss.str());
    }
}

void Query::bindBlob(int idx, const std::string& str)
{
    // Bind parameters.  We use SQLITE_STATIC even if the pointers will be
    // pointing to invalid memory at exit of this function, because
    // sqlite3_step will not be called again without rebinding parameters.
    if (sqlite3_bind_blob(m_stm, idx, str.data(), str.size(), SQLITE_STATIC) != SQLITE_OK)
    {
        stringstream ss;
        ss << "cannot bind string to " << name << " query parameter #" << idx;
        m_db.throwException(ss.str());
    }
}

void Query::bindBlobTransient(int idx, const std::string& str)
{
    if (sqlite3_bind_blob(m_stm, idx, str.data(), str.size(), SQLITE_TRANSIENT) != SQLITE_OK)
    {
        stringstream ss;
        ss << "cannot bind string to " << name << " query parameter #" << idx;
        m_db.throwException(ss.str());
    }
}

void Query::bindNull(int idx)
{
    if (sqlite3_bind_null(m_stm, idx) != SQLITE_OK)
    {
        stringstream ss;
        ss << "cannot bind NULL to " << name << " query parameter #" << idx;
        m_db.throwException(ss.str());
    }
}

void Query::bindType(int idx, const types::Type& item)
{
    vector<uint8_t> buf;
    BinaryEncoder enc(buf);
    item.encodeBinary(enc);
    bindTransient(idx, buf);
}

unique_ptr<types::Type> Query::fetchType(int column)
{
    const uint8_t* buf = (const uint8_t*)fetchBlob(column);
    int len = fetchBytes(column);
    if (len == 0) return unique_ptr<types::Type>();

    BinaryDecoder dec(buf, len);

    TypeCode el_type;
    BinaryDecoder inner = dec.pop_type_envelope(el_type);

    return types::decodeInner(el_type, inner);
}

bool Query::step()
{
	int rc = sqlite3_step(m_stm);
	switch (rc)
	{
		case SQLITE_DONE:
			return false;
		case SQLITE_ROW:
			return true;
		default:
			sqlite3_reset(m_stm);
			m_db.throwException("executing " + name + " query");
	}
	// Not reached, but makes gcc happy
	return false;
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
		sqlite3_reset(m_stm);
		// TODO: check rc here if we want to handle specially the case of
		// SQLITE_CONSTRAINT (for example, if we want to take special
		// action in case of duplicate entries)
		throw SQLiteError(m_db, "executing query " + m_query);
	}
#endif
}


void SqliteTransaction::commit()
{
	committer.commit();
	fired = true;
}

void SqliteTransaction::rollback()
{
	committer.rollback();
	fired = true;
}

bool InsertQuery::step()
{
	int rc = sqlite3_step(m_stm);
	if (rc != SQLITE_DONE)
		rc = sqlite3_reset(m_stm);
	switch (rc)
	{
		case SQLITE_DONE:
			return false;
		case SQLITE_CONSTRAINT:
			throw DuplicateInsert("executing " + name + " query");
		default:
			m_db.throwException("executing " + name + " query");
	}
	// Not reached, but makes gcc happy
	return false;
}


}
}
}
