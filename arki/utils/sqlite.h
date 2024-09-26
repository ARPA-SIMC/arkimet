#ifndef ARKI_UTILS_SQLITE_H
#define ARKI_UTILS_SQLITE_H

/// SQLite helpers

#include <arki/core/transaction.h>
#include <arki/types/fwd.h>
#include <sqlite3.h>
#include <filesystem>
#include <string>
#include <sstream>
#include <vector>
#include <limits>
#include <cstdint>

// Enable compatibility with old sqlite.
// TODO: use #if macros to test sqlite version instead
//#define LEGACY_SQLITE

namespace arki {
namespace utils {
namespace sqlite {

class SQLiteError : public std::runtime_error
{
public:
    // Note: msg will be deallocated using sqlite3_free
    SQLiteError(char* sqlite_msg, const std::string& msg);

    SQLiteError(sqlite3* db, const std::string& msg);
};

/**
 * Exception thrown in case of duplicate inserts
 */
struct DuplicateInsert : public std::runtime_error
{
    DuplicateInsert(sqlite3* db, const std::string& msg);
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

    operator sqlite3*() const { return m_db; }

    bool isOpen() const { return m_db != 0; }
    /**
     * Open the database and set the given busy timeout
     *
     * Note: to open an in-memory database, use ":memory:" as the pathname
     */
    void open(const std::filesystem::path& pathname, int timeout_ms = 3600*1000);

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

    /**
     * Run a query that has no return values.
     *
     * In case of error, use nag::warning instead of throwing an exception
     *
     * If it fails with SQLITE_BUSY, wait a bit and retry.
     */
    void exec_nothrow(const std::string& query) noexcept;

	/// Run a SELECT LAST_INSERT_ROWID() statement and return the result
	int lastInsertID();

    /// Throw a SQLiteError exception with the given extra information
    [[noreturn]] void throwException(const std::string& msg) const;

	/// Get the message describing the last error message
	std::string errmsg() const { return sqlite3_errmsg(m_db); }

	/// Checkpoint the journal contents into the database file
	void checkpoint();

    /**
     * Enable/change/disable SQLite tracing.
     *
     * See sqlite3_trace_v2 docmentation for values for mask use 0 to disable.
     */
    void trace(unsigned mask=SQLITE_TRACE_STMT);
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
    sqlite3_stmt* m_stm = nullptr;
    // Name of the query, to use in error messages
    std::string name;

public:
    Query(const std::string& name, SQLiteDB& db) : m_db(db), name(name) {}
    ~Query();

    /// Check if the query has already been compiled
    bool compiled() const { return m_stm != nullptr; }

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
        if (std::numeric_limits<T>::is_exact)
        {
            if (sizeof(T) <= 4)
            {
                if (sqlite3_bind_int(m_stm, idx, val) != SQLITE_OK)
                {
                    std::stringstream ss;
                    ss << name << ": cannot bind query parameter #" << idx << " as int";
                    m_db.throwException(ss.str());
                }
            }
            else
            {
                if (sqlite3_bind_int64(m_stm, idx, val) != SQLITE_OK)
                {
                    std::stringstream ss;
                    ss << name << ": cannot bind query parameter #" << idx << " as int64";
                    m_db.throwException(ss.str());
                }
            }
        } else {
            if (sqlite3_bind_double(m_stm, idx, val) != SQLITE_OK)
            {
                std::stringstream ss;
                ss << name << ": cannot bind query parameter #" << idx << " as double";
                m_db.throwException(ss.str());
            }
        }
    }

    /// Bind a query parameter
    void bind(int idx, const std::string& str);

    /// Bind a Blob query parameter
    void bind(int idx, const std::vector<uint8_t>& str);

    /// Bind a buffer that will not exist until the query is performed
    void bindTransient(int idx, const std::vector<uint8_t>& buf);

	/// Bind a string that will not exist until the query is performed
	void bindTransient(int idx, const std::string& str);

	/// Bind a query parameter as a Blob
	void bindBlob(int idx, const std::string& str);

	/// Bind a blob that will not exist until the query is performed
	void bindBlobTransient(int idx, const std::string& str);

	/// Bind NULL to a query parameter
	void bindNull(int idx);

    /// Bind a UItem
    void bindType(int idx, const types::Type& item);

	/**
	 * Fetch a query row.
	 *
	 * Return true when there is another row to fetch, else false
	 */
	bool step();

    /// Check if an output column is NULL
    bool isNULL(int column)
    {
        return sqlite3_column_type(m_stm, column) == SQLITE_NULL;
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

    /// Run the query, ignoring all results
    void execute();

    /**
     * Run the query, calling on_row for every row in the result.
     *
     * At the end of the function, the statement is reset, even in case an
     * exception is thrown.
     */
    void execute(std::function<void()> on_row);

    /**
     * Bind all the arguments in a single invocation.
     *
     * Note that the parameter positions are used as bind column numbers, so
     * calling this function twice will re-bind columns instead of adding new
     * ones.
     */
    template<typename... Args> void bind_all(const Args& ...args)
    {
        bindn<sizeof...(args)>(args...);
    }

    /// bind_all and execute
    template<typename... Args> void run(const Args& ...args)
    {
        bindn<sizeof...(args)>(args...);
        execute();
    }

private:
    // Implementation of variadic bind: terminating condition
    template<size_t total> void bindn() {}
    // Implementation of variadic bind: recursive iteration over the parameter pack
    template<size_t total, typename ...Args, typename T> void bindn(const T& first, const Args& ...args)
    {
        bind(total - sizeof...(args), first);
        bindn<total>(args...);
    }
};

/**
 * Simple use of a precompiled query
 */
class PrecompiledQuery : public Query
{
public:
    using Query::Query;
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

    /**
     * Run but avoid throwing exceptions.
     *
     * In case of error, use nag::warning instead of throwing an exception
     */
    void nothrow() noexcept;
};

/**
 * Holds precompiled (just not yet) begin, commit and rollback statements
 */
struct Committer
{
	OneShotQuery begin;
	OneShotQuery commit;
	OneShotQuery rollback;

    Committer(SQLiteDB& db, const char* type=nullptr);

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
struct SqliteTransaction : public core::Transaction
{
    int _ref;

    Committer committer;
    bool fired;

    SqliteTransaction(SQLiteDB& db, const char* type=nullptr)
        : _ref(0), committer(db, type), fired(false)
    {
        committer.begin();
    }
    ~SqliteTransaction()
    {
        if (!fired) committer.rollback();
    }

    void commit() override;
    void rollback() override;
    void rollback_nothrow() noexcept override;
};

/**
 * Query that in case of duplicates throws DuplicateInsert
 */
struct InsertQuery : public utils::sqlite::Query
{
    InsertQuery(utils::sqlite::SQLiteDB& db) : utils::sqlite::Query("insert", db) {}

    // Step, but throw DuplicateInsert in case of duplicates
    bool step();
};

struct Trace
{
    SQLiteDB& db;

    Trace(SQLiteDB& db, unsigned mask=SQLITE_TRACE_STMT);
    ~Trace();
};

}
}
}
#endif
