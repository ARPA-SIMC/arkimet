#include "config.h"
#include "index.h"
#include "arki/dataset/maintenance.h"
#include "arki/configfile.h"
#include "arki/reader.h"
#include "arki/metadata.h"
#include "arki/metadata/collection.h"
#include "arki/matcher.h"
#include "arki/matcher/reftime.h"
#include "arki/dataset.h"
#include "arki/dataset/lock.h"
#include "arki/types/reftime.h"
#include "arki/types/source.h"
#include "arki/types/value.h"
#include "arki/summary.h"
#include "arki/summary/stats.h"
#include "arki/utils/files.h"
#include "arki/sort.h"
#include "arki/nag.h"
#include "arki/runtime/io.h"
#include "arki/utils/string.h"
#include "arki/utils/sys.h"

#include <sstream>
#include <ctime>
#include <cassert>
#include <cerrno>
#include <cstdlib>

using namespace std;
using namespace arki;
using namespace arki::types;
using namespace arki::utils;
using namespace arki::utils::sqlite;
using namespace arki::dataset::index;
using arki::core::Time;

namespace arki {
namespace dataset {
namespace iseg {

struct IndexGlobalData
{
    std::set<types::Code> all_components;

    IndexGlobalData() {
        all_components.insert(TYPE_ORIGIN);
        all_components.insert(TYPE_PRODUCT);
        all_components.insert(TYPE_LEVEL);
        all_components.insert(TYPE_TIMERANGE);
        all_components.insert(TYPE_AREA);
        all_components.insert(TYPE_PRODDEF);
        all_components.insert(TYPE_RUN);
        //all_components.insert(TYPE_REFTIME);
        all_components.insert(TYPE_QUANTITY);
        all_components.insert(TYPE_TASK);
    }
};
static IndexGlobalData igd;

Index::Index(std::shared_ptr<const iseg::Config> config, const std::string& data_relpath, std::shared_ptr<dataset::Lock> lock)
    : m_config(config),
      data_relpath(data_relpath),
      data_pathname(str::joinpath(config->path, data_relpath)),
      index_pathname(data_pathname + ".index"),
      lock(lock)
#if 0
    :  m_get_id("getid", m_db), m_get_current("getcurrent", m_db)
#endif
{
    if (not config->unique.empty())
        m_uniques = new Aggregate(m_db, "mduniq", config->unique);

    // Instantiate m_others after the database has been opened, so we can scan
    // the database to see if some attributes are not available
}

Index::~Index()
{
    delete m_uniques;
    delete m_others;
}

Pending Index::begin_transaction()
{
    return Pending(new SqliteTransaction(m_db));
}

std::set<types::Code> Index::available_other_tables() const
{
    // See what metadata types are already handled by m_uniques,
    // if any
    std::set<types::Code> unique_members;
    if (m_uniques)
        unique_members = m_uniques->members();

    // See what columns are available in the database
    std::set<types::Code> available_columns;
    Query q("gettables", m_db);
    q.compile("SELECT name FROM sqlite_master WHERE type='table'");
    while (q.step())
    {
        string name = q.fetchString(0);
        if (!str::startswith(name, "sub_"))
            continue;
        types::Code code = types::checkCodeName(name.substr(4));
        if (code != TYPE_INVALID
         && unique_members.find(code) == unique_members.end()
         && igd.all_components.find(code) != igd.all_components.end())
            available_columns.insert(code);
    }

    return available_columns;
}

std::set<types::Code> Index::all_other_tables() const
{
    std::set<types::Code> res;

    // See what metadata types are already handled by m_uniques,
    // if any
    std::set<types::Code> unique_members;
    if (m_uniques)
        unique_members = m_uniques->members();

    // Handle all the rest
    for (std::set<types::Code>::const_iterator i = igd.all_components.begin();
            i != igd.all_components.end(); ++i)
        if (unique_members.find(*i) == unique_members.end())
            res.insert(*i);
    return res;
}

void Index::init_others()
{
    std::set<types::Code> other_members = available_other_tables();
    if (not other_members.empty())
        m_others = new Aggregate(m_db, "mdother", other_members);
}

#if 0
void Contents::initQueries()
{
    string query = "SELECT id FROM md WHERE reftime=?";
    if (m_uniques) query += " AND uniq=?";
    if (m_others) query += " AND other=?";
    if (config().smallfiles) query += " AND data=?";
    m_get_id.compile(query);

    query = "SELECT id, format, file, offset, size, notes, reftime";
    if (m_uniques) query += ", uniq";
    if (m_others) query += ", other";
    if (config().smallfiles) query += ", data";
    query += " FROM md WHERE reftime=?";
    if (m_uniques) query += " AND uniq=?";
    m_get_current.compile(query);
}
#endif

std::set<types::Code> Index::unique_codes() const
{
    std::set<types::Code> res;
    if (m_uniques) res = m_uniques->members();
    res.insert(TYPE_REFTIME);
    return res;
}

void Index::setup_pragmas()
{
    // Faster but riskier, since we do not have a flagfile to trap
    // interrupted transactions
    //m_db.exec("PRAGMA synchronous = OFF");
    // Faster but riskier, since we do not have a flagfile to trap
    // interrupted transactions
    //m_db.exec("PRAGMA journal_mode = MEMORY");
    // Truncate the journal instead of delete: faster on many file systems
    // m_db.exec("PRAGMA journal_mode = TRUNCATE");
    // Zero the header of the journal instead of delete: faster on many file systems
    // Use a WAL journal, which allows reads and writes together
    m_db.exec("PRAGMA journal_mode = WAL");
    // Also, since the way we do inserts cause no trouble if a reader reads a
    // partial insert, we do not need read locking
    //m_db.exec("PRAGMA read_uncommitted = 1");
    // Use new features, if we write we read it, so we do not need to
    // support sqlite < 3.3.0 if we are above that version
    m_db.exec("PRAGMA legacy_file_format = 0");
}

#if 0
int Contents::id(const Metadata& md) const
{
    m_get_id.reset();

    int idx = 0;
    const reftime::Position* rt = md.get<reftime::Position>();
    if (!rt) return -1;
    string sqltime = rt->time.to_sql();
    m_get_id.bind(++idx, sqltime);

    if (m_uniques)
    {
        int id = m_uniques->get(md);
        // If we do not have this aggregate, then we do not have this metadata
        if (id == -1) return -1;
        m_get_id.bind(++idx, id);
    }

    if (m_others)
    {
        int id = m_others->get(md);
        // If we do not have this aggregate, then we do not have this metadata
        if (id == -1) return -1;
        m_get_id.bind(++idx, id);
    }

    int id = -1;
    while (m_get_id.step())
        id = m_get_id.fetch<int>(0);

    return id;
}

bool Contents::get_current(const Metadata& md, Metadata& current) const
{
    m_get_current.reset();

    int idx = 0;
    const reftime::Position* rt = md.get<reftime::Position>();
    if (!rt) return -1;
    string sqltime = rt->time.to_sql();
    m_get_current.bind(++idx, sqltime);

    int id_unique = -1;
    if (m_uniques)
    {
        id_unique = m_uniques->get(md);
        // If we do not have this aggregate, then we do not have this metadata
        if (id_unique == -1) return false;
        m_get_current.bind(++idx, id_unique);
    }

    bool found = false;
    while (m_get_current.step())
    {
        current.clear();
        build_md(m_get_current, current);
        found = true;
    }
    return found;
}

size_t Contents::count() const
{
    Query sq("count", m_db);
    sq.compile("SELECT COUNT(*) FROM md");
    size_t res = 0;
    while (sq.step())
        res = sq.fetch<size_t>(0);
    return res;
}

bool Contents::segment_timespan(const std::string& relname, Time& start_time, Time& end_time) const
{
    Query sq("max_file_reftime", m_db);
    sq.compile("SELECT MIN(reftime), MAX(reftime) FROM md WHERE file=?");
    sq.bind(1, relname);
    bool res = false;
    while (sq.step())
    {
        start_time.set_sql(sq.fetchString(0));
        end_time.set_sql(sq.fetchString(1));
        res = true;
    }
    return res;
}
#endif

void Index::scan(metadata_dest_func dest, const std::string& order_by) const
{
    string query = "SELECT m.offset, m.size, m.notes, m.reftime";
    if (m_uniques) query += ", m.uniq";
    if (m_others) query += ", m.other";
    if (config().smallfiles) query += ", m.data";
    query += " FROM md AS m";
    query += " ORDER BY " + order_by;

    Query mdq("scan_file_md", m_db);
    mdq.compile(query);

    auto reader = arki::Reader::create_new(data_pathname, lock);

    while (mdq.step())
    {
        // Rebuild the Metadata
        unique_ptr<Metadata> md(new Metadata);
        build_md(mdq, *md, reader);
        dest(move(md));
    }
}

void Index::query_segment(metadata_dest_func dest) const
{
    scan(dest);
}

static void db_time_extremes(utils::sqlite::SQLiteDB& db, unique_ptr<Time>& begin, unique_ptr<Time>& end)
{
    // SQLite can compute min and max of an indexed column very fast,
    // provided that it is the ONLY thing queried.
    Query q1("min_date", db);
    q1.compile("SELECT MIN(reftime) FROM md");
    while (q1.step())
        if (!q1.isNULL(0))
            begin.reset(new Time(Time::create_sql(q1.fetchString(0))));

    Query q2("min_date", db);
    q2.compile("SELECT MAX(reftime) FROM md");
    while (q2.step())
    {
        if (!q2.isNULL(0))
            end.reset(new Time(Time::create_sql(q2.fetchString(0))));
    }
}

void Index::add_joins_and_constraints(const Matcher& m, std::string& query) const
{
    vector<string> constraints;

    // Add database constraints
    if (not m.empty())
    {
        unique_ptr<Time> begin;
        unique_ptr<Time> end;
        if (!m.restrict_date_range(begin, end))
            // The matcher matches an impossible datetime span: convert it
            // into an impossible clause that evaluates quickly
            constraints.push_back("1 == 2");
        else if (!begin.get() && !end.get())
        {
            // No restriction on a range of reftimes, but still add sql
            // constraints if there is an unbounded reftime matcher (#116)
            if (auto reftime = m.get(TYPE_REFTIME))
                constraints.push_back(reftime->toReftimeSQL("reftime"));
        } else {
            // Compare with the reftime bounds in the database
            unique_ptr<Time> db_begin;
            unique_ptr<Time> db_end;
            db_time_extremes(m_db, db_begin, db_end);
            if (db_begin.get() && db_end.get())
            {
                // Intersect the time bounds of the query with the time
                // bounds of the database
                if (!begin.get())
                   begin.reset(new Time(*db_begin));
                else if (*begin < *db_begin)
                   *begin = *db_begin;
                if (!end.get())
                   end.reset(new Time(*db_end));
                else if (*end > *db_end)
                   *end = *db_end;
                long long int qrange = Time::duration(*begin, *end);
                long long int dbrange = Time::duration(*db_begin, *db_end);
                // If the query chooses less than 20%
                // if the time span, force the use of
                // the reftime index
                if (dbrange > 0 && qrange * 100 / dbrange < 20)
                {
                    query += " INDEXED BY md_idx_reftime";
                    constraints.push_back("reftime BETWEEN \'" + begin->to_sql()
                            + "\' AND \'" + end->to_sql() + "\'");
                }
            }

            if (auto reftime = m.get(TYPE_REFTIME))
                constraints.push_back(reftime->toReftimeSQL("reftime"));
        }

        // Join with the aggregate tables and add constraints on aggregate tables
        if (m_uniques)
        {
            string s = m_uniques->make_subquery(m);
            if (!s.empty())
                constraints.push_back("uniq IN ("+s+")");
        }

        if (m_others)
        {
            string s = m_others->make_subquery(m);
            if (!s.empty())
                constraints.push_back("other IN ("+s+")");
        }
    }

    if (!constraints.empty())
        query += " WHERE " + str::join(" AND ", constraints.begin(), constraints.end());
}

void Index::build_md(Query& q, Metadata& md, std::shared_ptr<arki::Reader> reader) const
{
    // Rebuild the Metadata
    if (reader)
        md.set_source(Source::createBlob(
                config().format, config().path, data_relpath,
                q.fetch<uint64_t>(0), q.fetch<uint64_t>(1), reader));
    else
        md.set_source(Source::createBlobUnlocked(
                config().format, config().path, data_relpath,
                q.fetch<uint64_t>(0), q.fetch<uint64_t>(1)));
    // md.notes = mdq.fetchItems<types::Note>(5);
    const uint8_t* notes_p = (const uint8_t*)q.fetchBlob(2);
    int notes_l = q.fetchBytes(2);
    md.set_notes_encoded(vector<uint8_t>(notes_p, notes_p + notes_l));
    md.set(Reftime::createPosition(Time::create_sql(q.fetchString(3))));
    int j = 4;
    if (m_uniques)
    {
        if (!q.isNULL(j))
            m_uniques->read(q.fetch<int>(j), md);
        ++j;
    }
    if (m_others)
    {
        if (!q.isNULL(j))
            m_others->read(q.fetch<int>(j), md);
        ++j;
    }
    if (config().smallfiles)
    {
        if (!q.isNULL(j))
        {
            string data = q.fetchString(j);
            md.set(types::Value::create(data));
        }
        ++j;
    }
}

bool Index::query_data(const dataset::DataQuery& q, metadata_dest_func dest)
{
    string query = "SELECT m.offset, m.size, m.notes, m.reftime";

    if (m_uniques) query += ", m.uniq";
    if (m_others) query += ", m.other";
    if (config().smallfiles) query += ", m.data";

    query += " FROM md AS m";

    try {
        add_joins_and_constraints(q.matcher, query);
    } catch (NotFound) {
        // If one of the subqueries did not find any match, we can directly
        // return true here, as we are not going to get any result
        return true;
    }

    query += " ORDER BY m.reftime";

    nag::verbose("Running query %s", query.c_str());

    metadata::Collection mdbuf;
    std::shared_ptr<arki::Reader> reader;
    if (q.with_data)
        reader = arki::Reader::create_new(data_pathname, lock);

    // Limited scope for mdq, so we finalize the query before starting to
    // emit results
    {
        Query mdq("mdq", m_db);
        mdq.compile(query);

        // TODO: see if it's worth sorting mdbuf by file and offset

//fprintf(stderr, "PRE\n");
//system(str::fmtf("ps u %d >&2", getpid()).c_str());
        while (mdq.step())
        {
            // Rebuild the Metadata
            unique_ptr<Metadata> md(new Metadata);
            build_md(mdq, *md, reader);
            // Buffer the results in memory, to release the database lock as soon as possible
            mdbuf.acquire(move(md));
        }
//fprintf(stderr, "POST %zd\n", mdbuf.size());
//system(str::fmtf("ps u %d >&2", getpid()).c_str());
    }

    // Sort and output the rest
    if (q.sorter) mdbuf.sort(*q.sorter);

    // pass it to consumer
    bool res = mdbuf.move_to(dest);

//fprintf(stderr, "POSTQ %zd\n", mdbuf.size());
//system(str::fmtf("ps u %d >&2", getpid()).c_str());
    return res;
}

#if 0
void Contents::rebuildSummaryCache()
{
    scache.invalidate();
    // Rebuild all summaries
    Summary s;
    summaryForAll(s);
}

void Contents::querySummaryFromDB(const std::string& where, Summary& summary) const
{
    string query = "SELECT COUNT(1), SUM(size), MIN(reftime), MAX(reftime)";

    if (m_uniques) query += ", uniq";
    if (m_others) query += ", other";

    query += " FROM md";
    if (!where.empty())
        query += " WHERE " + where;

    if (m_uniques && m_others)
        query += " GROUP BY uniq, other";
    else if (m_uniques)
        query += " GROUP BY uniq";
    else if (m_others)
        query += " GROUP BY other";

    nag::verbose("Running query %s", query.c_str());

    Query sq("sq", m_db);
    sq.compile(query);

    while (sq.step())
    {
        // Fill in the summary statistics
        summary::Stats st;
        st.count = sq.fetch<size_t>(0);
        st.size = sq.fetch<unsigned long long>(1);
        st.begin = Time::create_sql(sq.fetchString(2));
        st.end = Time::create_sql(sq.fetchString(3));

        // Fill in the metadata fields
        Metadata md;
        int j = 4;
        if (m_uniques)
        {
            if (!sq.isNULL(j))
                m_uniques->read(sq.fetch<int>(j), md);
            ++j;
        }
        if (m_others)
        {
            if (!sq.isNULL(j))
                m_others->read(sq.fetch<int>(j), md);
            ++j;
        }

        // Feed the results to the summary
        summary.add(md, st);
    }
}

void Contents::summaryForMonth(int year, int month, Summary& out) const
{
    if (!scache.read(out, year, month))
    {
        Summary monthly;
        int nextyear = year + (month/12);
        int nextmonth = (month % 12) + 1;

        char buf[128];
        snprintf(buf, 128, "reftime >= '%04d-%02d-01 00:00:00' AND reftime < '%04d-%02d-01 00:00:00'",
                year, month, nextyear, nextmonth);
        querySummaryFromDB(buf, monthly);

        scache.write(monthly, year, month);
        out.add(monthly);
    }
}

void Contents::summaryForAll(Summary& out) const
{
    if (!scache.read(out))
    {
        // Find the datetime extremes in the database
        unique_ptr<Time> begin;
        unique_ptr<Time> end;
        db_time_extremes(m_db, begin, end);

        // If there is data in the database, get all the involved
        // monthly summaries
        if (begin.get() && end.get())
        {
            int year = begin->ye;
            int month = begin->mo;
            while (year < end->ye || (year == end->ye && month <= end->mo))
            {
                summaryForMonth(year, month, out);

                // Increment the month
                month = (month%12) + 1;
                if (month == 1)
                    ++year;
            }
        }

        scache.write(out);
    }
}
#endif

bool Index::query_summary_from_db(const Matcher& m, Summary& summary) const
{
    string query = "SELECT COUNT(1), SUM(size), MIN(reftime), MAX(reftime)";

    if (m_uniques) query += ", uniq";
    if (m_others) query += ", other";

    query += " FROM md";

    try {
        add_joins_and_constraints(m, query);
    } catch (NotFound) {
        // If one of the subqueries did not find any match, we can directly
        // return true here, as we are not going to get any result
        return true;
    }

    if (m_uniques && m_others)
        query += " GROUP BY uniq, other";
    else if (m_uniques)
        query += " GROUP BY uniq";
    else if (m_others)
        query += " GROUP BY other";

    nag::verbose("Running query %s", query.c_str());

    Query sq("sq", m_db);
    sq.compile(query);

    while (sq.step())
    {
        // Fill in the summary statistics
        summary::Stats st;
        st.count = sq.fetch<size_t>(0);
        st.size = sq.fetch<unsigned long long>(1);
        st.begin = Time::create_sql(sq.fetchString(2));
        st.end = Time::create_sql(sq.fetchString(3));

        // Fill in the metadata fields
        Metadata md;
        int j = 4;
        if (m_uniques)
        {
            if (!sq.isNULL(j))
                m_uniques->read(sq.fetch<int>(j), md);
            ++j;
        }
        if (m_others)
        {
            if (!sq.isNULL(j))
                m_others->read(sq.fetch<int>(j), md);
            ++j;
        }

        // Feed the results to the summary
        summary.add(md, st);
    }

    return true;
}

#if 0
bool Contents::query_summary(const Matcher& matcher, Summary& summary)
{
    // Check if the matcher discriminates on reference times
    unique_ptr<Time> begin;
    unique_ptr<Time> end;
    if (!matcher.restrict_date_range(begin, end))
        return true; // If the matcher contains an impossible reftime, return right away

    if (!begin.get() && !end.get())
    {
        // The matcher does not contain reftime, or does not restrict on a
        // datetime range, we can work with a global summary
        Summary s;
        summaryForAll(s);
        s.filter(matcher, summary);
        return true;
    }

    // Amend open ends with the bounds from the database
    unique_ptr<Time> db_begin;
    unique_ptr<Time> db_end;
    db_time_extremes(m_db, db_begin, db_end);
    // If the database is empty then the result is empty:
    // we are done
    if (!db_begin.get())
        return true;
    bool begin_from_db = false;
    if (!begin.get())
    {
        begin.reset(new Time(*db_begin));
        begin_from_db = true;
    } else if (*begin < *db_begin) {
        *begin = *db_begin;
        begin_from_db = true;
    }
    bool end_from_db = false;
    if (!end.get())
    {
        end.reset(new Time(*db_end));
        end_from_db = true;
    } else if (*end > *db_end) {
        *end = *db_end;
        end_from_db = true;
    }

    // If the interval is under a week, query the DB directly
    long long int range = Time::duration(*begin, *end);
    if (range <= 7 * 24 * 3600)
        return querySummaryFromDB(matcher, summary);

    if (begin_from_db)
    {
        // Round down to month begin, so we reuse the cached summary if
        // available
        *begin = begin->start_of_month();
    }
    if (end_from_db)
    {
        // Round up to month end, so we reuse the cached summary if
        // available
        *end = end->end_of_month();
    }

    // If the selected interval does not envelope any whole month, query
    // the DB directly
    if (!range_envelopes_full_month(*begin, *end))
        return querySummaryFromDB(matcher, summary);

    // Query partial month at beginning, middle whole months, partial
    // month at end. Query whole months at extremes if they are indeed whole
    while (*begin <= *end)
    {
        Time endmonth = begin->end_of_month();

        bool starts_at_beginning = (begin->da == 1 && begin->ho == 0 && begin->mi == 0 && begin->se == 0);
        if (starts_at_beginning && endmonth <= *end)
        {
            Summary s;
            summaryForMonth(begin->ye, begin->mo, s);
            s.filter(matcher, summary);
        } else if (endmonth <= *end) {
            Summary s;
            querySummaryFromDB("reftime >= '" + begin->to_sql() + "'"
                       " AND reftime < '" + endmonth.to_sql() + "'", s);
            s.filter(matcher, summary);
        } else {
            Summary s;
            querySummaryFromDB("reftime >= '" + begin->to_sql() + "'"
                       " AND reftime < '" + end->to_sql() + "'", s);
            s.filter(matcher, summary);
        }

        // Advance to the beginning of the next month
        *begin = begin->start_of_next_month();
    }

    return true;
}

bool Contents::checkSummaryCache(const dataset::Base& ds, Reporter& reporter) const
{
    return scache.check(ds, reporter);
}
#endif

RIndex::RIndex(std::shared_ptr<const iseg::Config> config, const std::string& data_relpath, std::shared_ptr<dataset::ReadLock> lock)
    : Index(config, data_relpath, lock)
{
    if (!sys::access(index_pathname, F_OK))
    {
        stringstream ss;
        ss << "dataset index " << index_pathname << " does not exist";
        throw runtime_error(ss.str());
    }

    m_db.open(index_pathname);
    if (config->trace_sql) m_db.trace();

    init_others();
}


WIndex::WIndex(std::shared_ptr<const iseg::Config> config, const std::string& data_relpath, std::shared_ptr<dataset::Lock> lock)
    : Index(config, data_relpath, lock), m_insert(m_db), m_replace("replace", m_db)
{
    bool need_create = !sys::access(index_pathname, F_OK);

    if (need_create)
    {
        m_db.open(index_pathname);
        if (config->trace_sql) m_db.trace();
        setup_pragmas();
        if (!m_others)
        {
            std::set<types::Code> other_members = all_other_tables();
            if (not other_members.empty())
                m_others = new Aggregate(m_db, "mdother", other_members);
        }
        init_db();
    } else {
        m_db.open(index_pathname);
        if (config->trace_sql) m_db.trace();
        init_others();
    }
}

void WIndex::init_db()
{
    if (m_uniques) m_uniques->initDB(config().index);
    if (m_others) m_others->initDB(config().index);

    // Create the main table
    string query = "CREATE TABLE IF NOT EXISTS md ("
        " offset INTEGER PRIMARY KEY,"
        " size INTEGER NOT NULL,"
        " notes BLOB,"
        " reftime TEXT NOT NULL";
    if (m_uniques) query += ", uniq INTEGER NOT NULL";
    if (m_others) query += ", other INTEGER NOT NULL";
    if (config().smallfiles) query += ", data TEXT";
    if (m_uniques)
        query += ", UNIQUE(reftime, uniq)";
    else
        query += ", UNIQUE(reftime)";
    query += ")";
    m_db.exec(query);

    // Create indices on the main table
    m_db.exec("CREATE INDEX IF NOT EXISTS md_idx_reftime ON md (reftime)");
    if (m_uniques) m_db.exec("CREATE INDEX IF NOT EXISTS md_idx_uniq ON md (uniq)");
    if (m_others) m_db.exec("CREATE INDEX IF NOT EXISTS md_idx_other ON md (other)");
}

void WIndex::compile_insert()
{
    // Precompile insert query
    string un_ot;
    string placeholders;
    if (m_uniques)
    {
        un_ot += ", uniq";
        placeholders += ", ?";
    }
    if (m_others)
    {
        un_ot += ", other";
        placeholders += ", ?";
    }
    if (config().smallfiles)
    {
        un_ot += ", data";
        placeholders += ", ?";
    }

    // Precompile insert
    m_insert.compile("INSERT INTO md (offset, size, notes, reftime" + un_ot + ")"
             " VALUES (?, ?, ?, ?" + placeholders + ")");

    // Precompile replace
    m_replace.compile("INSERT OR REPLACE INTO md (offset, size, notes, reftime" + un_ot + ")"
                  " VALUES (?, ?, ?, ?" + placeholders + ")");
}

void WIndex::bind_insert(Query& q, const Metadata& md, uint64_t ofs, char* timebuf)
{
    int idx = 0;

    q.bind(++idx, ofs);
    q.bind(++idx, md.data_size());
    q.bind(++idx, md.notes_encoded());

    if (const reftime::Position* reftime = md.get<reftime::Position>())
    {
        const auto& t = reftime->time;
        int len = snprintf(timebuf, 25, "%04d-%02d-%02d %02d:%02d:%02d", t.ye, t.mo, t.da, t.ho, t.mi, t.se);
        q.bind(++idx, timebuf, len);
    } else {
        q.bindNull(++idx);
    }

    if (m_uniques)
    {
        int id = m_uniques->obtain(md);
        q.bind(++idx, id);
    }
    if (m_others)
    {
        int id = m_others->obtain(md);
        q.bind(++idx, id);
    }
    if (config().smallfiles)
    {
        if (const types::Value* v = md.get<types::Value>())
        {
            q.bind(++idx, v->buffer);
        } else {
            q.bindNull(++idx);
        }
    }
}

void WIndex::index(const Metadata& md, uint64_t ofs)
{
    if (!m_insert.compiled())
        compile_insert();
    m_insert.reset();

    char buf[25];
    bind_insert(m_insert, md, ofs, buf);

    while (m_insert.step())
        ;
}

void WIndex::replace(Metadata& md, uint64_t ofs)
{
    if (!m_replace.compiled())
        compile_insert();
    m_replace.reset();

    char buf[25];
    bind_insert(m_replace, md, ofs, buf);

    while (m_replace.step())
        ;
}

void WIndex::remove(off_t ofs)
{
    Query remove("remove", m_db);
    remove.compile("DELETE FROM md WHERE offset=?");
    remove.bind(1, ofs);
    while (remove.step())
        ;
}

void WIndex::flush()
{
    // Not needed for index data consistency, but we need it to ensure file
    // timestamps are consistent at this point.
    m_db.checkpoint();
}

void WIndex::reset()
{
    m_db.exec("DELETE FROM md");
}

void WIndex::vacuum()
{
    //m_db.exec("PRAGMA journal_mode = TRUNCATE");
    if (m_uniques)
        m_db.exec("delete from mduniq where id not in (select uniq from md)");
    if (m_others)
        m_db.exec("delete from mdother where id not in (select other from md)");
    m_db.exec("VACUUM");
    m_db.exec("ANALYZE");
    //m_db.exec("PRAGMA journal_mode = PERSIST");
}

void WIndex::test_make_overlap(unsigned overlap_size, unsigned data_idx)
{
    // Get the minimum offset to move
    uint64_t offset = 0;
    {
        Query query("test_make_overlap_get_ofs", m_db);
        query.compile("SELECT offset FROM md ORDER BY offset LIMIT ?, 1");
        query.bind(1, data_idx);
        while (query.step())
            offset = query.fetch<uint64_t>(0);
    }

    // Move all offsets >= of the first one back by 1
    Query query("test_make_overlap", m_db);
    query.compile("UPDATE md SET offset=offset-? WHERE offset >= ?");
    query.run(overlap_size, offset);
}

void WIndex::test_make_hole(unsigned hole_size, unsigned data_idx)
{
    // Get the minimum offset to move
    uint64_t offset = 0;
    bool has_min_offset = false;
    {
        Query query("test_make_hole_get_ofs", m_db);
        query.compile("SELECT offset FROM md ORDER BY offset LIMIT ?, 1");
        query.bind(1, data_idx);
        while (query.step())
        {
            offset = query.fetch<uint64_t>(0);
            has_min_offset = true;
        }
    }

    if (!has_min_offset)
        return;

    Query query("test_make_hole", m_db);
    query.compile("UPDATE md SET offset = offset + ? WHERE offset = ?");

    Query sel("select_ids", m_db);
    sel.compile("SELECT offset FROM md WHERE offset >= ? ORDER BY offset DESC");
    sel.bind(1, offset);

    // Move all offsets >= of the first one back by 1
    sel.execute([&]{
        query.run(hole_size, sel.fetch<uint64_t>(0));
    });
}

AIndex::AIndex(std::shared_ptr<const iseg::Config> config, std::shared_ptr<segment::Writer> segment, std::shared_ptr<dataset::AppendLock> lock)
    : WIndex(config, segment->relname, lock), segment(segment)
{
}

CIndex::CIndex(std::shared_ptr<const iseg::Config> config, const std::string& data_relpath, std::shared_ptr<dataset::CheckLock> lock)
    : WIndex(config, data_relpath, lock)
{
}

}
}
}
