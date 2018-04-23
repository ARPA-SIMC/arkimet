#include "config.h"
#include "index.h"
#include "arki/dataset/maintenance.h"
#include "arki/configfile.h"
#include "arki/metadata.h"
#include "arki/metadata/collection.h"
#include "arki/matcher.h"
#include "arki/matcher/reftime.h"
#include "arki/dataset.h"
#include "arki/types/reftime.h"
#include "arki/types/source.h"
#include "arki/types/source/blob.h"
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
namespace index {

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


Contents::Contents(std::shared_ptr<const ondisk2::Config> config)
    : m_config(config),  m_get_current("getcurrent", m_db),
      m_uniques(0), m_others(0), scache(config->summary_cache_pathname)
{
    m_components_indexed = parseMetadataBitmask(config->index);

    // What metadata components we use to create a unique id
    std::set<types::Code> unique_members = parseMetadataBitmask(config->unique);
    unique_members.erase(TYPE_REFTIME);
    if (not unique_members.empty())
        m_uniques = new Aggregate(m_db, "mduniq", unique_members);

    // Instantiate m_others at initQueries time, so we can scan the
    // database to see if some attributes are not available
}

Contents::~Contents()
{
    if (m_uniques) delete m_uniques;
    if (m_others) delete m_others;
}

bool Contents::exists() const
{
    return sys::exists(pathname());
}

std::set<types::Code> Contents::available_other_tables() const
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

std::set<types::Code> Contents::all_other_tables() const
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

void Contents::initQueries()
{
    if (!m_others)
    {
        std::set<types::Code> other_members = available_other_tables();
        if (not other_members.empty())
            m_others = new Aggregate(m_db, "mdother", other_members);
    }

    string query = "SELECT format, file, offset, size FROM md WHERE reftime=?";
    if (m_uniques) query += " AND uniq=?";
    m_get_current.compile(query);
}

std::set<types::Code> Contents::unique_codes() const
{
    std::set<types::Code> res;
    if (m_uniques) res = m_uniques->members();
    res.insert(TYPE_REFTIME);
    return res;
}

void Contents::setupPragmas()
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

std::unique_ptr<types::source::Blob> Contents::get_current(const Metadata& md) const
{
    if (lock.expired())
        throw std::runtime_error("cannot get_current while there is no lock held");
    m_get_current.reset();
    const reftime::Position* rt = md.get<reftime::Position>();
    if (!rt) throw std::runtime_error("cannot get_current on a Metadata with no reftime information");

    std::unique_ptr<types::source::Blob> res;

    int idx = 0;
    string sqltime = rt->time.to_sql();
    m_get_current.bind(++idx, sqltime);

    int id_unique = -1;
    if (m_uniques)
    {
        id_unique = m_uniques->get(md);
        // If we do not have this aggregate, then we do not have this metadata
        if (id_unique == -1) return res;
        m_get_current.bind(++idx, id_unique);
    }

    while (m_get_current.step())
        res = types::source::Blob::create_unlocked(
                m_get_current.fetchString(0),
                config().path,
                m_get_current.fetchString(1),
                m_get_current.fetch<uint64_t>(2),
                m_get_current.fetch<uint64_t>(3));
    return res;
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

void Contents::list_segments(std::function<void(const std::string&)> dest)
{
    Query sq("list_segments", m_db);
    sq.compile("SELECT DISTINCT file FROM md ORDER BY file");
    while (sq.step())
        dest(sq.fetchString(0));
}

void Contents::list_segments_filtered(const Matcher& matcher, std::function<void(const std::string&)> dest)
{
    if (matcher.empty())
        return list_segments(dest);

    unique_ptr<Time> begin;
    unique_ptr<Time> end;
    if (!matcher.restrict_date_range(begin, end))
        // The matcher matches an impossible datetime span: convert it
        // into an impossible clause that evaluates quickly
        return;

    if (!begin.get() && !end.get())
        return list_segments(dest);

    if (begin.get() && end.get())
    {
        Query sq("list_segments", m_db);
        sq.compile("SELECT DISTINCT file, MIN(reftime) AS begin, MAX(reftime) AS end FROM md GROUP BY file HAVING begin <= ? AND end >= ? ORDER BY file");
        string b = begin->to_sql();
        string e = end->to_sql();
        sq.bind(1, e);
        sq.bind(2, b);
        while (sq.step())
            dest(sq.fetchString(0));
    }
    else if (begin.get())
    {
        Query sq("list_segments", m_db);
        sq.compile("SELECT DISTINCT file, MAX(reftime) AS end FROM md GROUP BY file HAVING end >= ? ORDER BY file");
        string b = begin->to_sql();
        sq.bind(1, b);
        while (sq.step())
            dest(sq.fetchString(0));
    }
    else
    {
        Query sq("list_segments", m_db);
        sq.compile("SELECT DISTINCT file, MIN(reftime) AS begin FROM md GROUP BY file HAVING begin <= ? ORDER BY file");
        string e = end->to_sql();
        sq.bind(1, e);
        while (sq.step())
            dest(sq.fetchString(0));
    }
}

bool Contents::has_segment(const std::string& relpath) const
{
    Query q("sel_has_segment", m_db);
    q.compile("SELECT 1 FROM md WHERE file=? LIMIT 1");
    q.bind(1, relpath);
    bool res = false;
    while (q.step())
        res = true;
    return res;
}

void Contents::scan_file(SegmentManager& segs, const std::string& relpath, metadata_dest_func dest, const std::string& order_by) const
{
    if (lock.expired())
        throw std::runtime_error("cannot scan_file while there is no lock held");
    string query = "SELECT m.id, m.format, m.file, m.offset, m.size, m.notes, m.reftime";
    if (m_uniques) query += ", m.uniq";
    if (m_others) query += ", m.other";
    if (config().smallfiles) query += ", m.data";
    query += " FROM md AS m";
    query += " WHERE m.file=? ORDER BY " + order_by;

    Query mdq("scan_file_md", m_db);
    mdq.compile(query);
    mdq.bind(1, relpath);

    auto reader = segs.get_reader(relpath, lock.lock());
    while (mdq.step())
    {
        // Rebuild the Metadata
        unique_ptr<Metadata> md(new Metadata);
        build_md(mdq, *md, reader);
        dest(move(md));
    }
}

void Contents::query_segment(const std::string& relpath, SegmentManager& segs, metadata_dest_func dest) const
{
    scan_file(segs, relpath, dest);
}

bool Contents::segment_timespan(const std::string& relpath, Time& start_time, Time& end_time) const
{
    Query sq("max_file_reftime", m_db);
    sq.compile("SELECT MIN(reftime), MAX(reftime) FROM md WHERE file=?");
    sq.bind(1, relpath);
    bool res = false;
    while (sq.step())
    {
        start_time.set_sql(sq.fetchString(0));
        end_time.set_sql(sq.fetchString(1));
        res = true;
    }
    return res;
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

bool Contents::addJoinsAndConstraints(const Matcher& m, std::string& query) const
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

    /*
    if (not m.empty() and constraints.size() != m.m_impl->size())
        // We can only see what we index, the rest is lost
        // TODO: add a filter to the query results, before the
        //       postprocessor that pulls in data from disk
        return false;
    */

    if (!constraints.empty())
        query += " WHERE " + str::join(" AND ", constraints.begin(), constraints.end());
    return true;
}

void Contents::build_md(Query& q, Metadata& md, std::shared_ptr<arki::segment::Reader> reader) const
{
    // Rebuild the Metadata
    md.set_source(Source::createBlob(
            q.fetchString(1), config().path, q.fetchString(2),
            q.fetch<uint64_t>(3), q.fetch<uint64_t>(4), reader));
    // md.notes = mdq.fetchItems<types::Note>(5);
    const uint8_t* notes_p = (const uint8_t*)q.fetchBlob(5);
    int notes_l = q.fetchBytes(5);
    md.set_notes_encoded(vector<uint8_t>(notes_p, notes_p + notes_l));
    md.set(Reftime::createPosition(Time::create_sql(q.fetchString(6))));
    int j = 7;
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

bool Contents::query_data(const dataset::DataQuery& q, SegmentManager& segs, metadata_dest_func dest)
{
    if (lock.expired())
        throw std::runtime_error("cannot query_data while there is no lock held");
    string query = "SELECT m.id, m.format, m.file, m.offset, m.size, m.notes, m.reftime";

    if (m_uniques) query += ", m.uniq";
    if (m_others) query += ", m.other";
    if (config().smallfiles) query += ", m.data";

    query += " FROM md AS m";

    try {
        if (!addJoinsAndConstraints(q.matcher, query))
            return true;
    } catch (NotFound) {
        // If one of the subqueries did not find any match, we can directly
        // return true here, as we are not going to get any result
        return true;
    }

    query += " ORDER BY m.reftime";

    nag::verbose("Running query %s", query.c_str());

    metadata::Collection mdbuf;
    string last_fname;
    std::shared_ptr<arki::segment::Reader> reader;

    // This keeps the index locked for a potentially long time, if dest is slow
    // in processing data. Use iseg datasets if this is a problem.
    Query mdq("mdq", m_db);
    mdq.compile(query);

    while (mdq.step())
    {
        // At file boundary, sort and write out what we have so
        // far, so we don't keep it all in memory
        string srcname = mdq.fetchString(2);
        if (srcname != last_fname)
        {
            if (q.with_data)
                reader = segs.get_reader(srcname, lock.lock());

            if (!mdbuf.empty())
            {
                if (q.sorter) mdbuf.sort(*q.sorter);
                if (!mdbuf.move_to(dest))
                    return false;
            }

            last_fname = srcname;
        }

        // Rebuild the Metadata
        unique_ptr<Metadata> md(new Metadata);
        build_md(mdq, *md, reader);
        // Buffer the results in memory, to release the database lock as soon as possible
        mdbuf.acquire(move(md));
    }

    if (!mdbuf.empty())
    {
        if (q.sorter) mdbuf.sort(*q.sorter);
        if (!mdbuf.move_to(dest))
            return false;
    }

    return true;
}

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

bool Contents::querySummaryFromDB(const Matcher& m, Summary& summary) const
{
    string query = "SELECT COUNT(1), SUM(size), MIN(reftime), MAX(reftime)";

    if (m_uniques) query += ", uniq";
    if (m_others) query += ", other";

    query += " FROM md";

    try {
        if (!addJoinsAndConstraints(m, query))
            return false;
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

static inline bool range_envelopes_full_month(const Time& begin, const Time& end)
{
    bool begins_at_beginning = begin.da == 1 && begin.ho == 0 && begin.mi == 0 && begin.se == 0;
    if (begins_at_beginning)
        return end >= begin.end_of_month();

    bool ends_at_end = end.da == Time::days_in_month(end.ye, end.mo) && end.ho == 23 && end.mi == 59 && end.se == 59;
    if (ends_at_end)
        return begin <= end.start_of_month();

    return end.ye == begin.ye + begin.mo / 12 && end.mo == (begin.mo % 12) + 1;
}

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

RIndex::RIndex(std::shared_ptr<const ondisk2::Config> config)
    : Contents(config) {}

RIndex::~RIndex()
{
}

void RIndex::open()
{
    if (m_db.isOpen())
    {
        stringstream ss;
        ss << "dataset index " << pathname() << " is already open";
        throw runtime_error(ss.str());
    }

    if (!sys::access(pathname(), F_OK))
    {
        stringstream ss;
        ss << "dataset index " << pathname() << " does not exist";
        throw runtime_error(ss.str());
    }

    m_db.open(pathname());

    initQueries();

    scache.openRO();
}

void RIndex::initQueries()
{
    Contents::initQueries();
}

void RIndex::test_rename(const std::string& relpath, const std::string& new_relpath)
{
    throw std::runtime_error("test_rename is only allowed on WIndex");
}

void RIndex::test_deindex(const std::string& relpath)
{
    throw std::runtime_error("test_deindex is only allowed on WIndex");
}

void RIndex::test_make_overlap(const std::string& relpath, unsigned overlap_size, unsigned data_idx)
{
    throw std::runtime_error("test_make_overlap is only allowed on WIndex");
}

void RIndex::test_make_hole(const std::string& relpath, unsigned hole_size, unsigned data_idx)
{
    throw std::runtime_error("test_make_hole is only allowed on WIndex");
}


WIndex::WIndex(std::shared_ptr<const ondisk2::Config> config)
    : Contents(config), m_insert(m_db), m_delete("delete", m_db), m_replace("replace", m_db)
{
}

WIndex::~WIndex()
{
}

bool WIndex::open()
{
    if (m_db.isOpen())
    {
        stringstream ss;
        ss << "dataset index " << pathname() << " is already open";
        throw runtime_error(ss.str());
    }

    bool need_create = !sys::access(pathname(), F_OK);

    m_db.open(pathname());

    if (need_create)
    {
        setupPragmas();
        if (!m_others)
        {
            std::set<types::Code> other_members = all_other_tables();
            if (not other_members.empty())
                m_others = new Aggregate(m_db, "mdother", other_members);
        }
        initDB();
    }

    initQueries();

    scache.openRW();

    return need_create;
}

void WIndex::initQueries()
{
    Contents::initQueries();

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
    m_insert.compile("INSERT INTO md (format, file, offset, size, notes, reftime" + un_ot + ")"
             " VALUES (?, ?, ?, ?, ?, ?" + placeholders + ")");

    // Precompile replace
    m_replace.compile("INSERT OR REPLACE INTO md (format, file, offset, size, notes, reftime" + un_ot + ")"
                  " VALUES (?, ?, ?, ?, ?, ?" + placeholders + ")");

    // Precompile remove query
    m_delete.compile("DELETE FROM md WHERE id=?");
}

void WIndex::initDB()
{
    if (m_uniques) m_uniques->initDB(m_components_indexed);
    if (m_others) m_others->initDB(m_components_indexed);

    // Create the main table
    string query = "CREATE TABLE IF NOT EXISTS md ("
        "id INTEGER PRIMARY KEY,"
        " format TEXT NOT NULL,"
        " file TEXT NOT NULL,"
        " offset INTEGER NOT NULL,"
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
    m_db.exec("CREATE INDEX IF NOT EXISTS md_idx_file ON md (file)");
    if (m_uniques) m_db.exec("CREATE INDEX IF NOT EXISTS md_idx_uniq ON md (uniq)");
    if (m_others) m_db.exec("CREATE INDEX IF NOT EXISTS md_idx_other ON md (other)");
}

Pending WIndex::begin_transaction()
{
    return Pending(new SqliteTransaction(m_db));
}

namespace {

struct Inserter
{
    WIndex& idx;
    const Metadata& md;
    char timebuf[25];
    int timebuf_len;
    int id_uniques = -1;
    int id_others = -1;

    Inserter(WIndex& idx, const Metadata& md)
        : idx(idx), md(md)
    {
        if (const reftime::Position* reftime = md.get<reftime::Position>())
        {
            const auto& t = reftime->time;
            timebuf_len = snprintf(timebuf, 25, "%04d-%02d-%02d %02d:%02d:%02d", t.ye, t.mo, t.da, t.ho, t.mi, t.se);
        } else {
            timebuf[0] = 0;
            timebuf_len = 0;
        }

        if (idx.has_uniques()) id_uniques = idx.uniques().obtain(md);
        if (idx.has_others()) id_others = idx.others().obtain(md);
    }

    void bind_get_current(Query& q)
    {
        if (timebuf_len)
            q.bind(1, timebuf, timebuf_len);
        else
            q.bindNull(1);

        if (id_uniques != -1) q.bind(2, id_uniques);
    }

    void bind_insert(Query& q, const std::string& file, uint64_t ofs)
    {
        int qidx = 0;

        q.bind(++qidx, md.source().format);
        q.bind(++qidx, file);
        q.bind(++qidx, ofs);
        q.bind(++qidx, md.data_size());
        q.bind(++qidx, md.notes_encoded());

        if (timebuf_len)
            q.bind(++qidx, timebuf, timebuf_len);
        else
            q.bindNull(++qidx);

        if (id_uniques != -1) q.bind(++qidx, id_uniques);
        if (id_others != -1) q.bind(++qidx, id_others);
        if (idx.config().smallfiles)
        {
            if (const types::Value* v = md.get<types::Value>())
            {
                q.bind(++qidx, v->buffer);
            } else {
                q.bindNull(++qidx);
            }
        }
    }
};

}

std::unique_ptr<types::source::Blob> WIndex::index(const Metadata& md, const std::string& relpath, uint64_t ofs)
{
    std::unique_ptr<types::source::Blob> res;
    Inserter inserter(*this, md);

    // Check if a conflicting metadata exists in the dataset
    m_get_current.reset();
    inserter.bind_get_current(m_get_current);
    while (m_get_current.step())
        res = types::source::Blob::create_unlocked(
                m_get_current.fetchString(0),
                config().path,
                m_get_current.fetchString(1),
                m_get_current.fetch<uint64_t>(2),
                m_get_current.fetch<uint64_t>(3));
    if (res) return res;

    m_insert.reset();
    inserter.bind_insert(m_insert, relpath, ofs);
    while (m_insert.step())
        ;

    // Invalidate the summary cache for this month
    scache.invalidate(md);
    return res;
}

void WIndex::replace(Metadata& md, const std::string& relpath, uint64_t ofs)
{
    m_replace.reset();

    Inserter inserter(*this, md);
    inserter.bind_insert(m_replace, relpath, ofs);
    while (m_replace.step())
        ;

    // Invalidate the summary cache for this month
    scache.invalidate(md);
}

void WIndex::remove(const std::string& relpath, off_t ofs)
{
    Query fetch_id("byfile", m_db);
    fetch_id.compile("SELECT id, reftime FROM md WHERE file=? AND offset=?");
    fetch_id.bind(1, relpath);
    fetch_id.bind(2, ofs);
    int id = 0;
    string reftime;
    while (fetch_id.step())
    {
        id = fetch_id.fetch<int>(0);
        reftime = fetch_id.fetchString(1);
    }
    if (reftime.empty())
    {
        stringstream ss;
        ss << "cannot remove item " << relpath << ":" << ofs << " because it does not exist in the index";
        throw runtime_error(ss.str());
    }

    // Invalidate the summary cache for this month
    Time rt(Time::create_sql(reftime));
    scache.invalidate(rt.ye, rt.mo);

    // DELETE FROM md WHERE id=?
    m_delete.reset();
    m_delete.bind(1, id);
    while (m_delete.step())
        ;
}

void WIndex::reset()
{
    m_db.exec("DELETE FROM md");
    scache.invalidate();
}

void WIndex::reset(const std::string& file)
{
    // Get the file date extremes to invalidate the summary cache
    string fmin, fmax;
    {
        Query sq("file_reftime_extremes", m_db);
        sq.compile("SELECT MIN(reftime), MAX(reftime) FROM md WHERE file=?");
        sq.bind(1, file);
        while (sq.step())
        {
            fmin = sq.fetchString(0);
            fmax = sq.fetchString(1);
        }
        // If it did not find the file in the database, we do not need
        // to remove it
        if (fmin.empty())
            return;
    }
    Time tmin(Time::create_sql(fmin).start_of_month());
    Time tmax(Time::create_sql(fmax).start_of_month());

    // Clean the database
    Query query("reset_datafile", m_db);
    query.compile("DELETE FROM md WHERE file = ?");
    query.bind(1, file);
    query.step();

    // Clean affected summary cache
    scache.invalidate(tmin, tmax);
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

void WIndex::flush()
{
    // Not needed for index data consistency, but we need it to ensure file
    // timestamps are consistent at this point.
    m_db.checkpoint();
}

void WIndex::test_rename(const std::string& relpath, const std::string& new_relpath)
{
    Query query("test_rename", m_db);
    query.compile("UPDATE md SET file=? WHERE file=?");
    query.bind(1, new_relpath);
    query.bind(2, relpath);
    while (query.step())
        ;
}

void WIndex::test_deindex(const std::string& relpath)
{
    reset(relpath);
}

void WIndex::test_make_overlap(const std::string& relpath, unsigned overlap_size, unsigned data_idx)
{
    // Get the minimum offset to move
    uint64_t offset = 0;
    {
        Query query("test_make_overlap_get_ofs", m_db);
        query.compile("SELECT offset FROM md WHERE file=? ORDER BY offset LIMIT ?, 1");
        query.bind(1, relpath);
        query.bind(2, data_idx);
        while (query.step())
            offset = query.fetch<uint64_t>(0);
    }

    // Move all offsets >= of the first one back by 1
    Query query("test_make_overlap", m_db);
    query.compile("UPDATE md SET offset=offset-? WHERE file=? AND offset >= ?");
    query.run(overlap_size, relpath, offset);
}

void WIndex::test_make_hole(const std::string& relpath, unsigned hole_size, unsigned data_idx)
{
    // Get the minimum offset to move
    uint64_t offset = 0;
    bool has_min_offset = false;
    {
        Query query("test_make_hole_get_ofs", m_db);
        query.compile("SELECT offset FROM md WHERE file=? ORDER BY offset LIMIT ?, 1");
        query.bind(1, relpath);
        query.bind(2, data_idx);
        while (query.step())
        {
            offset = query.fetch<uint64_t>(0);
            has_min_offset = true;
        }
    }

    if (!has_min_offset)
        return;

    // Move all offsets >= of the first one back by 1
    Query query("test_make_hole", m_db);
    query.compile("UPDATE md SET offset=offset+? WHERE file=? AND offset >= ?");
    query.bind(1, hole_size);
    query.bind(2, relpath);
    query.bind(3, offset);
    while (query.step())
        ;
}

}
}
}
