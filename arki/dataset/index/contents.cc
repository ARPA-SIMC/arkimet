#include "config.h"
#include "contents.h"
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
    : m_config(config),  m_get_id("getid", m_db), m_get_current("getcurrent", m_db),
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

void Contents::list_segments(std::function<void(const std::string&)> dest)
{
    Query sq("list_segments", m_db);
    sq.compile("SELECT DISTINCT file FROM md ORDER BY file");
    while (sq.step())
        dest(sq.fetchString(0));
}

void Contents::scan_files(segment::contents_func v)
{
    string query = "SELECT m.id, m.format, m.file, m.offset, m.size, m.notes, m.reftime";
    if (m_uniques) query += ", m.uniq";
    if (m_others) query += ", m.other";
    if (config().smallfiles) query += ", m.data";
    query += " FROM md AS m";
    query += " ORDER BY m.file, m.reftime, m.offset";

    Query mdq("scan_files_md", m_db);
    mdq.compile(query);

    string last_file;
    metadata::Collection mdc;
    while (mdq.step())
    {
        string file = mdq.fetchString(2);
        if (file != last_file)
        {
            if (!last_file.empty())
            {
                v(last_file, SEGMENT_OK, mdc);
                mdc.clear();
            }
            last_file = file;
        }

        // Rebuild the Metadata
        unique_ptr<Metadata> md(new Metadata);
        build_md(mdq, *md);
        mdc.acquire(move(md));
    }

    if (!last_file.empty())
        v(last_file, SEGMENT_OK, mdc);
}

void Contents::scan_file(const std::string& relname, metadata_dest_func dest, const std::string& orderBy) const
{
    string query = "SELECT m.id, m.format, m.file, m.offset, m.size, m.notes, m.reftime";
    if (m_uniques) query += ", m.uniq";
    if (m_others) query += ", m.other";
    if (config().smallfiles) query += ", m.data";
    query += " FROM md AS m";
    query += " WHERE m.file=? ORDER BY " + orderBy;

    Query mdq("scan_file_md", m_db);
    mdq.compile(query);
    mdq.bind(1, relname);

    while (mdq.step())
    {
        // Rebuild the Metadata
        unique_ptr<Metadata> md(new Metadata);
        build_md(mdq, *md);
        dest(move(md));
    }
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
            ; // The matcher does not match on time ranges: do not add
              // constraints
        else
        {
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

void Contents::build_md(Query& q, Metadata& md) const
{
    // Rebuild the Metadata
    md.set_source(Source::createBlob(
            q.fetchString(1), config().path, q.fetchString(2),
            q.fetch<uint64_t>(3), q.fetch<uint64_t>(4)));
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

bool Contents::query_data(const dataset::DataQuery& q, metadata_dest_func dest)
{
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
    unique_ptr<runtime::Tempfile> tmpfile;

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
            // At file boundary, sort and write out what we have so
            // far, so we don't keep it all in memory
            string srcname = mdq.fetchString(2);
            if (srcname != last_fname)
            {
                // Note: since we chunk at file boundary, and since files
                // cluster data by reftime, we guarantee that sorting is
                // consistent as long as data is required to be sorted by
                // reftime first.
                if (mdbuf.size() > 8192)
                {
                    // If we pile up too many metadata, write them out
                    if (q.sorter) mdbuf.sort(*q.sorter);
                    if (tmpfile.get() == 0)
                        tmpfile.reset(new runtime::Tempfile);
                    mdbuf.write_to(*tmpfile, tmpfile->name());
                    mdbuf.clear();
                }
                last_fname = srcname;
            }

            // Rebuild the Metadata
            unique_ptr<Metadata> md(new Metadata);
            build_md(mdq, *md);
            // Buffer the results in memory, to release the database lock as soon as possible
            mdbuf.acquire(move(md));
        }
//fprintf(stderr, "POST %zd\n", mdbuf.size());
//system(str::fmtf("ps u %d >&2", getpid()).c_str());
    }
//if (tmpfile.get()) system(str::fmtf("ls -la --si %s >&2", tmpfile->name().c_str()).c_str());

    // If we had buffered some sorted metadata to a temporary file, replay them
    // now
    if (tmpfile.get() != 0)
    {
        metadata::ReadContext rc(tmpfile->name(), config().path);
        if (!Metadata::read_file(rc, [&](std::unique_ptr<Metadata> md) { md->sourceBlob().lock(); return dest(move(md)); }))
            return false;
    }

    // Sort and output the rest
    if (q.sorter) mdbuf.sort(*q.sorter);

    // pass it to consumer
    return mdbuf.move_to(dest);
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

RContents::RContents(std::shared_ptr<const ondisk2::Config> config)
    : Contents(config) {}

RContents::~RContents()
{
}

void RContents::open()
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
    setupPragmas();

    initQueries();

    scache.openRO();
}

void RContents::initQueries()
{
    Contents::initQueries();
}

void RContents::test_rename(const std::string& relname, const std::string& new_relname)
{
    throw std::runtime_error("renaming segments is only allowed on WIndex");
}

WContents::WContents(std::shared_ptr<const ondisk2::Config> config)
    : Contents(config), m_insert(m_db), m_delete("delete", m_db), m_replace("replace", m_db)
{
}

WContents::~WContents()
{
}

bool WContents::open()
{
    if (m_db.isOpen())
    {
        stringstream ss;
        ss << "dataset index " << pathname() << " is already open";
        throw runtime_error(ss.str());
    }

    bool need_create = !sys::access(pathname(), F_OK);

    m_db.open(pathname());
    setupPragmas();

    if (need_create)
    {
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

void WContents::initQueries()
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

void WContents::initDB()
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

void WContents::bindInsertParams(Query& q, const Metadata& md, const std::string& file, uint64_t ofs, char* timebuf)
{
    int idx = 0;

    q.bind(++idx, md.source().format);
    q.bind(++idx, file);
    q.bind(++idx, ofs);
    q.bind(++idx, md.data_size());
    //q.bindItems(++idx, md.notes);
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

Pending WContents::beginTransaction()
{
    return Pending(new SqliteTransaction(m_db));
}

Pending WContents::beginExclusiveTransaction()
{
    return Pending(new SqliteTransaction(m_db, "EXCLUSIVE"));
}

void WContents::index(const Metadata& md, const std::string& file, uint64_t ofs, int* id)
{
    m_insert.reset();

    char buf[25];
    bindInsertParams(m_insert, md, file, ofs, buf);

    while (m_insert.step())
        ;

    if (id)
        *id = m_db.lastInsertID();

    // Invalidate the summary cache for this month
    scache.invalidate(md);
}

void WContents::replace(Metadata& md, const std::string& file, uint64_t ofs, int* id)
{
    m_replace.reset();

    char buf[25];
    bindInsertParams(m_replace, md, file, ofs, buf);

    while (m_replace.step())
        ;

    if (id)
        *id = m_db.lastInsertID();

    // Invalidate the summary cache for this month
    scache.invalidate(md);
}

void WContents::remove(const std::string& relname, off_t ofs)
{
    Query fetch_id("byfile", m_db);
    fetch_id.compile("SELECT id, reftime FROM md WHERE file=? AND offset=?");
    fetch_id.bind(1, relname);
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
        ss << "cannot remove item " << relname << ":" << ofs << " because it does not exist in the index";
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

void WContents::reset()
{
    m_db.exec("DELETE FROM md");
    scache.invalidate();
}

void WContents::reset(const std::string& file)
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

void WContents::vacuum()
{
    m_db.exec("PRAGMA journal_mode = TRUNCATE");
    if (m_uniques)
        m_db.exec("delete from mduniq where id not in (select uniq from md)");
    if (m_others)
        m_db.exec("delete from mdother where id not in (select other from md)");
    m_db.exec("VACUUM");
    m_db.exec("ANALYZE");
    m_db.exec("PRAGMA journal_mode = PERSIST");
}

void WContents::flush()
{
    // Not needed for index data consistency, but we need it to ensure file
    // timestamps are consistent at this point.
    m_db.checkpoint();
}

void WContents::test_rename(const std::string& relname, const std::string& new_relname)
{
    Query query("test_rename", m_db);
    query.compile("UPDATE md SET file=? WHERE fille=?");
    query.bind(1, new_relname);
    query.bind(2, relname);
    while (query.step())
        ;
}

}
}
}
