#include "index.h"
#include "arki/metadata.h"
#include "arki/metadata/collection.h"
#include "arki/matcher.h"
#include "arki/matcher/utils.h"
#include "arki/dataset/lock.h"
#include "arki/dataset/session.h"
#include "arki/dataset/query.h"
#include "arki/dataset/index/base.h"
#include "arki/types/reftime.h"
#include "arki/types/source.h"
#include "arki/types/source/blob.h"
#include "arki/types/value.h"
#include "arki/core/binary.h"
#include "arki/summary.h"
#include "arki/summary/stats.h"
#include "arki/nag.h"
#include "arki/utils/string.h"
#include "arki/utils/sys.h"

#include <sstream>
#include <cstdlib>

using namespace arki;
using namespace arki::types;
using namespace arki::utils;
using namespace arki::utils::sqlite;
using namespace arki::dataset::index;

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

Index::Index(std::shared_ptr<iseg::Dataset> dataset, const std::string& data_relpath, std::shared_ptr<dataset::Lock> lock)
    : dataset(dataset),
      data_relpath(data_relpath),
      data_pathname(str::joinpath(dataset->path, data_relpath)),
      index_pathname(data_pathname + ".index"),
      lock(lock)
{
    if (not dataset->unique.empty())
        m_uniques = new Aggregate(m_db, "mduniq", dataset->unique);

    // Instantiate m_others after the database has been opened, so we can scan
    // the database to see if some attributes are not available
}

Index::~Index()
{
    delete m_uniques;
    delete m_others;
}

core::Pending Index::begin_transaction()
{
    return core::Pending(new SqliteTransaction(m_db));
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
        std::string name = q.fetchString(0);
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

std::set<types::Code> Index::unique_codes() const
{
    std::set<types::Code> res;
    if (m_uniques) res = m_uniques->members();
    res.insert(TYPE_REFTIME);
    return res;
}

void Index::setup_pragmas()
{
    if (dataset->eatmydata)
    {
        m_db.exec("PRAGMA synchronous = OFF");
        m_db.exec("PRAGMA journal_mode = MEMORY");
    } else {
        // Use a WAL journal, which allows reads and writes together
        m_db.exec("PRAGMA journal_mode = WAL");
    }
    // Also, since the way we do inserts cause no trouble if a reader reads a
    // partial insert, we do not need read locking
    //m_db.exec("PRAGMA read_uncommitted = 1");
    // Use new features, if we write we read it, so we do not need to
    // support sqlite < 3.3.0 if we are above that version
    m_db.exec("PRAGMA legacy_file_format = 0");
}

void Index::scan(metadata_dest_func dest, const std::string& order_by) const
{
    std::string query = "SELECT m.offset, m.size, m.notes, m.reftime";
    if (m_uniques) query += ", m.uniq";
    if (m_others) query += ", m.other";
    if (dataset->smallfiles) query += ", m.data";
    query += " FROM md AS m";
    query += " ORDER BY " + order_by;

    Query mdq("scan_file_md", m_db);
    mdq.compile(query);

    auto reader = dataset->segment_reader(data_relpath, lock);

    while (mdq.step())
    {
        // Rebuild the Metadata
        std::unique_ptr<Metadata> md(new Metadata);
        build_md(mdq, *md, reader);
        dest(move(md));
    }
}

void Index::query_segment(metadata_dest_func dest) const
{
    scan(dest);
}

/// Begin extreme is included in the range, end extreme is excluded
static void db_time_extremes(utils::sqlite::SQLiteDB& db, core::Interval& interval)
{
    // SQLite can compute min and max of an indexed column very fast,
    // provided that it is the ONLY thing queried.
    Query q1("min_date", db);
    q1.compile("SELECT MIN(reftime) FROM md");
    while (q1.step())
        if (!q1.isNULL(0))
            interval.begin.set_sql(q1.fetchString(0));

    Query q2("min_date", db);
    q2.compile("SELECT MAX(reftime) FROM md");
    while (q2.step())
    {
        if (!q2.isNULL(0))
        {
            interval.end.set_sql(q2.fetchString(0));
            ++interval.end.se;
            interval.end.normalise();
        }
    }
}

void Index::add_joins_and_constraints(const Matcher& m, std::string& query) const
{
    std::vector<std::string> constraints;

    // Add database constraints
    if (not m.empty())
    {
        core::Interval interval;
        if (!m.intersect_interval(interval))
            // The matcher matches an impossible datetime span: convert it
            // into an impossible clause that evaluates quickly
            constraints.push_back("1 == 2");
        else if (!interval.begin.is_set() && !interval.end.is_set())
        {
            // No restriction on a range of reftimes, but still add sql
            // constraints if there is an unbounded reftime matcher (#116)
            if (auto reftime = m.get(TYPE_REFTIME))
            {
                std::string sql = reftime->toReftimeSQL("reftime");
                if (!sql.empty())
                    constraints.push_back(sql);
            }
        } else {
            // Compare with the reftime bounds in the database
            core::Interval db_interval;
            db_time_extremes(m_db, db_interval);
            if (db_interval.begin.is_set() && db_interval.end.is_set())
            {
                // Intersect the time bounds of the query with the time
                // bounds of the database
                interval.intersect(db_interval);
                long long int qrange = core::Time::duration(interval);
                long long int dbrange = core::Time::duration(db_interval);
                // If the query chooses less than 20%
                // if the time span, force the use of
                // the reftime index
                if (dbrange > 0 && qrange * 100 / dbrange < 20)
                {
                    query += " INDEXED BY md_idx_reftime";
                    constraints.push_back(
                        "reftime >= \'" + interval.begin.to_sql() + "\' AND reftime < \'" + interval.end.to_sql() + "\'");
                }
            }

            if (auto reftime = m.get(TYPE_REFTIME))
                constraints.push_back(reftime->toReftimeSQL("reftime"));
        }

        // Join with the aggregate tables and add constraints on aggregate tables
        if (m_uniques)
        {
            std::string s = m_uniques->make_subquery(m);
            if (!s.empty())
                constraints.push_back("uniq IN ("+s+")");
        }

        if (m_others)
        {
            std::string s = m_others->make_subquery(m);
            if (!s.empty())
                constraints.push_back("other IN ("+s+")");
        }
    }

    if (!constraints.empty())
        query += " WHERE " + str::join(" AND ", constraints.begin(), constraints.end());
}

void Index::build_md(Query& q, Metadata& md, std::shared_ptr<arki::segment::Reader> reader) const
{
    // Rebuild the Metadata
    md.set(Reftime::createPosition(core::Time::create_sql(q.fetchString(3))));
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
    if (dataset->smallfiles)
    {
        if (!q.isNULL(j))
        {
            std::string data = q.fetchString(j);
            md.set(types::Value::create(data));
        }
        ++j;
    }

    // md.notes = mdq.fetchItems<types::Note>(5);
    const uint8_t* notes_p = (const uint8_t*)q.fetchBlob(2);
    int notes_l = q.fetchBytes(2);
    md.set_notes_encoded(notes_p, notes_l);

    if (reader)
        md.set_source(Source::createBlob(
                dataset->format, dataset->path, data_relpath,
                q.fetch<uint64_t>(0), q.fetch<uint64_t>(1), reader));
    else
        md.set_source(Source::createBlobUnlocked(
                dataset->format, dataset->path, data_relpath,
                q.fetch<uint64_t>(0), q.fetch<uint64_t>(1)));
}

bool Index::query_data(const dataset::DataQuery& q, dataset::Session& session, metadata_dest_func dest)
{
    std::string query = "SELECT m.offset, m.size, m.notes, m.reftime";

    if (m_uniques) query += ", m.uniq";
    if (m_others) query += ", m.other";
    if (dataset->smallfiles) query += ", m.data";

    query += " FROM md AS m";

    try {
        add_joins_and_constraints(q.matcher, query);
    } catch (NotFound) {
        // If one of the subqueries did not find any match, we can directly
        // return true here, as we are not going to get any result
        return true;
    }

    query += " ORDER BY m.reftime";

    nag::debug("Running query %s", query.c_str());

    metadata::Collection mdbuf;
    std::shared_ptr<arki::segment::Reader> reader;
    if (q.with_data)
        reader = session.segment_reader(dataset->format, dataset->path, data_relpath, lock);

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
            std::unique_ptr<Metadata> md(new Metadata);
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

bool Index::query_summary_from_db(const Matcher& m, Summary& summary) const
{
    std::string query = "SELECT COUNT(1), SUM(size), MIN(reftime), MAX(reftime)";

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

    nag::debug("Running query %s", query.c_str());

    Query sq("sq", m_db);
    sq.compile(query);

    while (sq.step())
    {
        // Fill in the summary statistics
        summary::Stats st;
        st.count = sq.fetch<size_t>(0);
        st.size = sq.fetch<unsigned long long>(1);
        st.begin = core::Time::create_sql(sq.fetchString(2));
        st.end = core::Time::create_sql(sq.fetchString(3));

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


RIndex::RIndex(std::shared_ptr<iseg::Dataset> dataset, const std::string& data_relpath, std::shared_ptr<dataset::ReadLock> lock)
    : Index(dataset, data_relpath, lock)
{
    if (!sys::access(index_pathname, F_OK))
    {
        std::stringstream ss;
        ss << "dataset index " << index_pathname << " does not exist";
        throw std::runtime_error(ss.str());
    }

    m_db.open(index_pathname);
    if (dataset->trace_sql) m_db.trace();

    init_others();
}


WIndex::WIndex(std::shared_ptr<iseg::Dataset> dataset, const std::string& data_relpath, std::shared_ptr<dataset::Lock> lock)
    : Index(dataset, data_relpath, lock), m_get_current("get_current", m_db), m_insert(m_db), m_replace("replace", m_db)
{
    bool need_create = !sys::access(index_pathname, F_OK);

    if (need_create)
    {
        m_db.open(index_pathname);
        if (dataset->trace_sql) m_db.trace();
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
        if (dataset->trace_sql) m_db.trace();
        setup_pragmas();
        init_others();
    }
}

void WIndex::init_db()
{
    if (m_uniques) m_uniques->initDB(dataset->index);
    if (m_others) m_others->initDB(dataset->index);

    // Create the main table
    std::string query = "CREATE TABLE IF NOT EXISTS md ("
        " offset INTEGER PRIMARY KEY,"
        " size INTEGER NOT NULL,"
        " notes BLOB,"
        " reftime TEXT NOT NULL";
    if (m_uniques) query += ", uniq INTEGER NOT NULL";
    if (m_others) query += ", other INTEGER NOT NULL";
    if (dataset->smallfiles) query += ", data TEXT";
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
    std::string un_ot;
    std::string placeholders;
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
    if (dataset->smallfiles)
    {
        un_ot += ", data";
        placeholders += ", ?";
    }

    // Precompile get_current
    std::string cur_query = "SELECT offset, size FROM md WHERE reftime=?";
    if (m_uniques) cur_query += " AND uniq=?";
    m_get_current.compile(cur_query);

    // Precompile insert
    m_insert.compile("INSERT INTO md (offset, size, notes, reftime" + un_ot + ")"
             " VALUES (?, ?, ?, ?" + placeholders + ")");

    // Precompile replace
    m_replace.compile("INSERT OR REPLACE INTO md (offset, size, notes, reftime" + un_ot + ")"
                  " VALUES (?, ?, ?, ?" + placeholders + ")");
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
    std::vector<uint8_t> buf;

    Inserter(WIndex& idx, const Metadata& md)
        : idx(idx), md(md)
    {
        if (const reftime::Position* reftime = md.get<reftime::Position>())
        {
            auto t = reftime->get_Position();
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

    void bind_insert(Query& q, uint64_t ofs)
    {
        int qidx = 0;

        q.bind(++qidx, ofs);
        q.bind(++qidx, md.data_size());
        buf.clear();
        core::BinaryEncoder enc(buf);
        md.encode_notes(enc);
        q.bind(++qidx, buf);

        if (timebuf_len)
            q.bind(++qidx, timebuf, timebuf_len);
        else
            q.bindNull(++qidx);

        if (id_uniques != -1) q.bind(++qidx, id_uniques);
        if (id_others != -1) q.bind(++qidx, id_others);
        if (idx.dataset->smallfiles)
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

std::unique_ptr<types::source::Blob> WIndex::index(const Metadata& md, uint64_t ofs)
{
    std::unique_ptr<types::source::Blob> res;
    if (!m_insert.compiled()) compile_insert();
    Inserter inserter(*this, md);

    // Check if a conflicting metadata exists in the dataset
    m_get_current.reset();
    inserter.bind_get_current(m_get_current);
    while (m_get_current.step())
        res = types::source::Blob::create_unlocked(
                dataset->format, dataset->path, data_relpath,
                m_get_current.fetch<uint64_t>(0),
                m_get_current.fetch<uint64_t>(1));
    if (res) return res;

    m_insert.reset();
    inserter.bind_insert(m_insert, ofs);
    while (m_insert.step())
        ;

    return res;
}

void WIndex::replace(Metadata& md, uint64_t ofs)
{
    if (!m_replace.compiled())
        compile_insert();
    m_replace.reset();

    Inserter inserter(*this, md);
    inserter.bind_insert(m_replace, ofs);
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

AIndex::AIndex(std::shared_ptr<iseg::Dataset> config, std::shared_ptr<segment::Writer> segment, std::shared_ptr<dataset::AppendLock> lock)
    : WIndex(config, segment->segment().relpath, lock)
{
}

CIndex::CIndex(std::shared_ptr<iseg::Dataset> config, const std::string& data_relpath, std::shared_ptr<dataset::CheckLock> lock)
    : WIndex(config, data_relpath, lock)
{
}

}
}
}
