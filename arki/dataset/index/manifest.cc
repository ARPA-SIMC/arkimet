#include "manifest.h"
#include "arki/libconfig.h"
#include "arki/exceptions.h"
#include "arki/core/file.h"
#include "arki/dataset.h"
#include "arki/dataset/maintenance.h"
#include "arki/metadata.h"
#include "arki/metadata/collection.h"
#include "arki/metadata/consumer.h"
#include "arki/types/source/blob.h"
#include "arki/summary.h"
#include "arki/types/reftime.h"
#include "arki/matcher.h"
#include "arki/utils/sqlite.h"
#include "arki/utils/files.h"
#include "arki/sort.h"
#include "arki/nag.h"
#include "arki/iotrace.h"
#include "arki/utils/sys.h"
#include "arki/utils/string.h"
#include "arki/scan.h"
#include <algorithm>
#include <unistd.h>
#include <fcntl.h>
#include <ctime>

using namespace std;
using namespace arki;
using namespace arki::core;
using namespace arki::types;
using namespace arki::utils;
using namespace arki::utils::sqlite;
using namespace arki::dataset::maintenance;
using arki::core::Time;

namespace arki {
namespace dataset {
namespace index {

Manifest::Manifest(const std::string& path, const core::lock::Policy* lock_policy) : m_path(path), lock_policy(lock_policy) {}
Manifest::~Manifest() {}

void Manifest::querySummaries(const Matcher& matcher, Summary& summary)
{
    vector<string> files = file_list(matcher);

    for (vector<string>::const_iterator i = files.begin(); i != files.end(); ++i)
    {
        string pathname = str::joinpath(m_path, *i);

        // Silently skip files that have been deleted
        if (!sys::access(pathname + ".summary", R_OK))
            continue;

        Summary s;
        s.readFile(pathname + ".summary");
        s.filter(matcher, summary);
    }
}

bool Manifest::query_data(const dataset::DataQuery& q, SegmentManager& segs, metadata_dest_func dest)
{
    if (lock.expired())
        throw std::runtime_error("cannot query_data while there is no lock held");
    vector<string> files = file_list(q.matcher);

    // TODO: does it make sense to check with the summary first?

    shared_ptr<sort::Compare> compare;
    if (q.sorter)
        compare = q.sorter;
    else
        // If no sorter is provided, sort by reftime in case segment files have
        // not been sorted before archiving
        compare = sort::Compare::parse("reftime");

    sort::Stream sorter(*compare, dest);

    string absdir = sys::abspath(m_path);
    string prepend_fname;

    for (vector<string>::const_iterator i = files.begin(); i != files.end(); ++i)
    {
        prepend_fname = str::dirname(*i);
        string abspath = str::joinpath(absdir, *i);
        string fullpath = abspath + ".metadata";
        if (!sys::exists(fullpath)) continue;
        std::shared_ptr<arki::segment::Reader> reader;
        if (q.with_data)
            reader = segs.get_reader(scan::Scanner::format_from_filename(*i), *i, lock.lock());
        // This generates filenames relative to the metadata
        // We need to use absdir as the dirname, and prepend dirname(*i) to the filenames
        Metadata::read_file(fullpath, [&](unique_ptr<Metadata> md) {
            // Filter using the matcher in the query
            if (!q.matcher(*md)) return true;

            // Tweak Blob sources replacing basedir and prepending a directory to the file name
            if (const source::Blob* s = md->has_source_blob())
            {
                if (q.with_data)
                    md->set_source(Source::createBlob(s->format, absdir, str::joinpath(prepend_fname, s->filename), s->offset, s->size, reader));
                else
                    md->set_source(Source::createBlobUnlocked(s->format, absdir, str::joinpath(prepend_fname, s->filename), s->offset, s->size));
            }
            return sorter.add(move(md));
        });
        if (!sorter.flush())
            return false;
    }

    return true;
}

bool Manifest::query_summary(const Matcher& matcher, Summary& summary)
{
    // Check if the matcher discriminates on reference times
    auto rtmatch = matcher.get(TYPE_REFTIME);

    if (!rtmatch)
    {
        // The matcher does not contain reftime, we can work with a
        // global summary
        string cache_pathname = str::joinpath(m_path, "summary");

        if (sys::access(cache_pathname, R_OK))
        {
            Summary s;
            s.readFile(cache_pathname);
            s.filter(matcher, summary);
        } else if (sys::access(m_path, W_OK)) {
            // Rebuild the cache
            Summary s;
            querySummaries(Matcher(), s);

            // Save the summary
            s.writeAtomically(cache_pathname);

            // Query the newly generated summary that we still have
            // in memory
            s.filter(matcher, summary);
        } else
            querySummaries(matcher, summary);
    } else {
        querySummaries(matcher, summary);
    }

    return true;
}

void Manifest::query_segment(const std::string& relpath, SegmentManager& segs, metadata_dest_func dest) const
{
    if (lock.expired())
        throw std::runtime_error("cannot query_segment while there is no lock held");
    string absdir = sys::abspath(m_path);
    string prepend_fname = str::dirname(relpath);
    string abspath = str::joinpath(m_path, relpath);
    auto reader = segs.get_reader(scan::Scanner::format_from_filename(relpath), relpath, lock.lock());
    Metadata::read_file(abspath + ".metadata", [&](unique_ptr<Metadata> md) {
        // Tweak Blob sources replacing the file name with relpath
        if (const source::Blob* s = md->has_source_blob())
            md->set_source(Source::createBlob(s->format, absdir, str::joinpath(prepend_fname, s->filename), s->offset, s->size, reader));
        return dest(move(md));
    });
}

void Manifest::invalidate_summary()
{
    sys::unlink_ifexists(str::joinpath(m_path, "summary"));
}

void Manifest::invalidate_summary(const std::string& relpath)
{
    sys::unlink_ifexists(str::joinpath(m_path, relpath) + ".summary");
    invalidate_summary();
}

void Manifest::test_deindex(const std::string& relpath)
{
    remove(relpath);
}

void Manifest::test_make_overlap(const std::string& relpath, unsigned overlap_size, unsigned data_idx)
{
    string pathname = str::joinpath(m_path, relpath) + ".metadata";
    utils::files::PreserveFileTimes pf(pathname);
    sys::File fd(pathname, O_RDWR);
    metadata::Collection mds;
    mds.read_from_file(fd);
    for (unsigned i = data_idx; i < mds.size(); ++i)
        mds[i].sourceBlob().offset -= overlap_size;
    fd.lseek(0);
    mds.write_to(fd);
    fd.ftruncate(fd.lseek(0, SEEK_CUR));
}

void Manifest::test_make_hole(const std::string& relpath, unsigned hole_size, unsigned data_idx)
{
    string pathname = str::joinpath(m_path, relpath) + ".metadata";
    utils::files::PreserveFileTimes pf(pathname);
    sys::File fd(pathname, O_RDWR);
    metadata::Collection mds;
    mds.read_from_file(fd);
    for (unsigned i = data_idx; i < mds.size(); ++i)
        mds[i].sourceBlob().offset += hole_size;
    fd.lseek(0);
    mds.write_to(fd);
    fd.ftruncate(fd.lseek(0, SEEK_CUR));
}


namespace manifest {
static bool mft_force_sqlite = false;

class PlainManifest : public Manifest
{
    struct Info
    {
        std::string file;
        time_t mtime;
        core::Time start_time;
        core::Time end_time;

        Info(const std::string& file, time_t mtime, const Time& start, const Time& end)
            : file(file), mtime(mtime), start_time(start), end_time(end)
        {
        }

        bool operator<(const Info& i) const
        {
            return file < i.file;
        }

        bool operator==(const Info& i) const
        {
            return file == i.file;
        }

        bool operator!=(const Info& i) const
        {
            return file != i.file;
        }

        void write(NamedFileDescriptor& out) const
        {
            stringstream ss;
            ss << file << ";" << mtime << ";" << start_time.to_sql() << ";" << end_time.to_sql() << endl;
            out.write_all_or_throw(ss.str());
        }
    };
    vector<Info> info;
    ino_t last_inode;
    bool dirty;
    bool rw;

    /**
     * Reread the MANIFEST file.
     *
     * @returns true if the MANIFEST file existed, false if not
     */
    bool reread()
    {
        string pathname(str::joinpath(m_path, "MANIFEST"));
        ino_t inode = sys::inode(pathname, 0);

        if (inode == last_inode) return inode != 0;

        info.clear();
        last_inode = inode;
        if (last_inode == 0)
            return false;

        File infd(pathname, O_RDONLY);
        iotrace::trace_file(pathname, 0, 0, "read MANIFEST");

        auto reader = LineReader::from_fd(infd);
        string line;
        for (size_t lineno = 1; reader->getline(line); ++lineno)
        {
            // Skip empty lines
            if (line.empty()) continue;

            size_t beg = 0;
            size_t end = line.find(';');
            if (end == string::npos)
            {
                stringstream ss;
                ss << "cannot parse " << pathname << ":" << lineno << ": line has only 1 field";
                throw runtime_error(ss.str());
            }

            string file = line.substr(beg, end-beg);

            beg = end + 1;
            end = line.find(';', beg);
            if (end == string::npos)
            {
                stringstream ss;
                ss << "cannot parse " << pathname << ":" << lineno << ": line has only 2 fields";
                throw runtime_error(ss.str());
            }

            time_t mtime = strtoul(line.substr(beg, end-beg).c_str(), 0, 10);

            beg = end + 1;
            end = line.find(';', beg);
            if (end == string::npos)
            {
                stringstream ss;
                ss << "cannot parse " << pathname << ":" << lineno << ": line has only 3 fields";
                throw runtime_error(ss.str());
            }

            info.push_back(Info(
                        file, mtime,
                        Time::create_sql(line.substr(beg, end-beg)),
                        Time::create_sql(line.substr(end+1))));
        }

        infd.close();
        dirty = false;
        return true;
    }

public:
    PlainManifest(const std::string& dir, const core::lock::Policy* lock_policy)
        : Manifest(dir, lock_policy), last_inode(0), dirty(false), rw(false)
    {
    }

    virtual ~PlainManifest()
    {
        flush();
    }

    void openRO()
    {
        if (!reread())
            throw std::runtime_error("cannot open archive index: MANIFEST does not exist in " + m_path);
        rw = false;
    }

    void openRW()
    {
        if (!reread())
            dirty = true;
        rw = true;
    }

    std::vector<std::string> file_list(const Matcher& matcher) override
    {
        std::vector<std::string> files;
        reread();

        string query;
        unique_ptr<Time> begin;
        unique_ptr<Time> end;
        if (!matcher.restrict_date_range(begin, end))
            return files;

        std::vector<const Info*> res;
        if (!begin.get() && !end.get())
        {
            // No restrictions on reftime: get all files
            for (const auto& i: info)
                res.push_back(&i);
        } else {
            // Get files with matching reftime
            for (const auto& i: info)
            {
                if (begin.get() && i.end_time < *begin) continue;
                if (end.get() && i.start_time > *end) continue;
                res.push_back(&i);
            }
        }

        // Re-sort to maintain the invariant that results are sorted by
        // start_time even if we have segments with names outside the normal
        // dataset step that do not sort properly
        std::sort(res.begin(), res.end(), [](const Info* a, const Info* b) { return a->start_time < b->start_time; });

        for (const auto* i: res)
            files.push_back(i->file);

        return files;
    }

    bool segment_timespan(const std::string& relpath, Time& start_time, Time& end_time) const override
    {
        // Lookup the file (FIXME: reimplement binary search so we
        // don't need to create a temporary Info)
        Info sample(relpath, 0, Time(0, 0, 0), Time(0, 0, 0));
        vector<Info>::const_iterator lb = lower_bound(info.begin(), info.end(), sample);
        if (lb != info.end() && lb->file == relpath)
        {
            start_time = lb->start_time;
            end_time = lb->end_time;
            return true;
        } else {
            return false;
        }
    }

    void expand_date_range(unique_ptr<Time>& begin, unique_ptr<Time>& end) const override
    {
        for (vector<Info>::const_iterator i = info.begin(); i != info.end(); ++i)
        {
            if (!begin.get() || i->start_time < *begin)
                begin.reset(new Time(i->start_time));
            if (!end.get() || i->end_time > *end)
                end.reset(new Time(i->end_time));
        }
    }

    size_t vacuum()
    {
        return 0;
    }

    Pending test_writelock()
    {
        return Pending();
    }

    void acquire(const std::string& relpath, time_t mtime, const Summary& sum)
    {
        reread();

        // Add to index
        unique_ptr<Reftime> rt = sum.getReferenceTime();

        Info item(relpath, mtime, rt->period_begin(), rt->period_end());

        // Insertion sort; at the end, everything is already sorted and we
        // avoid inserting lots of duplicate items
        vector<Info>::iterator lb = lower_bound(info.begin(), info.end(), item);
        if (lb == info.end())
            info.push_back(item);
        else if (*lb != item)
            info.insert(lb, item);
        else
            *lb = item;

        dirty = true;
    }

    void remove(const std::string& relpath) override
    {
        reread();
        vector<Info>::iterator i;
        for (i = info.begin(); i != info.end(); ++i)
            if (i->file == relpath)
                break;
        if (i != info.end())
            info.erase(i);
        dirty = true;
    }

    void list_segments(std::function<void(const std::string&)> dest) override
    {
        reread();
        for (const auto& i: this->info)
            dest(i.file);
    }

    void list_segments_filtered(const Matcher& matcher, std::function<void(const std::string&)> dest) override
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

        reread();
        if (begin.get() && end.get())
        {
            for (const auto& i: this->info)
                if (i.start_time <= *end && i.end_time >= *begin)
                    dest(i.file);
        }
        else if (begin.get())
        {
            for (const auto& i: this->info)
                if (i.end_time >= *begin)
                    dest(i.file);
        }
        else
        {
            for (const auto& i: this->info)
                if (i.start_time <= *end)
                    dest(i.file);
        }
    }

    bool has_segment(const std::string& relpath) const override
    {
        // Lookup the file (FIXME: reimplement binary search so we
        // don't need to create a temporary Info)
        Info sample(relpath, 0, Time(0, 0, 0), Time(0, 0, 0));
        vector<Info>::const_iterator lb = lower_bound(info.begin(), info.end(), sample);
        return lb != info.end() && lb->file == relpath;
    }

    time_t segment_mtime(const std::string& relpath) const override
    {
        // Lookup the file (FIXME: reimplement binary search so we
        // don't need to create a temporary Info)
        Info sample(relpath, 0, Time(0, 0, 0), Time(0, 0, 0));
        vector<Info>::const_iterator lb = lower_bound(info.begin(), info.end(), sample);
        if (lb != info.end() && lb->file == relpath)
            return lb->mtime;
        else
            return 0;
    }

    void flush()
    {
        if (dirty)
        {
            string pathname(str::joinpath(m_path, "MANIFEST.tmp"));

            File out(pathname, O_WRONLY | O_CREAT | O_TRUNC);
            for (vector<Info>::const_iterator i = info.begin();
                    i != info.end(); ++i)
                i->write(out);
            out.fdatasync();
            out.close();

            if (::rename(pathname.c_str(), str::joinpath(m_path, "MANIFEST").c_str()) < 0)
                throw_system_error("cannot rename " + pathname + " to " + str::joinpath(m_path, "MANIFEST"));

            invalidate_summary();
            dirty = false;
        }

        if (rw && ! sys::exists(str::joinpath(m_path, "summary")))
        {
            Summary s;
            query_summary(Matcher(), s);
        }
    }

    void test_rename(const std::string& relpath, const std::string& new_relpath) override
    {
        for (auto& i: info)
            if (i.file == relpath)
            {
                i.file = new_relpath;
                dirty = true;
            }
        std::sort(info.begin(), info.end());
    }

    static bool exists(const std::string& dir)
    {
        string pathname(str::joinpath(dir, "MANIFEST"));
        return sys::access(pathname, F_OK);
    }
};


class SqliteManifest : public Manifest
{
    mutable utils::sqlite::SQLiteDB m_db;
    utils::sqlite::InsertQuery m_insert;

    void setupPragmas()
    {
        // Also, since the way we do inserts cause no trouble if a reader reads a
        // partial insert, we do not need read locking
        //m_db.exec("PRAGMA read_uncommitted = 1");
        // Use new features, if we write we read it, so we do not need to
        // support sqlite < 3.3.0 if we are above that version
        m_db.exec("PRAGMA legacy_file_format = 0");
        // Enable WAL journaling, which doesn't lock reads while writing
        m_db.exec("PRAGMA journal_mode = WAL");
    }

    void initQueries()
    {
        m_insert.compile("INSERT OR REPLACE INTO files (file, mtime, start_time, end_time) VALUES (?, ?, ?, ?)");
    }

    void initDB()
    {
        // Create the main table
        string query = "CREATE TABLE IF NOT EXISTS files ("
            "id INTEGER PRIMARY KEY,"
            " file TEXT NOT NULL,"
            " mtime INTEGER NOT NULL,"
            " start_time TEXT NOT NULL,"
            " end_time TEXT NOT NULL,"
            " UNIQUE(file) )";
        m_db.exec(query);
        m_db.exec("CREATE INDEX idx_files_start ON files (start_time)");
        m_db.exec("CREATE INDEX idx_files_end ON files (end_time)");
    }


public:
    SqliteManifest(const std::string& dir, const core::lock::Policy* lock_policy)
        : Manifest(dir, lock_policy), m_insert(m_db)
    {
    }

    virtual ~SqliteManifest()
    {
    }

    void flush()
    {
        // Not needed for index data consistency, but we need it to ensure file
        // timestamps are consistent at this point.
        m_db.checkpoint();
    }

    void openRO()
    {
        string pathname(str::joinpath(m_path, "index.sqlite"));
        if (m_db.isOpen())
            throw std::runtime_error("cannot open archive index: index " + pathname + " is already open");

        if (!sys::access(pathname, F_OK))
            throw std::runtime_error("opening archive index: index " + pathname + " does not exist");

        m_db.open(pathname);
        setupPragmas();

        initQueries();
    }

    void openRW()
    {
        string pathname(str::joinpath(m_path, "index.sqlite"));
        if (m_db.isOpen())
        {
            stringstream ss;
            ss << "archive index " << pathname << "is already open";
            throw runtime_error(ss.str());
        }

        bool need_create = !sys::access(pathname, F_OK);

        m_db.open(pathname);
        setupPragmas();
        
        if (need_create)
            initDB();

        initQueries();
    }

    std::vector<std::string> file_list(const Matcher& matcher) override
    {
        std::vector<std::string> files;
        string query;
        unique_ptr<Time> begin;
        unique_ptr<Time> end;
        if (!matcher.restrict_date_range(begin, end))
            return files;

        if (!begin.get() && !end.get())
        {
            // No restrictions on reftime: get all files
            query = "SELECT file FROM files ORDER BY start_time";
        } else {
            query = "SELECT file FROM files";

            if (begin.get())
                query += " WHERE end_time >= '" + begin->to_sql() + "'";
            if (end.get())
            {
                if (begin.get())
                    query += " AND start_time <= '" + end->to_sql() + "'";
                else
                    query += " WHERE start_time <= '" + end->to_sql() + "'";
            }

            query += " ORDER BY start_time";
        }

        Query q("sel_archive", m_db);
        q.compile(query);
        while (q.step())
            files.push_back(q.fetchString(0));
        return files;
    }

    bool segment_timespan(const std::string& relpath, Time& start_time, Time& end_time) const override
    {
        Query q("sel_file_ts", m_db);
        q.compile("SELECT start_time, end_time FROM files WHERE file=?");
        q.bind(1, relpath);

        bool found = false;
        while (q.step())
        {
            start_time.set_sql(q.fetchString(0));
            end_time.set_sql(q.fetchString(1));
            found = true;
        }
        return found;
    }

    void expand_date_range(unique_ptr<Time>& begin, unique_ptr<Time>& end) const override
    {
        Query q("sel_date_extremes", m_db);
        q.compile("SELECT MIN(start_time), MAX(end_time) FROM files");

        while (q.step())
        {
            Time st(Time::create_sql(q.fetchString(0)));
            Time et(Time::create_sql(q.fetchString(1)));

            if (!begin.get() || st < *begin)
                begin.reset(new Time(st));
            if (!end.get() || et > *end)
                end.reset(new Time(et));
        }
    }

    size_t vacuum()
    {
        // Vacuum the database
        try {
            m_db.exec("VACUUM");
            m_db.exec("ANALYZE");
        } catch (std::exception& e) {
            nag::warning("ignoring failed attempt to optimize database: %s", e.what());
        }
        return 0;
    }

    Pending test_writelock()
    {
        return Pending(new SqliteTransaction(m_db, "EXCLUSIVE"));
    }

    void acquire(const std::string& relpath, time_t mtime, const Summary& sum)
    {
        // Add to index
        unique_ptr<types::Reftime> rt = sum.getReferenceTime();

        string bt, et;

        switch (rt->style())
        {
            case types::Reftime::Style::POSITION: {
                const reftime::Position* p = dynamic_cast<const reftime::Position*>(rt.get());
                bt = et = p->time.to_sql();
                break;
            }
            case types::Reftime::Style::PERIOD: {
                const reftime::Period* p = dynamic_cast<const reftime::Period*>(rt.get());
                bt = p->begin.to_sql();
                et = p->end.to_sql();
                break;
            }
            default: {
                stringstream ss;
                ss << "unsupported reference time " << types::Reftime::formatStyle(rt->style());
                throw runtime_error(ss.str());
            }
        }

        m_insert.reset();
        m_insert.bind(1, relpath);
        m_insert.bind(2, mtime);
        m_insert.bind(3, bt);
        m_insert.bind(4, et);
        m_insert.step();
    }

    virtual void remove(const std::string& relpath)
    {
        Query q("del_file", m_db);
        q.compile("DELETE FROM files WHERE file=?");
        q.bind(1, relpath);
        while (q.step())
            ;
    }

    void list_segments(std::function<void(const std::string&)> dest) override
    {
        Query q("sel_archive", m_db);
        q.compile("SELECT DISTINCT file FROM files ORDER BY start_time");

        while (q.step())
            dest(q.fetchString(0));
    }

    void list_segments_filtered(const Matcher& matcher, std::function<void(const std::string&)> dest) override
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
            sq.compile("SELECT DISTINCT file FROM files WHERE start_time <= ? AND end_time >= ? ORDER BY start_time");
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
            sq.compile("SELECT DISTINCT file FROM files WHERE end_time >= ? ORDER BY start_time");
            string b = begin->to_sql();
            sq.bind(1, b);
            while (sq.step())
                dest(sq.fetchString(0));
        }
        else
        {
            Query sq("list_segments", m_db);
            sq.compile("SELECT DISTINCT file FROM files WHERE start_time <= ? ORDER BY start_time");
            string e = end->to_sql();
            sq.bind(1, e);
            while (sq.step())
                dest(sq.fetchString(0));
        }
    }

    bool has_segment(const std::string& relpath) const override
    {
        Query q("sel_has_segment", m_db);
        q.compile("SELECT 1 FROM files WHERE file=?");
        q.bind(1, relpath);
        bool res = false;
        while (q.step())
            res = true;
        return res;
    }

    time_t segment_mtime(const std::string& relpath) const override
    {
        Query q("sel_mtime", m_db);
        q.compile("SELECT mtime FROM files WHERE file=?");
        q.bind(1, relpath);
        time_t res = 0;
        while (q.step())
            res = q.fetch<time_t>(0);
        return res;
    }

    void test_rename(const std::string& relpath, const std::string& new_relpath) override
    {
        Query q("test_rename", m_db);
        q.compile("UPDATE files SET file=? WHERE file=?");
        q.bind(1, new_relpath);
        q.bind(2, relpath);
        while (q.step())
            ;
    }

    static bool exists(const std::string& dir)
    {
        string pathname(str::joinpath(dir, "index.sqlite"));
        return sys::access(pathname, F_OK);
    }
};

}

bool Manifest::get_force_sqlite()
{
    return manifest::mft_force_sqlite;
}

void Manifest::set_force_sqlite(bool val)
{
    manifest::mft_force_sqlite = val;
}

bool Manifest::exists(const std::string& dir)
{
    return manifest::PlainManifest::exists(dir) ||
        manifest::SqliteManifest::exists(dir);
}

std::unique_ptr<Manifest> Manifest::create(const std::string& dir, const core::lock::Policy* lock_policy, const std::string& index_type)
{
    if (index_type.empty())
    {
        if (manifest::mft_force_sqlite || manifest::SqliteManifest::exists(dir))
            return unique_ptr<Manifest>(new manifest::SqliteManifest(dir, lock_policy));
        else
            return unique_ptr<Manifest>(new manifest::PlainManifest(dir, lock_policy));
    }
    else if (index_type == "plain")
        return unique_ptr<Manifest>(new manifest::PlainManifest(dir, lock_policy));
    else if (index_type == "sqlite")
        return unique_ptr<Manifest>(new manifest::SqliteManifest(dir, lock_policy));
    else
        throw std::runtime_error("unsupported index_type " + index_type);
}

}
}
}
