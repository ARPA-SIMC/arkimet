#ifndef ARKI_DATASET_ARCHIVE_H
#define ARKI_DATASET_ARCHIVE_H

/*
 * dataset/archive - Handle archived data
 *
 * Copyright (C) 2009--2015  ARPA-SIM <urpsim@smr.arpa.emr.it>
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

#include <arki/dataset.h>
#include <arki/summary.h>
#include <string>
#include <map>
#include <iosfwd>

namespace arki {
class Summary;
class Matcher;

namespace metadata{
class Collection;
}

namespace dataset {
class DataQuery;
class ByteQuery;

namespace maintenance {
class MaintFileVisitor;
}

namespace index {
class Manifest;
}

namespace simple {
class Reader;
}

class Archive : public ReadonlyDataset
{
public:
    virtual ~Archive();

    virtual void acquire(const std::string& relname) = 0;
    virtual void acquire(const std::string& relname, metadata::Collection& mds) = 0;
    virtual void remove(const std::string& relname) = 0;
    virtual void rescan(const std::string& relname) = 0;
    virtual void deindex(const std::string& relname) = 0;
    virtual void flush() = 0;
    virtual void maintenance(maintenance::MaintFileVisitor& v) = 0;
    virtual void vacuum() = 0;
    /**
     * Expand the given begin and end ranges to include the datetime extremes
     * of this manifest.
     *
     * If begin and end are unset, set them to the datetime extremes of this
     * manifest.
     */
    virtual void expand_date_range(std::unique_ptr<types::Time>& begin, std::unique_ptr<types::Time>& end) const = 0;
    /**
     * Output to \a cons the idx-th element of each file
     *
     * @return true if something was produced, else false
     */
    virtual size_t produce_nth(metadata::Eater& cons, size_t idx=0) = 0;

    static bool is_archive(const std::string& dir);
    static Archive* create(const std::string& dir, bool writable=false);
};

class OnlineArchive : public Archive
{
protected:
    std::string m_dir;
    index::Manifest* m_mft;

    void querySummaries(const Matcher& matcher, Summary& summary);

public:
    OnlineArchive(const std::string& dir);
    ~OnlineArchive();

    const std::string& path() const { return m_dir; }

    void openRO();
    void openRW();

    virtual void queryData(const dataset::DataQuery& q, metadata::Eater& consumer);
    virtual void queryBytes(const dataset::ByteQuery& q, std::ostream& out);
    virtual void querySummary(const Matcher& matcher, Summary& summary);
    void expand_date_range(std::unique_ptr<types::Time>& begin, std::unique_ptr<types::Time>& end) const override;
    virtual size_t produce_nth(metadata::Eater& cons, size_t idx=0);

    virtual void acquire(const std::string& relname);
    virtual void acquire(const std::string& relname, metadata::Collection& mds);
    virtual void remove(const std::string& relname);
    virtual void rescan(const std::string& relname);
    virtual void deindex(const std::string& relname);

    virtual void flush();

    virtual void maintenance(maintenance::MaintFileVisitor& v);

    virtual void vacuum();

    /*
       void repack(std::ostream& log, bool writable=false);
       void check(std::ostream& log);
       */
};

/**
 * Archive that has been put offline (only a summary file is left)
 */
struct OfflineArchive : public Archive
{
    std::string fname;
    Summary sum;

    OfflineArchive(const std::string& fname);
    ~OfflineArchive();

    virtual void queryData(const dataset::DataQuery& q, metadata::Eater& consumer);
    virtual void queryBytes(const dataset::ByteQuery& q, std::ostream& out);
    virtual void querySummary(const Matcher& matcher, Summary& summary);
    void expand_date_range(std::unique_ptr<types::Time>& begin, std::unique_ptr<types::Time>& end) const override;
    virtual size_t produce_nth(metadata::Eater& cons, size_t idx=0);

    virtual void acquire(const std::string& relname);
    virtual void acquire(const std::string& relname, metadata::Collection& mds);
    virtual void remove(const std::string& relname);
    virtual void rescan(const std::string& relname);
    virtual void deindex(const std::string& relname);
    virtual void flush();
    virtual void maintenance(maintenance::MaintFileVisitor& v);
    virtual void vacuum();
};

/**
 * Set of archives.
 *
 * Every archive is a subdirectory. Subdirectories are always considered in
 * alphabetical order, except for a subdirectory named "last". The subdirectory
 * named "last" if it exists it always appears last.
 *
 * The idea is to have one archive called "last" where data is automatically
 * archived from the live dataset, and other archives maintained manually by
 * moving files from "last" into them.
 *
 * ondisk2 dataset maintenance will therefore only write files older than
 * "archive age" to the "last" archive.
 *
 * When doing archive maintenance, all archives are checked. If a file is
 * manually moved from an archive to another, it will be properly deindexed and
 * reindexed during the archive maintenance.
 *
 * When querying, all archives are queried, following the archive order:
 * alphabetical order except the archive named "last" is queried last.
 */
class Archives : public ReadonlyDataset
{
protected:
	std::string m_scache_root;
	std::string m_dir;
	bool m_read_only;

	std::map<std::string, Archive*> m_archives;
	Archive* m_last;

	// Look up an archive, returns 0 if not found
	Archive* lookup(const std::string& name);

    void invalidate_summary_cache();
    void summary_for_all(Summary& out);
    void rebuild_summary_cache();
    void queryData(const dataset::DataQuery& q, metadata::Eater& consumer) override;

public:
	Archives(const std::string& root, const std::string& dir, bool read_only = true);
	virtual ~Archives();

    /**
     * Update the list of archives
     *
     * It can be called to update the archive view after some have been moved.
     */
    void rescan_archives();

	const std::string& path() const { return m_dir; }

    void queryBytes(const dataset::ByteQuery& q, std::ostream& out) override;
    void querySummary(const Matcher& matcher, Summary& summary) override;
    virtual size_t produce_nth(metadata::Eater& cons, size_t idx=0);
    void expand_date_range(std::unique_ptr<types::Time>& begin, std::unique_ptr<types::Time>& end) const;

	void acquire(const std::string& relname);
	void acquire(const std::string& relname, metadata::Collection& mds);
	void remove(const std::string& relname);
	void rescan(const std::string& relname);

	void flush();

	void maintenance(maintenance::MaintFileVisitor& v);
	/*
	void repack(std::ostream& log, bool writable=false);
	void check(std::ostream& log);
	*/

	void vacuum();
};

}
}

#endif
