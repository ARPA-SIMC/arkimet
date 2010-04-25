#ifndef ARKI_DATASET_ONDISK2_ARCHIVE_H
#define ARKI_DATASET_ONDISK2_ARCHIVE_H

/*
 * dataset/ondisk2/archive - Handle archived data
 *
 * Copyright (C) 2009  ARPA-SIM <urpsim@smr.arpa.emr.it>
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

#include <arki/dataset/local.h>

#include <string>
#include <iosfwd>

namespace arki {
class Matcher;
class MetadataConsumer;

namespace utils {
namespace metadata {
class Collector;
}
}

namespace dataset {

namespace maintenance {
class MaintFileVisitor;
}

namespace simple {
class Manifest;
class Reader;
}

namespace ondisk2 {

class Archive : public Local
{
protected:
	std::string m_dir;
	simple::Reader* m_reader;
	simple::Manifest* m_mft;

	void querySummaries(const Matcher& matcher, Summary& summary);

public:
	Archive(const std::string& dir);
	virtual ~Archive();

	const std::string& path() const { return m_dir; }

	void openRO();
	void openRW();

	virtual void queryData(const dataset::DataQuery& q, MetadataConsumer& consumer);
	virtual void queryBytes(const dataset::ByteQuery& q, std::ostream& out);
	virtual void querySummary(const Matcher& matcher, Summary& summary);

	void acquire(const std::string& relname);
	void acquire(const std::string& relname, const utils::metadata::Collector& mds);
	void remove(const std::string& relname);
	void rescan(const std::string& relname);
	void deindex(const std::string& relname);

	void flush();

	void maintenance(maintenance::MaintFileVisitor& v);
	/*
	void repack(std::ostream& log, bool writable=false);
	void check(std::ostream& log);
	*/

	void vacuum();

	static bool is_archive(const std::string& dir);
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
class Archives : public Local
{
protected:
	std::string m_dir;
	bool m_read_only;

	std::map<std::string, Archive*> m_archives;
	Archive* m_last;

	// Look up an archive, returns 0 if not found
	Archive* lookup(const std::string& name);

public:
	Archives(const std::string& dir, bool read_only = true);
	virtual ~Archives();

	const std::string& path() const { return m_dir; }

	virtual void queryData(const dataset::DataQuery& q, MetadataConsumer& consumer);
	virtual void queryBytes(const dataset::ByteQuery& q, std::ostream& out);
	virtual void querySummary(const Matcher& matcher, Summary& summary);

	void acquire(const std::string& relname);
	void acquire(const std::string& relname, const utils::metadata::Collector& mds);
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
}

#endif
