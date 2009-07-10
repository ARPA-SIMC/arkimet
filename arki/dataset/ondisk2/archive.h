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

#include <arki/utils/sqlite.h>
#include <arki/dataset/local.h>
#include <arki/dataset/index/base.h>

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
namespace ondisk2 {

namespace writer {
class MaintFileVisitor;
}

class Archive : public Local
{
protected:
	std::string m_dir;
	int m_delete_age;

	mutable utils::sqlite::SQLiteDB m_db;
	index::InsertQuery m_insert;

	void setupPragmas();
	void initQueries();
	void initDB();
	void fileList(const Matcher& matcher, std::vector<std::string>& files) const;

public:
	Archive(const std::string& dir, int delete_age = -1);
	virtual ~Archive();

	void openRO();
	void openRW();

	virtual void queryMetadata(const Matcher& matcher, bool withData, MetadataConsumer& consumer);
	virtual void queryBytes(const Matcher& matcher, std::ostream& out, ByteQuery qtype = BQ_DATA, const std::string& param = std::string());
	virtual void querySummary(const Matcher& matcher, Summary& summary);

	void acquire(const std::string& relname);
	void acquire(const std::string& relname, const utils::metadata::Collector& mds);
	void remove(const std::string& relname);

	void maintenance(writer::MaintFileVisitor& v);
	void repack(std::ostream& log, bool writable=false);
	void check(std::ostream& log);
};

}
}
}

#endif
