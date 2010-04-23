#ifndef ARKI_DATASET_SIMPLE_READER_H
#define ARKI_DATASET_SIMPLE_READER_H

/*
 * dataset/simple/reader - Reader for simple datasets with no duplicate checks
 *
 * Copyright (C) 2009--2010  ARPA-SIM <urpsim@smr.arpa.emr.it>
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
struct Manifest;

class Reader : public Local
{
protected:
	std::string m_dir;
	Manifest* m_mft;

	void querySummaries(const Matcher& matcher, Summary& summary);

public:
	Reader(const std::string& dir);
	virtual ~Reader();

	const std::string& path() const { return m_dir; }

	virtual void queryData(const dataset::DataQuery& q, MetadataConsumer& consumer);
	virtual void queryBytes(const dataset::ByteQuery& q, std::ostream& out);
	virtual void querySummary(const Matcher& matcher, Summary& summary);

	static bool is_dataset(const std::string& dir);
};

}
}
}

#endif
