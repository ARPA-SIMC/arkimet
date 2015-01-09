#ifndef ARKI_DATASET_SIMPLE_READER_H
#define ARKI_DATASET_SIMPLE_READER_H

/*
 * dataset/simple/reader - Reader for simple datasets with no duplicate checks
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

#include <arki/dataset/local.h>

#include <string>
#include <iosfwd>

namespace arki {
class ConfigFile;
class Matcher;

namespace dataset {
namespace maintenance {
class MaintFileVisitor;
}

namespace index {
struct Manifest;
}

namespace simple {

class Reader : public SegmentedLocal
{
protected:
    index::Manifest* m_mft;

public:
	Reader(const ConfigFile& cfg);
	virtual ~Reader();

	virtual void queryData(const dataset::DataQuery& q, metadata::Eater& consumer);
	virtual void querySummary(const Matcher& matcher, Summary& summary);
    virtual size_t produce_nth(metadata::Eater& cons, size_t idx=0);

	void maintenance(maintenance::MaintFileVisitor& v);

	static bool is_dataset(const std::string& dir);
};

}
}
}

#endif
