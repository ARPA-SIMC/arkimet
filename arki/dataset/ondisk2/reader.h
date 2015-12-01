#ifndef ARKI_DATASET_ONDISK2_READER_H
#define ARKI_DATASET_ONDISK2_READER_H

/*
 * dataset/ondisk2/reader - Local on disk dataset reader
 *
 * Copyright (C) 2007--2013  ARPA-SIM <urpsim@smr.arpa.emr.it>
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
#include <map>
#include <vector>

namespace arki {
class ConfigFile;
class Metadata;
class Matcher;
class Summary;

namespace dataset {
class TargetFile;

namespace index {
class RContents;
}

namespace ondisk2 {

class Reader : public Local
{
protected:
    index::RContents* m_idx;
	TargetFile* m_tf;

    // Query only the data in the dataset, without the archives
    void queryLocalData(const dataset::DataQuery& q, metadata_dest_func dest);

public:
	// Initialise the dataset with the information from the configuration in 'cfg'
	Reader(const ConfigFile& cfg);
	virtual ~Reader();

    void query_data(const dataset::DataQuery& q, metadata_dest_func dest) override;
    void querySummary(const Matcher& matcher, Summary& summary) override;
    size_t produce_nth(metadata::Eater& cons, size_t idx=0) override;

	/**
	 * Return true if this dataset has a working index.
	 *
	 * This method is mostly used for tests.
	 */
	bool hasWorkingIndex() const { return m_idx != 0; }
};

}
}
}

// vim:set ts=4 sw=4:
#endif
