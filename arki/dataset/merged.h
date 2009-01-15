#ifndef ARKI_DATASET_MERGED_H
#define ARKI_DATASET_MERGED_H

/*
 * dataset/merged - Access many datasets at the same time
 *
 * Copyright (C) 2007,2008  ARPA-SIM <urpsim@smr.arpa.emr.it>
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
#include <string>
#include <vector>

namespace arki {

class ConfigFile;
class Metadata;
class MetadataConsumer;
class Matcher;

namespace dataset {

/**
 * Access multiple datasets together
 */
class Merged : public ReadonlyDataset
{
protected:
	std::vector<ReadonlyDataset*> datasets;

public:
	// Initialise the dataset with the information from the configurationa in 'cfg'
	Merged();
	virtual ~Merged();

	/// Add a dataset to the group of datasets to merge
	void addDataset(ReadonlyDataset& ds);

	/**
	 * Query the dataset using the given matcher, and sending the results to
	 * the metadata consumer.
	 */
	virtual void queryMetadata(const Matcher& matcher, bool withData, MetadataConsumer& consumer);
	virtual void querySummary(const Matcher& matcher, Summary& summary);
	virtual void queryBytes(const Matcher& matcher, std::ostream& out, ByteQuery qtype = BQ_DATA, const std::string& param = std::string());
};

}
}

// vim:set ts=4 sw=4:
#endif
