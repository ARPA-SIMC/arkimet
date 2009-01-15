#ifndef ARKI_DATASET_ONDISK_READER_H
#define ARKI_DATASET_ONDISK_READER_H

/*
 * dataset/ondisk/reader - Local on disk dataset reader
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

#include <arki/dataset/local.h>
#include <string>
#include <map>

namespace arki {

class ConfigFile;
class Metadata;
class MetadataConsumer;
class Matcher;
class Summary;

namespace dataset {
class DSIndex;
class TargetFile;

namespace ondisk {
class Reader : public Local
{
protected:
	DSIndex* m_idx;
	std::string m_root;
	TargetFile* m_tf;

	/**
	 * Query the dataset, without using an index, using the given matcher, and
	 * sending the results to the metadata consumer.
	 */
	void queryWithoutIndex(const std::string& root, const Matcher& matcher, MetadataConsumer& consumer) const;

	void scanSummaries(const std::string& top, const Matcher& matcher, Summary& result);

public:
	// Initialise the dataset with the information from the configuration in 'cfg'
	Reader(const ConfigFile& cfg);
	virtual ~Reader();

	/**
	 * Query the dataset using the given matcher, and sending the results to
	 * the metadata consumer.
	 */
	virtual void queryMetadata(const Matcher& matcher, bool withData, MetadataConsumer& consumer);

	virtual void queryBytes(const Matcher& matcher, std::ostream& out, ByteQuery qtype = BQ_DATA, const std::string& param = std::string());

	virtual void querySummary(const Matcher& matcher, Summary& summary);

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
