#ifndef ARKI_DATASET_EMPTY_H
#define ARKI_DATASET_EMPTY_H

/*
 * dataset/empty - Virtual read only dataset that is always empty
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

namespace arki {

class ConfigFile;
class Metadata;
class MetadataConsumer;
class Matcher;

namespace dataset {

/**
 * Dataset that is always empty
 */
class Empty : public Local
{
public:
	// Initialise the dataset with the information from the configurationa in 'cfg'
	Empty(const ConfigFile& cfg);
	virtual ~Empty();

	// Nothing to do: the dataset is always empty
	virtual void queryData(const dataset::DataQuery& q, MetadataConsumer& consumer) {}
	virtual void querySummary(const Matcher& matcher, Summary& summary) {}
	virtual void queryBytes(const dataset::ByteQuery& q, std::ostream& out) {}

	virtual void rescanFile(const std::string& relpath) {}
	virtual size_t repackFile(const std::string& relpath) { return 0; }
	virtual size_t removeFile(const std::string& relpath, bool withData=false) { return 0; }
	virtual void archiveFile(const std::string& relpath) {}
	virtual size_t vacuum() { return 0; }
};

}
}

// vim:set ts=4 sw=4:
#endif
