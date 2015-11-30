#ifndef ARKI_DATASET_EMPTY_H
#define ARKI_DATASET_EMPTY_H

/*
 * dataset/empty - Virtual read only dataset that is always empty
 *
 * Copyright (C) 2007--2010  ARPA-SIM <urpsim@smr.arpa.emr.it>
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
class Matcher;

namespace metadata {
class Consumer;
}

namespace dataset {

/**
 * Dataset that is always empty
 */
class Empty : public Local
{
protected:
    void queryData(const dataset::DataQuery& q, metadata::Eater& consumer) override {}

public:
	// Initialise the dataset with the information from the configurationa in 'cfg'
	Empty(const ConfigFile& cfg);
	virtual ~Empty();

    // Nothing to do: the dataset is always empty
    void querySummary(const Matcher& matcher, Summary& summary) override {}
    void queryBytes(const dataset::ByteQuery& q, std::ostream& out) override {}
    size_t produce_nth(metadata::Eater& cons, size_t idx=0) override { return 0; }


    //void rescanFile(const std::string& relpath) override {}
    //size_t repackFile(const std::string& relpath) override { return 0; }
    //size_t removeFile(const std::string& relpath, bool withData=false) override { return 0; }
    //void archiveFile(const std::string& relpath) override {}
    //size_t vacuum() override { return 0; }
};

}
}

// vim:set ts=4 sw=4:
#endif
