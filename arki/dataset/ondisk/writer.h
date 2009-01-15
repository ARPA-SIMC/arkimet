#ifndef ARKI_DATASET_ONDISK_WRITER_H
#define ARKI_DATASET_ONDISK_WRITER_H

/*
 * dataset/ondisk/writer - Local on disk dataset writer
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
#include <arki/configfile.h>
#include <string>

namespace arki {

class Metadata;
class MetadataConsumer;
class Matcher;
class Summary;

namespace dataset {
namespace ondisk {
class MaintenanceAgent;
class RepackAgent;
class Visitor;

namespace writer {
class RootDirectory;
}

class Writer : public WritableDataset
{
protected:
	ConfigFile m_cfg;
	std::string m_name;
	ondisk::writer::RootDirectory* m_root;
	bool m_replace;

public:
	// Initialise the dataset with the information from the configurationa in 'cfg'
	Writer(const ConfigFile& cfg);

	virtual ~Writer();

	std::string path() const;

    // Compute the unique ID of a metadata in this dataset
	virtual std::string id(const Metadata& md) const;

    /**
	 * Acquire the given metadata item (and related data) in this dataset.
	 *
	 * After acquiring the data successfully, the data can be retrieved from
	 * the dataset.  Also, information such as the dataset name and the id of
	 * the data in the dataset are added to the Metadata object.
	 *
	 * @return true if the data is successfully stored in the dataset, else
	 * false.  If false is returned, a note is added to the dataset explaining
	 * the reason of the failure.
	 */
	virtual AcquireResult acquire(Metadata& md);

	virtual bool replace(Metadata& md);

	virtual void remove(const std::string& id);

	virtual void flush();

	/**
	 * Perform dataset maintenance, using a MaintenanceAgent to direct the operations
	 */
	void maintenance(MaintenanceAgent& a);

	/**
	 * Repack the dataset, using a RepackAgent to direct the operations
	 */
	void repack(RepackAgent& a);

	/**
	 * Iterate through the contents of the dataset, in depth-first order.
	 */
	void depthFirstVisit(Visitor& v);

	/**
	 * Invalidate all the dataset metadata and index, by creating the index
	 * flagfile and a pack flagfile for every metadata file in the dataset.
	 */
	void invalidateAll();

	static AcquireResult testAcquire(const ConfigFile& cfg, const Metadata& md, std::ostream& out);
};

}
}
}

// vim:set ts=4 sw=4:
#endif
