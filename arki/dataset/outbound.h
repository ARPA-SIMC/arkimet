#ifndef ARKI_DATASET_OUTBOUND_H
#define ARKI_DATASET_OUTBOUND_H

/*
 * dataset/outbound - Local, non queryable, on disk dataset
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

#include <arki/dataset.h>
#include <string>

namespace arki {
class ConfigFile;
class Metadata;
class Matcher;

namespace dataset {

class TargetFile;

/**
 * Store-only dataset.
 *
 * This dataset is not used for archival, but only to store data as an outbound
 * area.
 */
class Outbound : public WritableDataset
{
protected:
	std::string m_path;
	TargetFile* m_tf;

	void storeBlob(Metadata& md, const std::string& reldest);

public:
	// Initialise the dataset with the information from the configurationa in 'cfg'
	Outbound(const ConfigFile& cfg);

	virtual ~Outbound();

    /**
     * Acquire the given metadata item (and related data) in this dataset.
     *
     * @return true if the data is successfully stored in the dataset, else
     * false.  If false is returned, a note is added to the dataset explaining
     * the reason of the failure.
     */
    virtual AcquireResult acquire(Metadata& md, ReplaceStrategy replace=REPLACE_DEFAULT);

	virtual void remove(Metadata& id);
	virtual void removeAll(std::ostream& log, bool writable=false);

	static AcquireResult testAcquire(const ConfigFile& cfg, const Metadata& md, std::ostream& out);
};

}
}

// vim:set ts=4 sw=4:
#endif
