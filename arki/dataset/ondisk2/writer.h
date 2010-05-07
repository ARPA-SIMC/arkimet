#ifndef ARKI_DATASET_ONDISK2_WRITER_H
#define ARKI_DATASET_ONDISK2_WRITER_H

/*
 * dataset/ondisk2/writer - Local on disk dataset writer
 *
 * Copyright (C) 2007,2008,2009  ARPA-SIM <urpsim@smr.arpa.emr.it>
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
#include <arki/configfile.h>
#include <arki/dataset/ondisk2/index.h>
#include <string>
#include <memory>

namespace arki {
class Metadata;
class Matcher;
class Summary;

namespace dataset {
class TargetFile;

namespace maintenance {
class MaintFileVisitor;
}

namespace ondisk2 {
class Archive;
class Archives;

namespace writer {
class Datafile;
class RealRepacker;
class RealFixer;
}

class Writer : public WritableLocal
{
protected:
	ConfigFile m_cfg;
	WIndex m_idx;
	TargetFile* m_tf;
	bool m_replace;

	std::map<std::string, writer::Datafile*> m_df_cache;

	/// Return a (shared) instance of the Datafile for the given relative pathname
	writer::Datafile* file(const std::string& pathname);

public:
	// Initialise the dataset with the information from the configurationa in 'cfg'
	Writer(const ConfigFile& cfg);

	virtual ~Writer();

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

	virtual void maintenance(maintenance::MaintFileVisitor& v, bool quick=true);

	virtual void rescanFile(const std::string& relpath);
	virtual size_t repackFile(const std::string& relpath);
	virtual size_t removeFile(const std::string& relpath, bool withData=false);
	virtual void archiveFile(const std::string& relpath);
	virtual size_t vacuum();

	/**
	 * Iterate through the contents of the dataset, in depth-first order.
	 */
	//void depthFirstVisit(Visitor& v);

	static AcquireResult testAcquire(const ConfigFile& cfg, const Metadata& md, std::ostream& out);

	friend class writer::RealRepacker;
	friend class writer::RealFixer;
};

}
}
}

// vim:set ts=4 sw=4:
#endif
