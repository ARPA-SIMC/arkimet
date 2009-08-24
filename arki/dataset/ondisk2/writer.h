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

#include <arki/dataset.h>
#include <arki/configfile.h>
#include <arki/dataset/ondisk2/index.h>
#include <string>
#include <memory>

namespace arki {

class Metadata;
class MetadataConsumer;
class Matcher;
class Summary;

namespace dataset {
class TargetFile;

namespace ondisk2 {
class Archive;
class Archives;

namespace writer {
class Datafile;
class MaintFileVisitor;
class Agent;
class RealRepacker;
class MockRepacker;
class RealFixer;
class MockFixer;
}

class Writer : public WritableDataset
{
protected:
	ConfigFile m_cfg;
	std::string m_path;
	WIndex m_idx;
	TargetFile* m_tf;
	mutable Archives* m_archive;
	bool m_replace;
	int m_archive_age;
	int m_delete_age;

	std::map<std::string, writer::Datafile*> m_df_cache;

	/// Return a (shared) instance of the Datafile for the given relative pathname
	writer::Datafile* file(const std::string& pathname);

public:
	// Initialise the dataset with the information from the configurationa in 'cfg'
	Writer(const ConfigFile& cfg);

	virtual ~Writer();

	bool hasArchive() const;
	Archives& archive();
	const Archives& archive() const;

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
	 * Perform dataset maintenance, sending information to \a v
	 */
	void maintenance(writer::MaintFileVisitor& v, bool quick=true);

	/**
	 * Repack the dataset, logging status to the given file.
	 *
	 * If writable is false, the process is simulated but no changes are
	 * saved.
	 */
	virtual void repack(std::ostream& log, bool writable=false);

	/**
	 * Check the dataset for errors, logging status to the given file.
	 *
	 * If \a fix is false, the process is simulated but no changes are saved.
	 * If \a fix is true, errors are fixed.
	 */
	virtual void check(std::ostream& log, bool fix=false);

	/**
	 * Iterate through the contents of the dataset, in depth-first order.
	 */
	//void depthFirstVisit(Visitor& v);

	static AcquireResult testAcquire(const ConfigFile& cfg, const Metadata& md, std::ostream& out);

	friend class writer::Agent;
	friend class writer::RealRepacker;
	friend class writer::MockRepacker;
	friend class writer::RealFixer;
	friend class writer::MockFixer;
};

}
}
}

// vim:set ts=4 sw=4:
#endif
