#ifndef ARKI_DATASET_SIMPLE_WRITER_H
#define ARKI_DATASET_SIMPLE_WRITER_H

/*
 * dataset/simple/writer - Writer for simple datasets with no duplicate checks
 *
 * Copyright (C) 2009--2010  ARPA-SIM <urpsim@smr.arpa.emr.it>
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

#include <string>
#include <iosfwd>

namespace arki {
class Matcher;
class MetadataConsumer;

namespace utils {
namespace metadata {
class Collector;
}
}

namespace dataset {
class TargetFile;

namespace maintenance {
class MaintFileVisitor;
}

namespace simple {
class Manifest;
class Reader;
class Datafile;

class Writer : public WritableLocal
{
protected:
	simple::Manifest* m_mft;
	TargetFile* m_tf;
	std::map<std::string, Datafile*> m_df_cache;

	/// Return a (shared) instance of the Datafile for the given relative pathname
	Datafile* file(const std::string& pathname);

public:
	Writer(const ConfigFile& cfg);
	virtual ~Writer();

	/**
	 * Compute the unique ID of a metadata in this dataset
	 *
	 * This dataset does not detect duplicates, so it always returns the
	 * empty string
	 */
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

	/**
	 * This dataset does not detect duplicates, so replace is the same as
	 * acquire.
	 */
	bool replace(Metadata& md);

	/**
	 * This dataset stores duplicates, so it cannot unambiguously locale
	 * single data items. This functions does nothing.
	 */
	void remove(const std::string& id);

	virtual void flush();

	void maintenance(maintenance::MaintFileVisitor& v, bool quick=true);

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
	virtual void check(std::ostream& log, bool fix, bool quick);

	virtual void rescanFile(const std::string& relpath);
	virtual size_t repackFile(const std::string& relpath);
	virtual void archiveFile(const std::string& relpath);
	virtual size_t removeFile(const std::string& relpath, bool withData=false);
	virtual size_t vacuum();

	static AcquireResult testAcquire(const ConfigFile& cfg, const Metadata& md, std::ostream& out);
};

}
}
}

#endif














#if 0
namespace arki {

class Metadata;
class MetadataConsumer;
class Matcher;
class Summary;

namespace dataset {

namespace maintenance {
class MaintFileVisitor;
}

namespace ondisk2 {
class Archive;
class Archives;

namespace writer {
class Datafile;
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
	mutable Archives* m_archive;
	bool m_replace;
	int m_archive_age;
	int m_delete_age;

public:
	// Initialise the dataset with the information from the configurationa in 'cfg'
	Writer(const ConfigFile& cfg);

	virtual ~Writer();

	/**
	 * Iterate through the contents of the dataset, in depth-first order.
	 */
	//void depthFirstVisit(Visitor& v);

	friend class writer::Agent;
	friend class writer::RealRepacker;
	friend class writer::MockRepacker;
	friend class writer::RealFixer;
	friend class writer::MockFixer;
};

}
}
}
#endif

// vim:set ts=4 sw=4:
