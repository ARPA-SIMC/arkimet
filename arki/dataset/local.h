#ifndef ARKI_DATASET_LOCAL_H
#define ARKI_DATASET_LOCAL_H

/*
 * dataset/local - Base class for local datasets
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
struct Archives;

namespace maintenance {
class MaintFileVisitor;
}

/**
 * Base class for local datasets
 */
class Local : public ReadonlyDataset
{
protected:
	std::string m_name;
	std::string m_path;
	mutable Archives* m_archive;

public:
	Local(const ConfigFile& cfg);
	~Local();

	// Return the dataset name
	const std::string& name() const { return m_name; }

	// Return the dataset path
	const std::string& path() const { return m_path; }

	// Base implementations that query the archives if they exist
	virtual void queryData(const dataset::DataQuery& q, metadata::Consumer& consumer);
	virtual void querySummary(const Matcher& matcher, Summary& summary);

	bool hasArchive() const;
	Archives& archive();
	const Archives& archive() const;

	static void readConfig(const std::string& path, ConfigFile& cfg);
};

class WritableLocal : public WritableDataset
{
protected:
	std::string m_path;
	mutable Archives* m_archive;
	int m_archive_age;
	int m_delete_age;

public:
	WritableLocal(const ConfigFile& cfg);
	~WritableLocal();

	// Return the dataset path
	const std::string& path() const { return m_path; }

	bool hasArchive() const;
	Archives& archive();
	const Archives& archive() const;

	// Maintenance functions

	/**
	 * Perform dataset maintenance, sending information to \a v
	 *
	 * Subclassers should call WritableLocal's maintenance method at the
	 * end of their own maintenance, as it takes care of performing
	 * maintainance of archives, if present.
	 *
	 * @params v
	 *   The visitor-style class that gets notified of the state of the
	 *   various files in the dataset
	 * @params quick
	 *   If false, contents of the data files will also be checked for
	 *   consistency
	 */
	virtual void maintenance(maintenance::MaintFileVisitor& v, bool quick=true);

	/**
	 * Perform general sanity checks on the dataset, reporting to \a log.
	 *
	 * If \a writable is true, try to fix issues.
	 */
	virtual void sanityChecks(std::ostream& log, bool writable=false);

	/// Remove all data from the dataset
	void removeAll(std::ostream& log, bool writable);

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

	/**
	 * Consider all existing metadata about a file as invalid and rebuild
	 * them by rescanning the file
	 */
	virtual void rescanFile(const std::string& relpath) = 0;

	/**
	 * Optimise the contents of a data file
	 *
	 * In the resulting file, there are no holes for deleted data and all
	 * the data is sorted by reference time
	 *
	 * @returns The number of bytes freed on disk with this operation
	 */
	virtual size_t repackFile(const std::string& relpath) = 0;

	/**
	 * Remove the file from the dataset
	 *
	 * @returns The number of bytes freed on disk with this operation
	 */
	virtual size_t removeFile(const std::string& relpath, bool withData=false) = 0;

	/**
	 * Move the file to archive
	 *
	 * The default implementation moves the file and its associated
	 * metadata and summaries (if found) to the "last" archive, and adds it
	 * to its manifest
	 */
	virtual void archiveFile(const std::string& relpath);

	/**
	 * Perform generic packing and optimisations
	 *
	 * @returns The number of bytes freed on disk with this operation
	 */
	virtual size_t vacuum() = 0;

	/**
	 * Instantiate an appropriate Dataset for the given configuration
	 */
	static WritableLocal* create(const ConfigFile& cfg);

	/**
	 * Simulate acquiring the given metadata item (and related data) in this
	 * dataset.
	 *
	 * No change of any kind happens to the dataset.  Information such as the
	 * dataset name and the id of the data in the dataset are added to the
	 * Metadata object.
	 *
	 * @return The outcome of the operation.
	 */
	static AcquireResult testAcquire(const ConfigFile& cfg, const Metadata& md, std::ostream& out);
};

}
}

// vim:set ts=4 sw=4:
#endif
