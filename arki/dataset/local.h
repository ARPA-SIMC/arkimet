#ifndef ARKI_DATASET_LOCAL_H
#define ARKI_DATASET_LOCAL_H

/*
 * dataset/local - Base class for local datasets
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

namespace arki {

class ConfigFile;
class Metadata;
class Matcher;

namespace dataset {
struct Archives;

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
	 */
	virtual void archiveFile(const std::string& relpath) = 0;

	/**
	 * Perform generic packing and optimisations
	 *
	 * @returns The number of bytes freed on disk with this operation
	 */
	virtual size_t vacuum() = 0;
};

}
}

// vim:set ts=4 sw=4:
#endif
