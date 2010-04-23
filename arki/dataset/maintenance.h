#ifndef ARKI_DATASET_MAINTENANCE_H
#define ARKI_DATASET_MAINTENANCE_H

/*
 * dataset/maintenance - Dataset maintenance utilities
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

#include <string>
#include <vector>

namespace arki {

namespace scan {
struct Validator;
}

namespace dataset {
namespace maintenance {

/**
 * Visitor interface for scanning information about the files indexed in the database
 */
struct MaintFileVisitor
{
	enum State {
		OK,		/// File fully and consistently indexed
		TO_ARCHIVE,	/// File is ok, but old enough to be archived
		TO_DELETE,	/// File is ok, but old enough to be deleted
		TO_PACK,	/// File contains data that has been deleted
		TO_INDEX,	/// File is not present in the index
		TO_RESCAN,	/// File contents are inconsistent with the index
		DELETED,	/// File does not exist but has entries in the index
		ARC_OK,		/// File fully and consistently archived
		ARC_TO_INDEX,	/// File is not present in the archive index
		ARC_TO_RESCAN,	/// File contents need reindexing in the archive
		ARC_DELETED,	/// File does not exist, but has entries in the archive index
		STATE_MAX
	};

	virtual ~MaintFileVisitor() {}

	virtual void operator()(const std::string& file, State state) = 0;
};

/**
 * Visitor interface for scanning information about the files indexed in the database
 */
struct IndexFileVisitor
{
	virtual ~IndexFileVisitor() {}

	virtual void operator()(const std::string& file, int id, off_t offset, size_t size) = 0;
};

/**
 * IndexFileVisitor that feeds a MaintFileVisitor with OK or HOLES status.
 *
 * TO_INDEX and TO_RESCAN will have to be detected by MaintFileVisitors
 * further down the chain.
 */
struct HoleFinder : IndexFileVisitor
{
	MaintFileVisitor& next;

	const std::string& m_root;

	std::string last_file;
	off_t last_file_size;
	bool quick;
	const scan::Validator* validator;
	int validator_fd;
	bool has_hole;
	bool is_corrupted;

	HoleFinder(MaintFileVisitor& next, const std::string& root, bool quick=true);

	void finaliseFile();

	void operator()(const std::string& file, int id, off_t offset, size_t size);

	void end()
	{
		finaliseFile();
	}
};

/**
 * MaintFileVisitor that feeds a MaintFileVisitor with TO_INDEX status.
 *
 * The input feed is assumed to come from the index, and is checked against the
 * files found on disk in order to detect files that are on disk but not in the
 * index.
 */
struct FindMissing : public MaintFileVisitor
{
	MaintFileVisitor& next;
	std::vector<std::string> disk;

	FindMissing(MaintFileVisitor& next, const std::vector<std::string>& files);

	// files: a, b, c,    e, f, g
	// index:       c, d, e, f, g

	void operator()(const std::string& file, State state);
	void end();
};

/**
 * Print out the maintenance state for each file
 */
struct MaintPrinter : public MaintFileVisitor
{
	void operator()(const std::string& file, State state);
};

}
}
}

// vim:set ts=4 sw=4:
#endif
