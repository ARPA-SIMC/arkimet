#ifndef ARKI_DATASET_MAINTENANCE_H
#define ARKI_DATASET_MAINTENANCE_H

/*
 * dataset/maintenance - Dataset maintenance utilities
 *
 * Copyright (C) 2007--2014  ARPA-SIM <urpsim@smr.arpa.emr.it>
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
#include <sys/types.h>

namespace arki {

namespace metadata {
class Collection;
}

namespace scan {
struct Validator;
}

namespace data {
struct Info;
}

namespace dataset {
class WritableLocal;

namespace maintenance {

/**
 * Visitor interface for scanning information about the files indexed in the database
 */
struct MaintFileVisitor
{
    static const unsigned OK         = 0;
    static const unsigned TO_ARCHIVE = 1 << 0; /// File is ok, but old enough to be archived
    static const unsigned TO_DELETE  = 1 << 1; /// File is ok, but old enough to be deleted
    static const unsigned TO_PACK    = 1 << 2; /// File contains data that has been deleted
    static const unsigned TO_INDEX   = 1 << 3; /// File is not present in the index
    static const unsigned TO_RESCAN  = 1 << 4; /// File contents are inconsistent with the index
    static const unsigned TO_DEINDEX = 1 << 5; /// File does not exist but has entries in the index
    static const unsigned ARCHIVED   = 1 << 6; /// File is in the archive

    virtual ~MaintFileVisitor() {}

    virtual void operator()(const std::string& file, unsigned state) = 0;
};

/**
 * Visitor interface for scanning information about the files indexed in the database
 */
struct IndexFileVisitor
{
	virtual ~IndexFileVisitor() {}

    virtual void operator()(const std::string& file, const metadata::Collection& mdc) = 0;
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
    bool quick;

	HoleFinder(MaintFileVisitor& next, const std::string& root, bool quick=true);

	/**
	 * Scan the file using scan::scan, check for holes and (optionally)
	 * corruption, then report the result to the chained visitor
	 */
	void scan(const std::string& file);

    void operator()(const std::string& file, const metadata::Collection& mdc);
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

	void operator()(const std::string& file, unsigned state);
	void end();
};

/**
 * Print out the maintenance state for each file
 */
struct Dumper : public MaintFileVisitor
{
	void operator()(const std::string& file, unsigned state);
};

struct Tee : public MaintFileVisitor
{
	MaintFileVisitor& one;
	MaintFileVisitor& two;

	Tee(MaintFileVisitor& one, MaintFileVisitor& two);
	virtual ~Tee();
	void operator()(const std::string& file, unsigned state);
};

/// Base class for all repackers and rebuilders
struct Agent : public maintenance::MaintFileVisitor
{
	std::ostream& m_log;
	WritableLocal& w;
	bool lineStart;

	Agent(std::ostream& log, WritableLocal& w);

	std::ostream& log();

	// Start a line with multiple items logged
	void logStart();
	// Log another item on the current line
	std::ostream& logAdd();
	// End the line with multiple things logged
	void logEnd();

	virtual void end() {}
};

/**
 * Repacker used when some failsafe is triggered.
 * 
 * It only reports how many files would be deleted.
 */
struct FailsafeRepacker : public Agent
{
	size_t m_count_deleted;

	FailsafeRepacker(std::ostream& log, WritableLocal& w);

	void operator()(const std::string& file, unsigned state);
	void end();
};

/**
 * Simulate a repack and print information on what would have been done
 */
struct MockRepacker : public Agent
{
	size_t m_count_packed;
	size_t m_count_archived;
	size_t m_count_deleted;
	size_t m_count_deindexed;
	size_t m_count_rescanned;

	MockRepacker(std::ostream& log, WritableLocal& w);

	void operator()(const std::string& file, unsigned state);
	void end();
};

/**
 * Simulate a repack and print information on what would have been done
 */
struct MockFixer : public Agent
{
	size_t m_count_packed;
	size_t m_count_rescanned;
	size_t m_count_deindexed;

	MockFixer(std::ostream& log, WritableLocal& w);

	void operator()(const std::string& file, unsigned state);
	void end();
};

/**
 * Perform real repacking
 */
struct RealRepacker : public maintenance::Agent
{
	size_t m_count_packed;
	size_t m_count_archived;
	size_t m_count_deleted;
	size_t m_count_deindexed;
	size_t m_count_rescanned;
	size_t m_count_freed;
	bool m_touched_archive;
	bool m_redo_summary;

	RealRepacker(std::ostream& log, WritableLocal& w);

	void operator()(const std::string& file, unsigned state);
	void end();
};

/**
 * Perform real repacking
 */
struct RealFixer : public maintenance::Agent
{
	size_t m_count_packed;
	size_t m_count_rescanned;
	size_t m_count_deindexed;
	bool m_touched_archive;
	bool m_redo_summary;

	RealFixer(std::ostream& log, WritableLocal& w);

	void operator()(const std::string& file, unsigned state);
	void end();
};

}
}
}

// vim:set ts=4 sw=4:
#endif
