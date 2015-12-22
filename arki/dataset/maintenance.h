#ifndef ARKI_DATASET_MAINTENANCE_H
#define ARKI_DATASET_MAINTENANCE_H

/// dataset/maintenance - Dataset maintenance utilities

#include <arki/dataset/data.h>
#include <arki/types/time.h>
#include <string>
#include <vector>
#include <sys/types.h>

namespace arki {

namespace metadata {
class Collection;
}

namespace scan {
class Validator;
}

namespace dataset {
class SegmentedWriter;
class TargetFile;

namespace maintenance {

/**
 * Visitor interface for scanning information about the files indexed in the database
 */
struct MaintFileVisitor
{
    virtual ~MaintFileVisitor() {}

    virtual void operator()(const std::string& file, data::FileState state) = 0;
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

	void operator()(const std::string& file, data::FileState state);
	void end();
};

/**
 * Check the age of files in a dataset, to detect those that need to be deleted
 * or archived. It adds FILE_TO_DELETE or FILE_TO_ARCHIVE as needed.
 */
struct CheckAge : public MaintFileVisitor
{
    MaintFileVisitor& next;
    const TargetFile& tf;
    types::Time archive_threshold;
    types::Time delete_threshold;

    CheckAge(MaintFileVisitor& next, const TargetFile& tf, int archive_age=-1, int delete_age=-1);

    void operator()(const std::string& file, data::FileState state);
};

/**
 * Print out the maintenance state for each file
 */
struct Dumper : public MaintFileVisitor
{
	void operator()(const std::string& file, data::FileState state);
};

struct Tee : public MaintFileVisitor
{
	MaintFileVisitor& one;
	MaintFileVisitor& two;

	Tee(MaintFileVisitor& one, MaintFileVisitor& two);
	virtual ~Tee();
	void operator()(const std::string& file, data::FileState state);
};

/// Base class for all repackers and rebuilders
struct Agent : public maintenance::MaintFileVisitor
{
	std::ostream& m_log;
	SegmentedWriter& w;
	bool lineStart;

	Agent(std::ostream& log, SegmentedWriter& w);

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

	FailsafeRepacker(std::ostream& log, SegmentedWriter& w);

	void operator()(const std::string& file, data::FileState state);
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

	MockRepacker(std::ostream& log, SegmentedWriter& w);

	void operator()(const std::string& file, data::FileState state);
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

	MockFixer(std::ostream& log, SegmentedWriter& w);

	void operator()(const std::string& file, data::FileState state);
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

	RealRepacker(std::ostream& log, SegmentedWriter& w);

	void operator()(const std::string& file, data::FileState state);
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

	RealFixer(std::ostream& log, SegmentedWriter& w);

	void operator()(const std::string& file, data::FileState state);
	void end();
};

}
}
}

// vim:set ts=4 sw=4:
#endif
