#ifndef ARKI_DATASET_ARCHIVE_H
#define ARKI_DATASET_ARCHIVE_H

/// Handle archived data

#include <arki/dataset.h>
#include <arki/summary.h>
#include <string>
#include <map>
#include <iosfwd>

namespace arki {
class Summary;
class Matcher;

namespace metadata {
class Collection;
}

namespace dataset {
class DataQuery;
class ByteQuery;

namespace index {
class Manifest;
}

namespace simple {
class Reader;
}

namespace archive {

class ArchivesReaderRoot;
class ArchivesCheckerRoot;

bool is_archive(const std::string& dir);

}

/**
 * Set of archives.
 *
 * Every archive is a subdirectory. Subdirectories are always considered in
 * alphabetical order, except for a subdirectory named "last". The subdirectory
 * named "last" if it exists it always appears last.
 *
 * The idea is to have one archive called "last" where data is automatically
 * archived from the live dataset, and other archives maintained manually by
 * moving files from "last" into them.
 *
 * ondisk2 dataset maintenance will therefore only write files older than
 * "archive age" to the "last" archive.
 *
 * When doing archive maintenance, all archives are checked. If a file is
 * manually moved from an archive to another, it will be properly deindexed and
 * reindexed during the archive maintenance.
 *
 * When querying, all archives are queried, following the archive order:
 * alphabetical order except the archive named "last" is queried last.
 */
class ArchivesReader : public Reader
{
protected:
    archive::ArchivesReaderRoot* archives = nullptr;

    void summary_for_all(Summary& out);

public:
    ArchivesReader(const std::string& root);
    virtual ~ArchivesReader();

    std::string type() const override;

    void expand_date_range(std::unique_ptr<types::Time>& begin, std::unique_ptr<types::Time>& end) const;
    void query_data(const dataset::DataQuery& q, metadata_dest_func) override;
    void query_bytes(const dataset::ByteQuery& q, int out) override;
    void query_summary(const Matcher& matcher, Summary& summary) override;
};

class ArchivesChecker : public Checker
{
protected:
    archive::ArchivesCheckerRoot* archives = nullptr;

public:
    /// Create an archive for the dataset at the given root dir.
    ArchivesChecker(const std::string& root);
    virtual ~ArchivesChecker();

    std::string type() const override;

    //void indexFile(const std::string& relname);
    void indexFile(const std::string& relname, metadata::Collection&& mds);

    void removeAll(dataset::Reporter& reporter, bool writable=false) override;
    void repack(dataset::Reporter& reporter, bool writable=false) override;
    void check(dataset::Reporter& reporter, bool fix, bool quick) override;
};

}
}

#endif
