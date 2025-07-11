#ifndef ARKI_DATASET_ARCHIVE_H
#define ARKI_DATASET_ARCHIVE_H

/// Handle archived data

#include <arki/core/fwd.h>
#include <arki/dataset.h>
#include <arki/dataset/fwd.h>
#include <arki/dataset/impl.h>
#include <arki/matcher/fwd.h>
#include <arki/metadata/fwd.h>
#include <arki/segment/fwd.h>
#include <arki/summary.h>
#include <string>

namespace arki {
namespace dataset {

namespace segmented {
class Checker;
class CheckerSegment;
} // namespace segmented

namespace archive {

class ArchivesReaderRoot;
class ArchivesCheckerRoot;

bool is_archive(const std::filesystem::path& dir);

class Dataset : public dataset::Dataset
{
public:
    std::filesystem::path root;
    std::shared_ptr<segment::Session> segment_session;
    std::string step_name;

    Dataset(std::shared_ptr<Session> session, const std::filesystem::path& root,
            const std::string& step_name);

    std::shared_ptr<dataset::Reader> create_reader() override;
    std::shared_ptr<dataset::Checker> create_checker() override;
};

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
 * Dataset maintenance will therefore only write files older than "archive age"
 * to the "last" archive.
 *
 * When doing archive maintenance, all archives are checked. If a file is
 * manually moved from an archive to another, it will be properly deindexed and
 * reindexed during the archive maintenance.
 *
 * When querying, all archives are queried, following the archive order:
 * alphabetical order except the archive named "last" is queried last.
 */
class Reader : public DatasetAccess<Dataset, dataset::Reader>
{
protected:
    archive::ArchivesReaderRoot* archives = nullptr;

    void summary_for_all(Summary& out);

    bool impl_query_data(const query::Data& q, metadata_dest_func) override;
    void impl_query_summary(const Matcher& matcher, Summary& summary) override;
    void impl_stream_query_bytes(const query::Bytes& q,
                                 StreamOutput& out) override;

public:
    Reader(std::shared_ptr<Dataset> dataset);
    virtual ~Reader();

    std::string type() const override;

    core::Interval get_stored_time_interval() override;

    /// Return the number of archives found, used for testing
    unsigned test_count_archives() const;
};

class Checker : public DatasetAccess<Dataset, dataset::Checker>
{
protected:
    archive::ArchivesCheckerRoot* archives = nullptr;

public:
    /// Create an archive for the dataset at the given root dir.
    Checker(std::shared_ptr<Dataset> dataset);
    virtual ~Checker();

    std::string type() const override;

    void index_segment(const std::filesystem::path& relpath,
                       metadata::Collection&& mds);
    /**
     * Deindex the segment at relpath and move it to new_relpath in
     * segment_session.
     *
     * Only the data part of the segment is moved. The segment metadata is
     * returned so it can be reindexed
     */
    arki::metadata::Collection
    release_segment(const std::filesystem::path& relpath,
                    std::shared_ptr<const segment::Session> segment_session,
                    const std::filesystem::path& new_relpath);
    void segments_recursive(
        CheckerConfig& opts,
        std::function<void(segmented::Checker&, segmented::CheckerSegment&)>
            dest);

    void remove_old(CheckerConfig& opts) override;
    void remove_all(CheckerConfig& opts) override;
    void repack(CheckerConfig& opts, unsigned test_flags = 0) override;
    void check(CheckerConfig& opts) override;
    void check_issue51(CheckerConfig& opts) override;
    void tar(CheckerConfig& config) override;
    void zip(CheckerConfig& config) override;
    void compress(CheckerConfig& config, unsigned groupsize) override;
    void state(CheckerConfig& config) override;

    /// Return the number of archives found, used for testing
    unsigned test_count_archives() const;
};

} // namespace archive
} // namespace dataset
} // namespace arki

#endif
