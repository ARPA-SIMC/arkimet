#ifndef ARKI_DATASET_REPORTER_H
#define ARKI_DATASET_REPORTER_H

#include <string>
#include <iosfwd>

namespace arki {
namespace dataset {

/**
 * Interface for notifying the progress and results of check and repack
 * activities for dataset Checkers
 */
struct Reporter
{
    virtual ~Reporter();

    virtual void operation_progress(const std::string& ds, const std::string& operation, const std::string& message) = 0;
    virtual void operation_manual_intervention(const std::string& ds, const std::string& operation, const std::string& message) = 0;
    virtual void operation_aborted(const std::string& ds, const std::string& operation, const std::string& message) = 0;
    virtual void operation_report(const std::string& ds, const std::string& operation, const std::string& message) = 0;
    virtual void segment_info(const std::string& ds, const std::string& relpath, const std::string& message) = 0;
    virtual void segment_repack(const std::string& ds, const std::string& relpath, const std::string& message) = 0;
    virtual void segment_archive(const std::string& ds, const std::string& relpath, const std::string& message) = 0;
    virtual void segment_delete(const std::string& ds, const std::string& relpath, const std::string& message) = 0;
    virtual void segment_deindex(const std::string& ds, const std::string& relpath, const std::string& message) = 0;
    virtual void segment_rescan(const std::string& ds, const std::string& relpath, const std::string& message) = 0;
    virtual void segment_issue51(const std::string& ds, const std::string& relpath, const std::string& message) = 0;
};

struct NullReporter : public Reporter
{
    void operation_progress(const std::string& ds, const std::string& operation, const std::string& message) override {}
    void operation_manual_intervention(const std::string& ds, const std::string& operation, const std::string& message) override {}
    void operation_aborted(const std::string& ds, const std::string& operation, const std::string& message) override {}
    void operation_report(const std::string& ds, const std::string& operation, const std::string& message) override {}
    void segment_info(const std::string& ds, const std::string& relpath, const std::string& message) override {}
    void segment_repack(const std::string& ds, const std::string& relpath, const std::string& message) override {}
    void segment_archive(const std::string& ds, const std::string& relpath, const std::string& message) override {}
    void segment_delete(const std::string& ds, const std::string& relpath, const std::string& message) override {}
    void segment_deindex(const std::string& ds, const std::string& relpath, const std::string& message) override {}
    void segment_rescan(const std::string& ds, const std::string& relpath, const std::string& message) override {}
    void segment_issue51(const std::string& ds, const std::string& relpath, const std::string& message) override {}
};

struct OstreamReporter : public Reporter
{
    std::ostream& out;

    OstreamReporter(std::ostream& out);

    void operation_progress(const std::string& ds, const std::string& operation, const std::string& message) override;
    void operation_manual_intervention(const std::string& ds, const std::string& operation, const std::string& message) override;
    void operation_aborted(const std::string& ds, const std::string& operation, const std::string& message) override;
    void operation_report(const std::string& ds, const std::string& operation, const std::string& message) override;
    void segment_info(const std::string& ds, const std::string& relpath, const std::string& message) override;
    void segment_repack(const std::string& ds, const std::string& relpath, const std::string& message) override;
    void segment_archive(const std::string& ds, const std::string& relpath, const std::string& message) override;
    void segment_delete(const std::string& ds, const std::string& relpath, const std::string& message) override;
    void segment_deindex(const std::string& ds, const std::string& relpath, const std::string& message) override;
    void segment_rescan(const std::string& ds, const std::string& relpath, const std::string& message) override;
    void segment_issue51(const std::string& ds, const std::string& relpath, const std::string& message) override;
};

}
}
#endif
