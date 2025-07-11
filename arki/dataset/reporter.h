#ifndef ARKI_DATASET_REPORTER_H
#define ARKI_DATASET_REPORTER_H

#include <arki/segment/reporter.h>
#include <filesystem>
#include <iosfwd>
#include <string>

namespace arki::dataset {

/**
 * Interface for notifying the progress and results of check and repack
 * activities for dataset Checkers
 */
class Reporter
{
public:
    virtual ~Reporter();

    virtual void operation_progress(const std::string& ds,
                                    const std::string& operation,
                                    const std::string& message)            = 0;
    virtual void operation_manual_intervention(const std::string& ds,
                                               const std::string& operation,
                                               const std::string& message) = 0;
    virtual void operation_aborted(const std::string& ds,
                                   const std::string& operation,
                                   const std::string& message)             = 0;
    virtual void operation_report(const std::string& ds,
                                  const std::string& operation,
                                  const std::string& message)              = 0;
    virtual void segment_info(const std::string& ds,
                              const std::filesystem::path& relpath,
                              const std::string& message)                  = 0;
    virtual void segment_repack(const std::string& ds,
                                const std::filesystem::path& relpath,
                                const std::string& message)                = 0;
    virtual void segment_archive(const std::string& ds,
                                 const std::filesystem::path& relpath,
                                 const std::string& message)               = 0;
    virtual void segment_remove(const std::string& ds,
                                const std::filesystem::path& relpath,
                                const std::string& message)                = 0;
    virtual void segment_deindex(const std::string& ds,
                                 const std::filesystem::path& relpath,
                                 const std::string& message)               = 0;
    virtual void segment_rescan(const std::string& ds,
                                const std::filesystem::path& relpath,
                                const std::string& message)                = 0;
    virtual void segment_tar(const std::string& ds,
                             const std::filesystem::path& relpath,
                             const std::string& message)                   = 0;
    virtual void segment_compress(const std::string& ds,
                                  const std::filesystem::path& relpath,
                                  const std::string& message)              = 0;
    virtual void segment_issue51(const std::string& ds,
                                 const std::filesystem::path& relpath,
                                 const std::string& message)               = 0;
    virtual void
    segment_manual_intervention(const std::string& ds,
                                const std::filesystem::path& relpath,
                                const std::string& message) = 0;

    /**
     * Create a segment reporter that proxies to this reporter
     */
    std::unique_ptr<segment::Reporter> segment_reporter(const std::string& ds);
};

class NullReporter : public Reporter
{
public:
    void operation_progress(const std::string&, const std::string&,
                            const std::string&) override
    {
    }
    void operation_manual_intervention(const std::string&, const std::string&,
                                       const std::string&) override
    {
    }
    void operation_aborted(const std::string&, const std::string&,
                           const std::string&) override
    {
    }
    void operation_report(const std::string&, const std::string&,
                          const std::string&) override
    {
    }
    void segment_info(const std::string&, const std::filesystem::path&,
                      const std::string&) override
    {
    }
    void segment_repack(const std::string&, const std::filesystem::path&,
                        const std::string&) override
    {
    }
    void segment_archive(const std::string&, const std::filesystem::path&,
                         const std::string&) override
    {
    }
    void segment_remove(const std::string&, const std::filesystem::path&,
                        const std::string&) override
    {
    }
    void segment_deindex(const std::string&, const std::filesystem::path&,
                         const std::string&) override
    {
    }
    void segment_rescan(const std::string&, const std::filesystem::path&,
                        const std::string&) override
    {
    }
    void segment_tar(const std::string&, const std::filesystem::path&,
                     const std::string&) override
    {
    }
    void segment_compress(const std::string&, const std::filesystem::path&,
                          const std::string&) override
    {
    }
    void segment_issue51(const std::string&, const std::filesystem::path&,
                         const std::string&) override
    {
    }
    void segment_manual_intervention(const std::string&,
                                     const std::filesystem::path&,
                                     const std::string&) override
    {
    }
};

class OstreamReporter : public Reporter
{
public:
    std::ostream& out;

    OstreamReporter(std::ostream& out);

    void operation_progress(const std::string& ds, const std::string& operation,
                            const std::string& message) override;
    void operation_manual_intervention(const std::string& ds,
                                       const std::string& operation,
                                       const std::string& message) override;
    void operation_aborted(const std::string& ds, const std::string& operation,
                           const std::string& message) override;
    void operation_report(const std::string& ds, const std::string& operation,
                          const std::string& message) override;
    void segment_info(const std::string& ds,
                      const std::filesystem::path& relpath,
                      const std::string& message) override;
    void segment_repack(const std::string& ds,
                        const std::filesystem::path& relpath,
                        const std::string& message) override;
    void segment_archive(const std::string& ds,
                         const std::filesystem::path& relpath,
                         const std::string& message) override;
    void segment_remove(const std::string& ds,
                        const std::filesystem::path& relpath,
                        const std::string& message) override;
    void segment_deindex(const std::string& ds,
                         const std::filesystem::path& relpath,
                         const std::string& message) override;
    void segment_rescan(const std::string& ds,
                        const std::filesystem::path& relpath,
                        const std::string& message) override;
    void segment_tar(const std::string& ds,
                     const std::filesystem::path& relpath,
                     const std::string& message) override;
    void segment_compress(const std::string& ds,
                          const std::filesystem::path& relpath,
                          const std::string& message) override;
    void segment_issue51(const std::string& ds,
                         const std::filesystem::path& relpath,
                         const std::string& message) override;
    void segment_manual_intervention(const std::string& ds,
                                     const std::filesystem::path& relpath,
                                     const std::string& message) override;
};

} // namespace arki::dataset
#endif
