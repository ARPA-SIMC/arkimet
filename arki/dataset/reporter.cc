#include "reporter.h"
#include <ostream>

using namespace std;

namespace arki {
namespace dataset {

namespace {
class SegmentReporterAdapter : public segment::Reporter
{
    dataset::Reporter& reporter;
    std::string ds;

public:
    SegmentReporterAdapter(dataset::Reporter& reporter, const std::string& ds)
        : reporter(reporter), ds(ds)
    {
    }

    void info(const arki::Segment& segment, const std::string& message) override
    {
        reporter.segment_info(ds, segment.relpath(), message);
    }
    void repack(const arki::Segment& segment,
                const std::string& message) override
    {
        reporter.segment_repack(ds, segment.relpath(), message);
    }
    void archive(const arki::Segment& segment,
                 const std::string& message) override
    {
        reporter.segment_archive(ds, segment.relpath(), message);
    }
    void remove(const arki::Segment& segment,
                const std::string& message) override
    {
        reporter.segment_remove(ds, segment.relpath(), message);
    }
    void deindex(const arki::Segment& segment,
                 const std::string& message) override
    {
        reporter.segment_deindex(ds, segment.relpath(), message);
    }
    void rescan(const arki::Segment& segment,
                const std::string& message) override
    {
        reporter.segment_rescan(ds, segment.relpath(), message);
    }
    void tar(const arki::Segment& segment, const std::string& message) override
    {
        reporter.segment_tar(ds, segment.relpath(), message);
    }
    void compress(const arki::Segment& segment,
                  const std::string& message) override
    {
        reporter.segment_compress(ds, segment.relpath(), message);
    }
    void issue51(const arki::Segment& segment,
                 const std::string& message) override
    {
        reporter.segment_issue51(ds, segment.relpath(), message);
    }
    void manual_intervention(const arki::Segment& segment,
                             const std::string& message) override
    {
        reporter.segment_manual_intervention(ds, segment.relpath(), message);
    }
};
} // namespace

std::unique_ptr<segment::Reporter>
Reporter::segment_reporter(const std::string& ds)
{
    return std::make_unique<SegmentReporterAdapter>(*this, ds);
}

Reporter::~Reporter() {}

OstreamReporter::OstreamReporter(std::ostream& out) : out(out) {}

void OstreamReporter::operation_progress(const std::string& ds,
                                         const std::string& operation,
                                         const std::string& message)
{
    out << ds << ": " << operation << ": " << message << endl;
}

void OstreamReporter::operation_manual_intervention(
    const std::string& ds, const std::string& operation,
    const std::string& message)
{
    out << ds << ": " << operation
        << " manual intervention required: " << message << endl;
}

void OstreamReporter::operation_aborted(const std::string& ds,
                                        const std::string& operation,
                                        const std::string& message)
{
    out << ds << ": " << operation << " aborted: " << message << endl;
}

void OstreamReporter::operation_report(const std::string& ds,
                                       const std::string& operation,
                                       const std::string& message)
{
    out << ds << ": " << operation << " " << message << endl;
}

void OstreamReporter::segment_info(const std::string& ds,
                                   const std::filesystem::path& relpath,
                                   const std::string& message)
{
    out << ds << ":" << relpath.native() << ": " << message << endl;
}

void OstreamReporter::segment_repack(const std::string& ds,
                                     const std::filesystem::path& relpath,
                                     const std::string& message)
{
    out << ds << ":" << relpath.native() << ": " << message << endl;
}

void OstreamReporter::segment_archive(const std::string& ds,
                                      const std::filesystem::path& relpath,
                                      const std::string& message)
{
    out << ds << ":" << relpath.native() << ": " << message << endl;
}

void OstreamReporter::segment_remove(const std::string& ds,
                                     const std::filesystem::path& relpath,
                                     const std::string& message)
{
    out << ds << ":" << relpath.native() << ": " << message << endl;
}

void OstreamReporter::segment_deindex(const std::string& ds,
                                      const std::filesystem::path& relpath,
                                      const std::string& message)
{
    out << ds << ":" << relpath.native() << ": " << message << endl;
}

void OstreamReporter::segment_rescan(const std::string& ds,
                                     const std::filesystem::path& relpath,
                                     const std::string& message)
{
    out << ds << ":" << relpath.native() << ": " << message << endl;
}

void OstreamReporter::segment_tar(const std::string& ds,
                                  const std::filesystem::path& relpath,
                                  const std::string& message)
{
    out << ds << ":" << relpath.native() << ": " << message << endl;
}

void OstreamReporter::segment_compress(const std::string& ds,
                                       const std::filesystem::path& relpath,
                                       const std::string& message)
{
    out << ds << ":" << relpath.native() << ": " << message << endl;
}

void OstreamReporter::segment_issue51(const std::string& ds,
                                      const std::filesystem::path& relpath,
                                      const std::string& message)
{
    out << ds << ":" << relpath.native() << ": " << message << endl;
}

void OstreamReporter::segment_manual_intervention(
    const std::string& ds, const std::filesystem::path& relpath,
    const std::string& message)
{
    out << ds << ":" << relpath.native() << ": " << message << endl;
}

} // namespace dataset
} // namespace arki
