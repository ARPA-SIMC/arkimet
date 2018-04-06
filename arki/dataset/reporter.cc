#include "reporter.h"
#include <ostream>

using namespace std;

namespace arki {
namespace dataset {

Reporter::~Reporter()
{
}

OstreamReporter::OstreamReporter(std::ostream& out) : out(out) {}

void OstreamReporter::operation_progress(const std::string& ds, const std::string& operation, const std::string& message)
{
    out << ds << ": " << operation << ": " << message << endl;
}

void OstreamReporter::operation_manual_intervention(const std::string& ds, const std::string& operation, const std::string& message)
{
    out << ds << ": " << operation << " manual intervention required: " << message << endl;
}

void OstreamReporter::operation_aborted(const std::string& ds, const std::string& operation, const std::string& message)
{
    out << ds << ": " << operation << " aborted: " << message << endl;
}

void OstreamReporter::operation_report(const std::string& ds, const std::string& operation, const std::string& message)
{
    out << ds << ": " << operation << " " << message << endl;
}

void OstreamReporter::segment_info(const std::string& ds, const std::string& relpath, const std::string& message)
{
    out << ds << ":" << relpath << ": " << message << endl;
}

void OstreamReporter::segment_repack(const std::string& ds, const std::string& relpath, const std::string& message)
{
    out << ds << ":" << relpath << ": " << message << endl;
}

void OstreamReporter::segment_archive(const std::string& ds, const std::string& relpath, const std::string& message)
{
    out << ds << ":" << relpath << ": " << message << endl;
}

void OstreamReporter::segment_delete(const std::string& ds, const std::string& relpath, const std::string& message)
{
    out << ds << ":" << relpath << ": " << message << endl;
}

void OstreamReporter::segment_deindex(const std::string& ds, const std::string& relpath, const std::string& message)
{
    out << ds << ":" << relpath << ": " << message << endl;
}

void OstreamReporter::segment_rescan(const std::string& ds, const std::string& relpath, const std::string& message)
{
    out << ds << ":" << relpath << ": " << message << endl;
}

void OstreamReporter::segment_tar(const std::string& ds, const std::string& relpath, const std::string& message)
{
    out << ds << ":" << relpath << ": " << message << endl;
}

void OstreamReporter::segment_compress(const std::string& ds, const std::string& relpath, const std::string& message)
{
    out << ds << ":" << relpath << ": " << message << endl;
}

void OstreamReporter::segment_issue51(const std::string& ds, const std::string& relpath, const std::string& message)
{
    out << ds << ":" << relpath << ": " << message << endl;
}

}
}
