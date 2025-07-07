#include "reporter.h"
#include <ostream>

using namespace std;

namespace arki {
namespace segment {

Reporter::~Reporter() {}

OstreamReporter::OstreamReporter(std::ostream& out) : out(out) {}

void OstreamReporter::info(const Segment& segment, const std::string& message)
{
    out << segment.abspath() << ": " << message << endl;
}

void OstreamReporter::repack(const Segment& segment, const std::string& message)
{
    out << segment.abspath() << ": " << message << endl;
}

void OstreamReporter::archive(const Segment& segment,
                              const std::string& message)
{
    out << segment.abspath() << ": " << message << endl;
}

void OstreamReporter::remove(const Segment& segment, const std::string& message)
{
    out << segment.abspath() << ": " << message << endl;
}

void OstreamReporter::deindex(const Segment& segment,
                              const std::string& message)
{
    out << segment.abspath() << ": " << message << endl;
}

void OstreamReporter::rescan(const Segment& segment, const std::string& message)
{
    out << segment.abspath() << ": " << message << endl;
}

void OstreamReporter::tar(const Segment& segment, const std::string& message)
{
    out << segment.abspath() << ": " << message << endl;
}

void OstreamReporter::compress(const Segment& segment,
                               const std::string& message)
{
    out << segment.abspath() << ": " << message << endl;
}

void OstreamReporter::issue51(const Segment& segment,
                              const std::string& message)
{
    out << segment.abspath() << ": " << message << endl;
}

void OstreamReporter::manual_intervention(const Segment& segment,
                                          const std::string& message)
{
    out << segment.abspath() << ": " << message << endl;
}

} // namespace segment
} // namespace arki
