#ifndef ARKI_SEGMENT_REPORTER_H
#define ARKI_SEGMENT_REPORTER_H

#include <arki/segment.h>
#include <filesystem>
#include <iosfwd>
#include <string>

namespace arki::segment {

/**
 * Interface for notifying the progress and results of check and repack
 * activities for dataset Checkers
 */
class Reporter
{
public:
    virtual ~Reporter();

    virtual void info(const arki::Segment& segment,
                      const std::string& message)                = 0;
    virtual void repack(const arki::Segment& segment,
                        const std::string& message)              = 0;
    virtual void archive(const arki::Segment& segment,
                         const std::string& message)             = 0;
    virtual void remove(const arki::Segment& segment,
                        const std::string& message)              = 0;
    virtual void deindex(const arki::Segment& segment,
                         const std::string& message)             = 0;
    virtual void rescan(const arki::Segment& segment,
                        const std::string& message)              = 0;
    virtual void tar(const arki::Segment& segment,
                     const std::string& message)                 = 0;
    virtual void compress(const arki::Segment& segment,
                          const std::string& message)            = 0;
    virtual void issue51(const arki::Segment& segment,
                         const std::string& message)             = 0;
    virtual void manual_intervention(const arki::Segment& segment,
                                     const std::string& message) = 0;
};

class NullReporter : public Reporter
{
public:
    void info(const arki::Segment&, const std::string&) override {}
    void repack(const arki::Segment&, const std::string&) override {}
    void archive(const arki::Segment&, const std::string&) override {}
    void remove(const arki::Segment&, const std::string&) override {}
    void deindex(const arki::Segment&, const std::string&) override {}
    void rescan(const arki::Segment&, const std::string&) override {}
    void tar(const arki::Segment&, const std::string&) override {}
    void compress(const arki::Segment&, const std::string&) override {}
    void issue51(const arki::Segment&, const std::string&) override {}
    void manual_intervention(const arki::Segment&, const std::string&) override
    {
    }
};

class OstreamReporter : public Reporter
{
public:
    std::ostream& out;

    OstreamReporter(std::ostream& out);

    void info(const arki::Segment& segment,
              const std::string& message) override;
    void repack(const arki::Segment& segment,
                const std::string& message) override;
    void archive(const arki::Segment& segment,
                 const std::string& message) override;
    void remove(const arki::Segment& segment,
                const std::string& message) override;
    void deindex(const arki::Segment& segment,
                 const std::string& message) override;
    void rescan(const arki::Segment& segment,
                const std::string& message) override;
    void tar(const arki::Segment& segment, const std::string& message) override;
    void compress(const arki::Segment& segment,
                  const std::string& message) override;
    void issue51(const arki::Segment& segment,
                 const std::string& message) override;
    void manual_intervention(const arki::Segment& segment,
                             const std::string& message) override;
};

} // namespace arki::segment
#endif
