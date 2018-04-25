#ifndef ARKI_SEGMENT_BASE_H
#define ARKI_SEGMENT_BASE_H

#include <arki/segment.h>
#include <arki/types/fwd.h>
#include <arki/metadata/fwd.h>
#include <arki/scan/fwd.h>
#include <string>
#include <vector>
#include <functional>

namespace arki {
namespace segment {

template<typename Segment>
struct BaseReader : public segment::Reader
{
protected:
    Segment m_segment;

public:
    BaseReader(const std::string& format, const std::string& root, const std::string& relpath, const std::string& abspath, std::shared_ptr<core::Lock> lock)
        : segment::Reader(lock), m_segment(format, root, relpath, abspath)
    {
    }

    const Segment& segment() const override { return m_segment; }
};

template<typename Segment>
struct BaseWriter : public segment::Writer
{
};

template<typename Segment>
struct BaseChecker : public segment::Checker
{
    // std::shared_ptr<segment::Checker> checker_moved(const std::string& new_root, const std::string& new_relpath, const std::string& new_abspath) const override;
};

}
}

#endif
