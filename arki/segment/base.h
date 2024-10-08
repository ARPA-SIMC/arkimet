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
    BaseReader(const std::string& format, const std::filesystem::path& root, const std::filesystem::path& relpath, const std::filesystem::path& abspath, std::shared_ptr<core::Lock> lock)
        : segment::Reader(lock), m_segment(format, root, relpath, abspath)
    {
    }

    const Segment& segment() const override { return m_segment; }
};

template<typename Segment>
struct BaseWriter : public segment::Writer
{
protected:
    Segment m_segment;

public:
    BaseWriter(const WriterConfig& config, const std::string& format, const std::filesystem::path& root, const std::filesystem::path& relpath, const std::filesystem::path& abspath)
        : segment::Writer(config), m_segment(format, root, relpath, abspath)
    {
    }

    const Segment& segment() const override { return m_segment; }
};

template<typename Segment>
struct BaseChecker : public segment::Checker
{
protected:
    Segment m_segment;

public:
    BaseChecker(const std::string& format, const std::filesystem::path& root, const std::filesystem::path& relpath, const std::filesystem::path& abspath)
        : m_segment(format, root, relpath, abspath)
    {
    }

    const Segment& segment() const override { return m_segment; }
    // std::shared_ptr<segment::Checker> checker_moved(const std::string& new_root, const std::string& new_relpath, const std::string& new_abspath) const override;

    std::shared_ptr<Checker> move(const std::filesystem::path& new_root, const std::filesystem::path& new_relpath, const std::filesystem::path& new_abspath) override;
};

}
}

#endif
