#ifndef ARKI_SEGMENT_H
#define ARKI_SEGMENT_H

#include <filesystem>
#include <memory>
#include <string>
#include <arki/segment/fwd.h>
#include <arki/segment/session.h>

namespace arki {

class Segment : public std::enable_shared_from_this<Segment>
{
// protected:
//     std::shared_ptr<segment::Session> session;

public:
    std::shared_ptr<segment::Session> session;
    DataFormat format;
    std::filesystem::path root;
    std::filesystem::path relpath;
    std::filesystem::path abspath;

    Segment(std::shared_ptr<segment::Session> session, DataFormat format, const std::filesystem::path& root, const std::filesystem::path& relpath);
    virtual ~Segment();

    std::shared_ptr<segment::Reader> reader() const;

    /// Instantiate the right Data for this segment
    std::shared_ptr<segment::Data> detect_data() const;

    /// Instantiate the right Reader implementation for a segment that already exists
    std::shared_ptr<segment::data::Reader> detect_data_reader(std::shared_ptr<core::Lock> lock) const;

    /// Instantiate the right Writer implementation for a segment that already exists
    std::shared_ptr<segment::data::Writer> detect_data_writer(const segment::data::WriterConfig& config) const;

    /// Instantiate the right Checker implementation for a segment that already exists
    std::shared_ptr<segment::data::Checker> detect_data_checker() const;

    /// Check if the given file or directory is a segment
    static bool is_segment(const std::filesystem::path& abspath);

    /**
     * Return the segment path for this pathname, stripping .gz, .tar, and .zip extensions
     */
    static std::filesystem::path basename(const std::filesystem::path& pathname);
};

namespace segment {

class Reader
{
protected:
    std::shared_ptr<const Segment> m_segment;

public:
    explicit Reader(std::shared_ptr<const Segment> segment);
};

}

}

#endif
