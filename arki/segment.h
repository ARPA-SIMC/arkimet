#ifndef ARKI_SEGMENT_H
#define ARKI_SEGMENT_H

#include <filesystem>
#include <memory>
#include <string>
#include <arki/segment/session.h>

namespace arki {

class Segment : public std::enable_shared_from_this<Segment>
{
// protected:
//     std::shared_ptr<segment::Session> session;

public:
    enum class DefaultFileSegment {
        SEGMENT_FILE,
        SEGMENT_GZ,
        SEGMENT_DIR,
        SEGMENT_TAR,
        SEGMENT_ZIP,
    };
    enum class DefaultDirSegment {
        SEGMENT_DIR,
        SEGMENT_TAR,
        SEGMENT_ZIP,
    };

    std::shared_ptr<segment::Session> session;
    DataFormat format;
    std::filesystem::path root;
    std::filesystem::path relpath;
    std::filesystem::path abspath;

    Segment(std::shared_ptr<segment::Session> session, DataFormat format, const std::filesystem::path& root, const std::filesystem::path& relpath);
    virtual ~Segment();

    /// Instantiate the right Data for this segment
    std::shared_ptr<segment::Data> detect_data(
            DefaultFileSegment default_file_segment=DefaultFileSegment::SEGMENT_FILE,
            DefaultDirSegment default_dir_segment=DefaultDirSegment::SEGMENT_DIR) const;

    /// Instantiate the right Reader implementation for a segment that already exists
    std::shared_ptr<segment::data::Reader> detect_data_reader(std::shared_ptr<core::Lock> lock) const;

    /// Instantiate the right Writer implementation for a segment that already exists
    std::shared_ptr<segment::data::Writer> detect_data_writer(const segment::data::WriterConfig& config, bool mock_data=false) const;

    /// Instantiate the right Checker implementation for a segment that already exists
    std::shared_ptr<segment::data::Checker> detect_data_checker(bool mock_data=false) const;

    /// Check if the given file or directory is a segment
    static bool is_segment(const std::filesystem::path& abspath);

    /**
     * Return the segment path for this pathname, stripping .gz, .tar, and .zip extensions
     */
    static std::filesystem::path basename(const std::filesystem::path& pathname);
};

}

#endif
