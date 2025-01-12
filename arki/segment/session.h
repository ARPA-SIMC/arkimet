#ifndef ARKI_SEGMENT_SESSION_H
#define ARKI_SEGMENT_SESSION_H

#include <arki/segment/fwd.h>
#include <arki/core/fwd.h>
#include <unordered_map>
#include <filesystem>
#include <string>

namespace arki::segment {

class Session: public std::enable_shared_from_this<Session>
{
protected:
    /// Map segment absolute paths to possibly reusable reader instances
    // TODO: use std::filesystem::path on newer GCC
    std::unordered_map<std::string, std::weak_ptr<segment::data::Reader>> reader_pool;

public:
    DefaultFileSegment default_file_segment = DefaultFileSegment::SEGMENT_FILE;
    DefaultDirSegment default_dir_segment = DefaultDirSegment::SEGMENT_DIR;
    bool mock_data = false;
    std::filesystem::path root;

    explicit Session(const std::filesystem::path& root);
    Session(const Session&) = delete;
    Session(Session&&) = delete;
    virtual ~Session();
    Session& operator=(const Session&) = delete;
    Session& operator=(Session&&) = delete;

    virtual std::shared_ptr<Segment> segment(DataFormat format, const std::filesystem::path& root, const std::filesystem::path& relpath);
    virtual std::shared_ptr<Segment> segment_from_path(const std::filesystem::path& path);
    virtual std::shared_ptr<Segment> segment_from_path_and_format(const std::filesystem::path& path, DataFormat format);
    virtual std::shared_ptr<Segment> segment_from_relpath(const std::filesystem::path& relpath);
    virtual std::shared_ptr<Segment> segment_from_relpath_and_format(const std::filesystem::path& relpath, DataFormat format);


    virtual std::shared_ptr<segment::data::Reader> segment_reader(DataFormat format, const std::filesystem::path& root, const std::filesystem::path& relpath, std::shared_ptr<core::Lock> lock);
    virtual std::shared_ptr<segment::data::Writer> segment_writer(const segment::data::WriterConfig& config, DataFormat format, const std::filesystem::path& root, const std::filesystem::path& relpath);
    virtual std::shared_ptr<segment::data::Checker> segment_checker(DataFormat format, const std::filesystem::path& root, const std::filesystem::path& relpath);
};

/*
// Use virtual to allow adding this as a mixing without introducing a new Session parent
class DirSegmentsMixin : virtual public Session
{
public:
    using Session::Session;

    std::shared_ptr<segment::data::Reader> segment_reader(DataFormat format, const std::filesystem::path& root, const std::filesystem::path& relpath, std::shared_ptr<core::Lock> lock) override;
    std::shared_ptr<segment::data::Writer> segment_writer(const segment::data::WriterConfig& config, DataFormat format, const std::filesystem::path& root, const std::filesystem::path& relpath) override;
    std::shared_ptr<segment::data::Checker> segment_checker(DataFormat format, const std::filesystem::path& root, const std::filesystem::path& relpath) override;

};
*/

}
#endif
