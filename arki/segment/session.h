#ifndef ARKI_SEGMENT_SESSION_H
#define ARKI_SEGMENT_SESSION_H

#include <arki/segment/fwd.h>
#include <arki/metadata/fwd.h>
#include <arki/core/fwd.h>
#include <unordered_map>
#include <filesystem>
#include <string>

namespace arki::segment {

class Session: public std::enable_shared_from_this<Session>
{
protected:
    /// Map segment absolute paths to possibly reusable reader instances
    mutable std::unordered_map<std::filesystem::path, std::weak_ptr<segment::data::Reader>> reader_pool;

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

    /// Check if the given file or directory is a data segment
    virtual bool is_data_segment(const std::filesystem::path& relpath) const;

    virtual std::shared_ptr<Segment> segment_from_relpath(const std::filesystem::path& relpath) const;
    virtual std::shared_ptr<Segment> segment_from_relpath_and_format(const std::filesystem::path& relpath, DataFormat format) const;

    virtual std::shared_ptr<segment::Reader> segment_reader(std::shared_ptr<const Segment> segment, std::shared_ptr<const core::ReadLock> lock) const;
    virtual std::shared_ptr<segment::Checker> segment_checker(std::shared_ptr<const Segment> segment, std::shared_ptr<core::CheckLock> lock) const;

    virtual std::shared_ptr<segment::data::Reader> segment_data_reader(std::shared_ptr<const Segment> segment, std::shared_ptr<const core::ReadLock> lock) const;
    virtual std::shared_ptr<segment::data::Writer> segment_data_writer(std::shared_ptr<const Segment> segment, const segment::data::WriterConfig& config) const;
    virtual std::shared_ptr<segment::data::Checker> segment_data_checker(std::shared_ptr<const Segment> segment) const;

    virtual void create_scan(std::shared_ptr<Segment> segment, arki::metadata::Collection& mds) const;
    virtual void create_metadata(std::shared_ptr<Segment> segment, arki::metadata::Collection& mds) const;
    virtual void create_iseg(std::shared_ptr<Segment> segment, arki::metadata::Collection& mds) const;
};

/**
 * Session that only uses scan segments
 */
class ScanSession : public Session
{
public:
    using Session::Session;

    std::shared_ptr<segment::Reader> segment_reader(std::shared_ptr<const Segment> segment, std::shared_ptr<const core::ReadLock> lock) const override;
    std::shared_ptr<segment::Checker> segment_checker(std::shared_ptr<const Segment> segment, std::shared_ptr<core::CheckLock> lock) const override;
};

/**
 * Session that only uses scan segments
 */
class MetadataSession : public Session
{
public:
    using Session::Session;

    std::shared_ptr<segment::Reader> segment_reader(std::shared_ptr<const Segment> segment, std::shared_ptr<const core::ReadLock> lock) const override;
    std::shared_ptr<segment::Checker> segment_checker(std::shared_ptr<const Segment> segment, std::shared_ptr<core::CheckLock> lock) const override;
};

}
#endif
