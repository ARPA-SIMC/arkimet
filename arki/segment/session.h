#ifndef ARKI_SEGMENT_SESSION_H
#define ARKI_SEGMENT_SESSION_H

#include <arki/core/fwd.h>
#include <arki/metadata/fwd.h>
#include <arki/segment/fwd.h>
#include <filesystem>
#include <string>
#include <unordered_map>

namespace arki::segment {

class Session : public std::enable_shared_from_this<Session>
{
protected:
    /// Map segment absolute paths to possibly reusable reader instances
    // TODO: this is needed for rocky8 and ubuntu jammy. Use
    // std::filesystem::path when we can rely on newer GCC
    mutable std::unordered_map<std::string,
                               std::weak_ptr<segment::data::Reader>>
        reader_pool;

public:
    DefaultFileSegment default_file_segment = DefaultFileSegment::SEGMENT_FILE;
    DefaultDirSegment default_dir_segment   = DefaultDirSegment::SEGMENT_DIR;

    /// Toplevel directory for all segments in this session
    std::filesystem::path root;

    /**
     * Try to store the content of small files in the index if possible, to
     * avoid extra I/O when querying
     */
    bool smallfiles = false;

    /**
     * Trade write reliability and write concurrency in favour of performance.
     *
     * Useful for writing fast temporary private datasets.
     */
    bool eatmydata = false;

    /**
     * Discard data and use filesystem holes in segment data.
     *
     * This is only used in tests.
     */
    bool mock_data = false;

    explicit Session(const std::filesystem::path& root);
    explicit Session(const core::cfg::Section& cfg);
    Session(const Session&) = delete;
    Session(Session&&)      = delete;
    virtual ~Session();
    Session& operator=(const Session&) = delete;
    Session& operator=(Session&&)      = delete;

    /// Check if the given file or directory is a data segment
    virtual bool is_data_segment(const std::filesystem::path& relpath) const;

    virtual std::shared_ptr<Segment>
    segment_from_relpath(const std::filesystem::path& relpath) const;
    virtual std::shared_ptr<Segment>
    segment_from_relpath_and_format(const std::filesystem::path& relpath,
                                    DataFormat format) const;

    virtual std::shared_ptr<segment::Reader>
    segment_reader(std::shared_ptr<const Segment> segment,
                   std::shared_ptr<const core::ReadLock> lock) const;
    virtual std::shared_ptr<segment::Writer>
    segment_writer(std::shared_ptr<const Segment> segment,
                   std::shared_ptr<core::AppendLock> lock) const;
    virtual std::shared_ptr<segment::Checker>
    segment_checker(std::shared_ptr<const Segment> segment,
                    std::shared_ptr<core::CheckLock> lock) const;

    virtual std::shared_ptr<segment::data::Reader>
    segment_data_reader(std::shared_ptr<const Segment> segment,
                        std::shared_ptr<const core::ReadLock> lock) const;
    virtual std::shared_ptr<segment::data::Writer>
    segment_data_writer(std::shared_ptr<const Segment> segment,
                        const segment::WriterConfig& config) const;
    virtual std::shared_ptr<segment::data::Checker>
    segment_data_checker(std::shared_ptr<const Segment> segment) const;

    virtual void create_scan(std::shared_ptr<Segment> segment,
                             arki::metadata::Collection& mds) const;
    virtual void create_metadata(std::shared_ptr<Segment> segment,
                                 arki::metadata::Collection& mds) const;
    virtual void create_iseg(std::shared_ptr<Segment> segment,
                             arki::metadata::Collection& mds) const;
};

/**
 * Session that only uses scan segments
 */
class ScanSession : public Session
{
public:
    using Session::Session;

    std::shared_ptr<segment::Reader>
    segment_reader(std::shared_ptr<const Segment> segment,
                   std::shared_ptr<const core::ReadLock> lock) const override;
    std::shared_ptr<segment::Checker>
    segment_checker(std::shared_ptr<const Segment> segment,
                    std::shared_ptr<core::CheckLock> lock) const override;
};

/**
 * Session that only uses scan segments
 */
class MetadataSession : public Session
{
public:
    using Session::Session;

    std::shared_ptr<segment::Reader>
    segment_reader(std::shared_ptr<const Segment> segment,
                   std::shared_ptr<const core::ReadLock> lock) const override;
    std::shared_ptr<segment::Checker>
    segment_checker(std::shared_ptr<const Segment> segment,
                    std::shared_ptr<core::CheckLock> lock) const override;
};

} // namespace arki::segment
#endif
