#ifndef ARKI_SEGMENT_H
#define ARKI_SEGMENT_H

#include <filesystem>
#include <memory>
#include <string>
#include <arki/segment/fwd.h>
#include <arki/segment/session.h>
#include <arki/query/fwd.h>
#include <arki/summary.h>

namespace arki {

class Segment : public std::enable_shared_from_this<Segment>
{
protected:
    std::shared_ptr<const segment::Session> m_session;
    DataFormat m_format;
    std::filesystem::path m_root;
    std::filesystem::path m_relpath;
    std::filesystem::path m_abspath;

public:

    Segment(std::shared_ptr<const segment::Session> session, DataFormat format, const std::filesystem::path& relpath);
    virtual ~Segment();

    const segment::Session& session() const { return *m_session; }
    DataFormat format() const { return m_format; }
    std::filesystem::path root() const { return m_session->root; }
    std::filesystem::path relpath() const { return m_relpath; }
    std::filesystem::path abspath() const { return m_abspath; }
    std::filesystem::path abspath_metadata() const;
    std::filesystem::path abspath_summary() const;
    std::filesystem::path abspath_iseg_index() const;

    std::shared_ptr<segment::Reader> reader(std::shared_ptr<const core::ReadLock> lock) const;
    std::shared_ptr<segment::Checker> checker(std::shared_ptr<core::CheckLock> lock) const;

    /// Instantiate the right Data for this segment
    std::shared_ptr<segment::Data> detect_data() const;

    /// Instantiate the right Reader implementation for a segment that already exists
    std::shared_ptr<segment::data::Reader> detect_data_reader(std::shared_ptr<const core::ReadLock> lock) const;

    /// Instantiate the right Writer implementation for a segment that already exists
    std::shared_ptr<segment::data::Writer> detect_data_writer(const segment::data::WriterConfig& config) const;

    /// Instantiate the right Checker implementation for a segment that already exists
    std::shared_ptr<segment::data::Checker> data_checker() const;

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
    std::shared_ptr<const core::ReadLock> lock;

public:
    explicit Reader(std::shared_ptr<const Segment> segment, std::shared_ptr<const core::ReadLock> lock);
    Reader(const Reader&) = delete;
    Reader(Reader&&) = delete;
    Reader& operator=(const Reader&) = delete;
    Reader& operator=(Reader&&) = delete;
    virtual ~Reader();

    /// Access the segment
    const Segment& segment() const { return *m_segment; }

    /**
     * Send all data from the segment to dest, in the order in which they are
     * stored
     */
    virtual bool read_all(metadata_dest_func dest) = 0;

    /**
     * Query the segment using the given matcher, and sending the results to
     * the given function.
     *
     * Returns true if dest always returned true, false if iteration stopped
     * because dest returned false.
     */
    virtual bool query_data(const query::Data& q, metadata_dest_func dest) = 0;

    /**
     * Add to summary the summary of the data that would be extracted with the
     * given query.
     */
    virtual void query_summary(const Matcher& matcher, Summary& summary) = 0;

#if 0
    /**
     * Return the time interval of available data stored in this segment.
     *
     * If there is no data, it returns an unbound interval.
     */
    virtual core::Interval get_stored_time_interval() = 0;
#endif
};

class Checker
{
protected:
    std::shared_ptr<core::CheckLock> lock;
    std::shared_ptr<const Segment> m_segment;
    std::shared_ptr<segment::Data> m_data;

public:
    Checker(std::shared_ptr<const Segment> segment, std::shared_ptr<core::CheckLock> lock);
    Checker(const Checker&) = delete;
    Checker(Checker&&) = delete;
    Checker& operator=(const Checker&) = delete;
    Checker& operator=(Checker&&) = delete;
    virtual ~Checker();

    /**
     * Return the metadata for the contents of the whole segment
     */
    virtual arki::metadata::Collection scan() = 0;
};

/**
 * Reader that always returns no results
 */
class EmptyReader: public Reader
{
public:
    using Reader::Reader;

    bool read_all(metadata_dest_func dest) override;
    bool query_data(const query::Data& q, metadata_dest_func dest) override;
    void query_summary(const Matcher& matcher, Summary& summary) override;
};

}

}

#endif
