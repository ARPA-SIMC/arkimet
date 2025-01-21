#ifndef ARKI_SEGMENT_H
#define ARKI_SEGMENT_H

#include <filesystem>
#include <memory>
#include <string>
#include <arki/segment/fwd.h>
#include <arki/segment/session.h>
#include <arki/query/fwd.h>
#include <arki/core/time.h>
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
    std::shared_ptr<segment::Data> data() const;

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

class Reader : public std::enable_shared_from_this<Reader>
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

class Checker : public std::enable_shared_from_this<Checker>
{
protected:
    std::shared_ptr<core::CheckLock> lock;
    std::shared_ptr<const Segment> m_segment;
    std::shared_ptr<segment::Data> m_data;

public:
    struct FsckResult
    {
        size_t size = 0;
        time_t mtime = 0;
        core::Interval interval;
        segment::State state = SEGMENT_OK;
    };

    Checker(std::shared_ptr<const Segment> segment, std::shared_ptr<core::CheckLock> lock);
    Checker(const Checker&) = delete;
    Checker(Checker&&) = delete;
    Checker& operator=(const Checker&) = delete;
    Checker& operator=(Checker&&) = delete;
    virtual ~Checker();

    const Segment& segment() const { return *m_segment; }
    const segment::Data& data() const { return *m_data; }
    segment::Data& data() { return *m_data; }

    /**
     * Redo detection of the data accessor.
     *
     * Call this, for example, after converting the segment to a different
     * format.
     */
    void update_data();

    /**
     * Return the metadata for the contents of the whole segment
     */
    virtual arki::metadata::Collection scan() = 0;

    /**
     * Run consistency checks on the segment
     */
    virtual FsckResult fsck(segment::Reporter& reporter, bool quick=true) = 0;

    /**
     * Lock for writing and return a Fixer for this segment
     */
    virtual std::shared_ptr<Fixer> fixer() = 0;
};

class Fixer : public std::enable_shared_from_this<Fixer>
{
protected:
    std::shared_ptr<core::CheckWriteLock> lock;
    std::shared_ptr<Checker> m_checker;

    /**
     * Remove the file if it exists.
     *
     * @returns the number of bytes freed.
     */
    size_t remove_ifexists(const std::filesystem::path& path);

    /**
     * Get the data mtime after a fixer operation.
     *
     * @param operation_desc Description of the operation for error messages
     */
    time_t get_data_mtime_after_fix(const char* operation_desc);

public:
    struct ReorderResult
    {
        size_t size_pre = 0;
        size_t size_post = 0;
        time_t segment_mtime = 0;
    };

    struct ConvertResult
    {
        size_t size_pre = 0;
        size_t size_post = 0;
        time_t segment_mtime = 0;
    };

    struct MarkRemovedResult
    {
        time_t segment_mtime = 0;
        arki::core::Interval data_timespan;
    };

    Fixer(std::shared_ptr<Checker> checker, std::shared_ptr<core::CheckWriteLock> lock);
    Fixer(const Fixer&) = delete;
    Fixer(Fixer&&) = delete;
    Fixer& operator=(const Fixer&) = delete;
    Fixer& operator=(Fixer&&) = delete;
    virtual ~Fixer();

    const Segment& segment() const { return m_checker->segment(); }
    const Checker& checker() const { return *m_checker; }
    Checker& checker() { return *m_checker; }
    const segment::Data& data() const { return m_checker->data(); }
    segment::Data& data() { return m_checker->data(); }

    /**
     * Mark the data at the given offsets as removed.
     *
     * If the segment has an index, the data segment remains unchanged until
     * the next repack
     */
    virtual MarkRemovedResult mark_removed(const std::set<uint64_t>& offsets) = 0;

    /**
     * Rewrite the segment so that the data has the same order as `mds`.
     *
     * In the resulting file, there are no holes between data.
     *
     * @returns The size difference between the initial segment size and the
     * final segment size.
     */
    virtual ReorderResult reorder(arki::metadata::Collection& mds, const segment::data::RepackConfig& repack_config) = 0;

    /**
     * Convert the segment to use tar for the data.
     */
    virtual ConvertResult tar() = 0;

    /**
     * Convert the segment to use zip for the data.
     */
    virtual ConvertResult zip() = 0;

    /**
     * Compress the segment
     *
     * @param groupsize the number of data elements to compress together:
     *                  higher for more compression, lower for more efficent
     *                  seeking
     */
    virtual ConvertResult compress(unsigned groupsize) = 0;

    /**
     * Remove the segment.
     *
     * @param with_data: if false, only metadata is removed, and data is preserved.
     * @returns the number of bytes freed
     */
    virtual size_t remove(bool with_data) = 0;

    /**
     * Replace the segment index with the metadata in the given collection.
     *
     * Sources in mds are assumed to already point inside the segment.
     */
    virtual void reindex(arki::metadata::Collection& mds) = 0;

    virtual void test_corrupt_data(unsigned data_idx);
    virtual void test_truncate_data(unsigned data_idx);
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
