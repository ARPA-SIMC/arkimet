#ifndef ARKI_SEGMENT_DATA_H
#define ARKI_SEGMENT_DATA_H

/// Dataset segment read/write functions

#include <arki/core/fwd.h>
#include <arki/core/transaction.h>
#include <arki/defs.h>
#include <arki/metadata/fwd.h>
#include <arki/scan/fwd.h>
#include <arki/segment.h>
#include <arki/segment/defs.h>
#include <arki/segment/fwd.h>
#include <arki/stream/fwd.h>
#include <arki/types/fwd.h>
#include <arki/utils/files.h>
#include <cstdint>
#include <filesystem>
#include <memory>
#include <optional>
#include <string>
#include <vector>

namespace arki::segment {

namespace data {
struct RepackConfig
{
    /**
     * When repacking gzidx segments, how many data items are compressed
     * together.
     */
    unsigned gz_group_size = 512;

    /**
     * Turn on perturbating repack behaviour during tests
     */
    unsigned test_flags = 0;

    /// During repack, move all data to a different location than it was before
    static const unsigned TEST_MISCHIEF_MOVE_DATA = 1;

    RepackConfig();
    explicit RepackConfig(unsigned gz_group_size, unsigned test_flags = 0);
};
} // namespace data

/**
 * Interface for managing a segment.
 *
 * A segment is a group of data elements stored on disk. It can be a file with
 * all data elements appended one after the other, for formats like BUFR or
 * GRIB that support it; it can be a text file with one data item per line, for
 * line based formats like VM2, or it can be a tar file or a directory with
 * each data item in a different file, for formats like HDF5 that cannot be
 * trivially concatenated in the same file.
 */
class Data : public std::enable_shared_from_this<Data>
{
protected:
    std::shared_ptr<const Segment> m_segment;

public:
    Data(std::shared_ptr<const Segment> segment);
    virtual ~Data();

    /// Access the underlying segment
    const Segment& segment() const { return *m_segment; }
    /// Access the underlying segment session
    const segment::Session& session() const { return m_segment->session(); }

    /**
     * Return a name identifying the type of segment backend
     *
     * This is used mostly for tests, actual code must not differentiate on
     * this.
     */
    virtual const char* type() const = 0;

    /**
     * Return true if the segment stores everything in a single file
     */
    virtual bool single_file() const = 0;

    /**
     * Get the last modification timestamp of the segment.
     *
     * Returns missing if the data segment does not exist.
     */
    virtual std::optional<time_t> timestamp() const = 0;

    /**
     * Return the size of the data portion of the segment
     */
    virtual size_t size() const = 0;

    /// Check if the segment data exist on disk
    virtual bool exists_on_disk() const = 0;

    /**
     * Return true if the segment does not contain any data.
     *
     * Return false if the segment contains data, or if the segment does not
     * exist or is not a valid segment.
     */
    virtual bool is_empty() const = 0;

    /**
     * Compute the offset of the beginning of data following this one in the
     * segment
     */
    virtual size_t next_offset(size_t offset, size_t size) const = 0;

#if 0
    /// Get a read lock on this segment
    virtual std::shared_ptr<const core::ReadLock> read_lock() const = 0;

    /// Get an append lock on this segment
    virtual std::shared_ptr<core::AppendLock> append_lock() const = 0;

    /// Get a check lock on this segment
    virtual std::shared_ptr<core::CheckLock> check_lock() const = 0;
#endif

    /**
     * Instantiate a reader for this segment
     */
    virtual std::shared_ptr<segment::data::Reader>
    reader(std::shared_ptr<const core::ReadLock> lock) const = 0;

    /**
     * Instantiate a writer for this segment
     */
    virtual std::shared_ptr<segment::data::Writer>
    writer(const segment::WriterConfig& config) const = 0;

    /**
     * Instantiate a checker for this segment
     */
    virtual std::shared_ptr<segment::data::Checker> checker() const = 0;

    /**
     * Create a new segment with the given data.
     *
     * Replace metadata sources with pointers to the data in the newly created
     * segment
     */
    virtual void
    create_segment(arki::metadata::Collection& mds,
                   const data::RepackConfig& cfg = data::RepackConfig()) = 0;

    /**
     * Preserve segment mtime
     */
    virtual utils::files::PreserveFileTimes preserve_mtime() = 0;
};

namespace data {

class Reader : public std::enable_shared_from_this<Reader>
{
public:
    std::shared_ptr<const core::ReadLock> lock;

    Reader(std::shared_ptr<const core::ReadLock> lock);
    virtual ~Reader();

    virtual const Segment& segment() const = 0;
    virtual const Data& data() const       = 0;

    /**
     * Scan the segment contents ignoring all existing metadata (if any).
     *
     * Sends the resulting metadata to \a dest
     *
     * Returns true if dest always returned true.
     */
    virtual bool scan_data(metadata_dest_func dest) = 0;

    virtual std::vector<uint8_t> read(const types::source::Blob& src) = 0;
    virtual stream::SendResult stream(const types::source::Blob& src,
                                      StreamOutput& out);
};

class Writer : public core::Transaction,
               public std::enable_shared_from_this<Writer>
{
public:
    struct PendingMetadata
    {
        const segment::WriterConfig& config;
        Metadata& md;
        types::source::Blob* new_source;

        PendingMetadata(const segment::WriterConfig& config, Metadata& md,
                        std::unique_ptr<types::source::Blob> new_source);
        PendingMetadata(const PendingMetadata&) = delete;
        PendingMetadata(PendingMetadata&& o);
        ~PendingMetadata();
        PendingMetadata& operator=(const PendingMetadata&) = delete;
        PendingMetadata& operator=(PendingMetadata&&)      = delete;

        void set_source();
    };

    segment::WriterConfig config;
    bool fired = false;

    Writer() = default;
    Writer(const segment::WriterConfig& config) : config(config) {}
    virtual ~Writer() {}

    virtual const Segment& segment() const = 0;
    virtual const Data& data() const       = 0;

    /**
     * Return the write offset for the next append operation
     */
    virtual size_t next_offset() const = 0;

    /**
     * Append the data, in a transaction, updating md's source information.
     *
     * At the beginning of the transaction, the file is locked and the write
     * offset is read. Committing the transaction actually writes the data to
     * the file.
     *
     * Returns a reference to the blob source that will be set into \a md on
     * commit.
     */
    virtual const types::source::Blob& append(Metadata& md) = 0;
};

class Checker : public std::enable_shared_from_this<Checker>
{
protected:
    virtual void move_data(std::shared_ptr<const Segment> new_segment) = 0;

public:
    virtual ~Checker() {}

    virtual const Segment& segment() const = 0;
    virtual const Data& data() const       = 0;
    virtual segment::State
    check(std::function<void(const std::string&)> reporter,
          const arki::metadata::Collection& mds, bool quick = true) = 0;
    virtual size_t remove()                                         = 0;

    /**
     * Rescan the segment, possibly fixing fixable issues found during the
     * rescan
     */
    virtual bool rescan_data(std::function<void(const std::string&)> reporter,
                             std::shared_ptr<const core::ReadLock> lock,
                             metadata_dest_func dest) = 0;

    /**
     * Rewrite this segment so that the data are in the same order as in `mds`.
     */
    virtual core::Pending repack(arki::metadata::Collection& mds,
                                 const RepackConfig& cfg = RepackConfig()) = 0;

    /**
     * Replace this segment with a tar segment, updating the metadata in mds to
     * point to the right locations inside the tarball
     */
    virtual std::shared_ptr<Checker> tar(arki::metadata::Collection& mds);

    /**
     * Replace this segment with a zip segment, updating the metadata in mds to
     * point to the right locations inside the tarball
     */
    virtual std::shared_ptr<Checker> zip(arki::metadata::Collection& mds);

    /**
     * Replace this segment with a compressed segment, updating the metadata in
     * mds to point to the right locations if needed
     */
    virtual std::shared_ptr<Checker> compress(arki::metadata::Collection& mds,
                                              unsigned groupsize);

    /**
     * Move this segment to a new location.
     *
     * Using the segment after moving it can have unpredictable effects.
     *
     * Returns a Checker pointing to the new location
     */
    virtual std::shared_ptr<Checker>
    move(std::shared_ptr<const segment::Session> segment_session,
         const std::filesystem::path& new_relpath) = 0;

    /**
     * Truncate the segment at the given offset
     */
    virtual void test_truncate(size_t offset) = 0;

    /**
     * Move all the data in the segment starting from the one in position
     * `data_idx` forwards by `hole_size`.
     *
     * `mds` represents the state of the segment before the move, and is
     * updated to reflect the new state of the segment.
     */
    virtual void test_make_hole(arki::metadata::Collection& mds,
                                unsigned hole_size, unsigned data_idx) = 0;

    /**
     * Move all the data in the segment starting from the one in position
     * `data_idx` backwards by `overlap_size.
     *
     * `mds` represents the state of the segment before the move, and is
     * updated to reflect the new state of the segment.
     */
    virtual void test_make_overlap(arki::metadata::Collection& mds,
                                   unsigned overlap_size,
                                   unsigned data_idx) = 0;

    /**
     * Corrupt the data at position `data_idx`, by replacing its first byte
     * with the value 0.
     */
    virtual void test_corrupt(const arki::metadata::Collection& mds,
                              unsigned data_idx) = 0;

    /**
     * Set the modification time of everything in the segment
     */
    virtual void test_touch_contents(time_t timestamp) = 0;
};

} // namespace data
} // namespace arki::segment
#endif
