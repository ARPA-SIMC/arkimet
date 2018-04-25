#ifndef ARKI_SEGMENT_H
#define ARKI_SEGMENT_H

/// Dataset segment read/write functions

#include <arki/libconfig.h>
#include <arki/defs.h>
#include <arki/core/fwd.h>
#include <arki/types/fwd.h>
#include <arki/scan/fwd.h>
#include <arki/metadata/fwd.h>
#include <arki/segment/fwd.h>
#include <arki/transaction.h>
#include <string>
#include <iosfwd>
#include <memory>
#include <vector>

namespace arki {
namespace segment {

static const unsigned SEGMENT_OK          = 0;
static const unsigned SEGMENT_DIRTY       = 1 << 0; /// Segment contains data deleted or out of order
static const unsigned SEGMENT_UNALIGNED   = 1 << 1; /// Segment contents are inconsistent with the index
static const unsigned SEGMENT_MISSING     = 1 << 2; /// Segment is known to the index, but does not exist on disk
static const unsigned SEGMENT_DELETED     = 1 << 3; /// Segment contents have been entirely deleted
static const unsigned SEGMENT_CORRUPTED   = 1 << 4; /// File is broken in a way that needs manual intervention
static const unsigned SEGMENT_ARCHIVE_AGE = 1 << 5; /// File is old enough to be archived
static const unsigned SEGMENT_DELETE_AGE  = 1 << 6; /// File is old enough to be deleted

static const unsigned TEST_MISCHIEF_MOVE_DATA = 1; /// During repack, move all data to a different location than it was before


/**
 * State of a segment
 */
struct State
{
    unsigned value;

    State() : value(SEGMENT_OK) {}
    State(unsigned value) : value(value) {}

    bool is_ok() const { return value == SEGMENT_OK; }

    bool has(unsigned state) const
    {
        return value & state;
    }

    State& operator+=(const State& fs)
    {
        value |= fs.value;
        return *this;
    }

    State& operator-=(const State& fs)
    {
        value &= ~fs.value;
        return *this;
    }

    State operator+(const State& fs) const
    {
        return State(value | fs.value);

    }

    State operator-(const State& fs) const
    {
        return State(value & ~fs.value);

    }

    bool operator==(const State& fs) const
    {
        return value == fs.value;
    }

    /// Return a text description of this file state
    std::string to_string() const;
};

/// Print to ostream
std::ostream& operator<<(std::ostream&, const State&);

struct Span
{
    size_t offset;
    size_t size;
    Span() = default;
    Span(size_t offset, size_t size) : offset(offset), size(size) {}
    bool operator<(const Span& o) const { return std::tie(offset, size) < std::tie(o.offset, o.size); }
};

}

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
class Segment
{
public:
    std::string format;
    std::string root;
    std::string relpath;
    std::string abspath;

    Segment(const std::string& format, const std::string& root, const std::string& relpath, const std::string& abspath);
    virtual ~Segment();

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
     * Get the last modification timestamp of the segment
     */
    virtual time_t timestamp() const = 0;

    /**
     * Instantiate a reader for this segment
     */
    virtual std::shared_ptr<segment::Reader> reader(std::shared_ptr<core::Lock> lock) const = 0;

    /**
     * Instantiate a checker for this segment
     */
    virtual std::shared_ptr<segment::Checker> checker() const = 0;

    /**
     * Return the segment path for this pathname, stripping .gz, .tar, and .zip extensions
     */
    static std::string basename(const std::string& pathname);

    /// Instantiate the right Reader implementation for a segment that already exists
    static std::shared_ptr<segment::Reader> make_reader(const std::string& format, const std::string& root, const std::string& relpath, const std::string& abspath, std::shared_ptr<core::Lock> lock);

    /// Instantiate the right Writer implementation for a segment that already exists
    static std::shared_ptr<segment::Writer> make_writer(const std::string& format, const std::string& root, const std::string& relpath, const std::string& abspath, bool mock_data=false);

    /// Instantiate the right Checker implementation for a segment that already exists
    static std::shared_ptr<segment::Checker> make_checker(const std::string& format, const std::string& root, const std::string& relpath, const std::string& abspath, bool mock_data=false);
};


namespace segment {

struct Reader : public std::enable_shared_from_this<Reader>
{
    std::shared_ptr<core::Lock> lock;

    Reader(std::shared_ptr<core::Lock> lock);
    virtual ~Reader();

    virtual const Segment& segment() const = 0;

    /**
     * Scan the segment contents, and sends the resulting metadata to \a dest.
     *
     * If a .metadata file exists for this segment and its timestamp is the
     * same than the segment or newer, it will be used instead performing the
     * scan.
     *
     * Returns true if dest always returned true.
     */
    bool scan(metadata_dest_func dest);

    /**
     * Scan the segment contents ignoring all existing metadata (if any).
     *
     * Sends the resulting metadata to \a dest
     *
     * Returns true if dest always returned true.
     */
    virtual bool scan_data(metadata_dest_func dest) = 0;

    virtual std::vector<uint8_t> read(const types::source::Blob& src) = 0;
    virtual size_t stream(const types::source::Blob& src, core::NamedFileDescriptor& out) = 0;
};

struct Writer : public Transaction, public std::enable_shared_from_this<Writer>
{
    struct PendingMetadata
    {
        Metadata& md;
        types::source::Blob* new_source;

        PendingMetadata(Metadata& md, std::unique_ptr<types::source::Blob> new_source);
        PendingMetadata(const PendingMetadata&) = delete;
        PendingMetadata(PendingMetadata&& o);
        ~PendingMetadata();
        PendingMetadata& operator=(const PendingMetadata&) = delete;
        PendingMetadata& operator=(PendingMetadata&&) = delete;

        void set_source();
    };

    bool fired = false;

    virtual const Segment& segment() const = 0;

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
     * commit
     */
    virtual const types::source::Blob& append(Metadata& md) = 0;
};


class Checker : public std::enable_shared_from_this<Checker>
{
protected:
    virtual void move_data(const std::string& new_root, const std::string& new_relpath, const std::string& new_abspath) = 0;

public:
    virtual const Segment& segment() const = 0;
    virtual segment::State check(std::function<void(const std::string&)> reporter, const metadata::Collection& mds, bool quick=true) = 0;
    virtual size_t remove() = 0;
    virtual size_t size() = 0;

    /**
     * Check if the segment exists on disk
     */
    virtual bool exists_on_disk() = 0;

    /**
     * Rescan the segment, using Reader::scan_data of the right reader for this segment
     */
    bool scan_data(std::shared_ptr<core::Lock> lock, metadata_dest_func dest);

    /**
     * Rewrite this segment so that the data are in the same order as in `mds`.
     *
     * `rootdir` is the directory to use as root for the Blob sources in `mds`.
     */
    virtual Pending repack(const std::string& rootdir, metadata::Collection& mds, unsigned test_flags=0) = 0;

    /**
     * Replace this segment with a tar segment, updating the metadata in mds to
     * point to the right locations inside the tarball
     */
    virtual std::shared_ptr<Checker> tar(metadata::Collection& mds);

    /**
     * Replace this segment with a zip segment, updating the metadata in mds to
     * point to the right locations inside the tarball
     */
    virtual std::shared_ptr<Checker> zip(metadata::Collection& mds);

    /**
     * Replace this segment with a compressed segment, updating the metadata in
     * mds to point to the right locations if needed
     */
    virtual std::shared_ptr<Checker> compress(metadata::Collection& mds);

    /**
     * Move this segment to a new location.
     *
     * Using the segment after moving it can have unpredictable effects.
     *
     * Returns a Checker pointing to the new location
     */
    std::shared_ptr<Checker> move(const std::string& new_root, const std::string& new_relpath, const std::string& new_abspath);

    /**
     * After this segment has been moved, create a checker for the one in the
     * new location
     */
    //virtual std::shared_ptr<Checker> checker_moved(const std::string& new_root, const std::string& new_relpath, const std::string& new_abspath) const = 0;

    /**
     * Truncate the segment at the given offset
     */
    virtual void test_truncate(size_t offset) = 0;

    /**
     * Truncate the data at position `data_idx`
     */
    virtual void test_truncate(const metadata::Collection& mds, unsigned data_idx);

    /**
     * Move all the data in the segment starting from the one in position
     * `data_idx` forwards by `hole_size`.
     *
     * `mds` represents the state of the segment before the move, and is
     * updated to reflect the new state of the segment.
     */
    virtual void test_make_hole(metadata::Collection& mds, unsigned hole_size, unsigned data_idx) = 0;

    /**
     * Move all the data in the segment starting from the one in position
     * `data_idx` backwards by `overlap_size.
     *
     * `mds` represents the state of the segment before the move, and is
     * updated to reflect the new state of the segment.
     */
    virtual void test_make_overlap(metadata::Collection& mds, unsigned overlap_size, unsigned data_idx) = 0;

    /**
     * Corrupt the data at position `data_idx`, by replacing its first byte
     * with the value 0.
     */
    virtual void test_corrupt(const metadata::Collection& mds, unsigned data_idx) = 0;
};

}
}
#endif

