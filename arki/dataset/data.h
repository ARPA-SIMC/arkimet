#ifndef ARKI_DATASET_DATA_H
#define ARKI_DATASET_DATA_H

/*
 * data - Read/write functions for data blobs
 *
 * Copyright (C) 2012--2015  ARPA-SIM <urpsim@smr.arpa.emr.it>
 *
 * This program is free software; you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation; either version 2 of the License, or
 * (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License along
 * with this program; if not, write to the Free Software Foundation, Inc.,
 * 51 Franklin Street, Fifth Floor, Boston, MA 02110-1301 USA.
 *
 * Author: Enrico Zini <enrico@enricozini.com>
 */

#include <arki/libconfig.h>
#include <arki/defs.h>
#include <arki/transaction.h>
#include <arki/nag.h>
#include <string>
#include <iosfwd>
#include <sys/types.h>
#include <memory>

namespace wibble {
namespace sys {
class Buffer;
}
}

namespace arki {
class Metadata;
class ConfigFile;

namespace metadata {
class Collection;
}

namespace dataset {
static const unsigned FILE_OK    = 0;
static const unsigned FILE_TO_ARCHIVE = 1 << 0; /// File is ok, but old enough to be archived
static const unsigned FILE_TO_DELETE  = 1 << 1; /// File is ok, but old enough to be deleted
static const unsigned FILE_TO_PACK    = 1 << 2; /// File contains data that has been deleted
static const unsigned FILE_TO_INDEX   = 1 << 3; /// File is not present in the index
static const unsigned FILE_TO_RESCAN  = 1 << 4; /// File contents are inconsistent with the index
static const unsigned FILE_TO_DEINDEX = 1 << 5; /// File does not exist but has entries in the index
static const unsigned FILE_ARCHIVED   = 1 << 6; /// File is in the archive

namespace data {
class Segment;

/**
 * State of a file in a dataset, as one or more of the FILE_* flags
 */
struct FileState
{
    unsigned value;

    FileState() : value(FILE_OK) {}
    FileState(unsigned value) : value(value) {}

    bool is_ok() const { return value == FILE_OK; }
    bool is_archived_ok() const { return value == FILE_ARCHIVED; }

    bool has(unsigned state) const
    {
        return value & state;
    }

    FileState operator+(const FileState& fs) const
    {
        return FileState(value + fs.value);

    }

    FileState operator-(const FileState& fs) const
    {
        return FileState(value & ~fs.value);

    }

    /// Return a text description of this file state
    std::string to_string() const;
};

namespace impl {

template<typename T, unsigned max_size=3>
class Cache
{
protected:
    T* items[max_size];

    /// Move an existing element to the front of the list
    void move_to_front(unsigned pos)
    {
        // Save a pointer to the item
        T* item = items[pos];
        // Shift all previous items forward
        for (unsigned i = pos; i > 0; --i)
            items[i] = items[i - 1];
        // Set the first element to item
        items[0] = item;
    }

public:
    Cache()
    {
        for (unsigned i = 0; i < max_size; ++i)
            items[i] = 0;
    }

    ~Cache()
    {
        for (unsigned i = 0; i < max_size && items[i]; ++i)
            delete items[i];
    }

    void clear()
    {
        for (int i = max_size - 1; i >= 0; --i)
        {
            if (!items[i]) continue;
            delete items[i];
            items[i] = 0;
        }
    }

    /**
     * Get an element from the cache given its file name
     *
     * @returns 0 if not found, else a pointer to the element, whose memory is
     *          still managed by the cache
     */
    T* get(const std::string& relname)
    {
        for (unsigned i = 0; i < max_size && items[i]; ++i)
            if (items[i]->relname == relname)
            {
                // Move to front if it wasn't already
                if (i > 0)
                    move_to_front(i);
                return items[0];
            }
        // Not found
        return 0;
    }

    /**
     * Add an element to the cache, taking ownership of its memory management
     *
     * @returns the element itself, just for the convenience of being able to
     * type "return cache.add(Value::create());"
     */
    T* add(std::auto_ptr<T> val)
    {
        // Delete the last element if the cache is full
        if (items[max_size - 1])
        {
            try {
                delete items[max_size - 1];
            } catch (std::exception& e) {
                // Prevent an error in the destructor of an unrelated file to
                // confuse what we are doing here
                nag::warning("Cannot close the least recently used segment: %s", e.what());
            }
        }
        // Shift all other elements forward
        for (unsigned i = max_size - 1; i > 0; --i)
            items[i] = items[i - 1];
        // Set the first element to val
        return items[0] = val.release();
    }
};

}

/// Manage instantiating the right readers/writers for a dataset
class SegmentManager
{
protected:
    impl::Cache<Segment> segments;

public:
    virtual ~SegmentManager();

    void flush_writers();

    virtual Segment* get_segment(const std::string& relname) = 0;
    virtual Segment* get_segment(const std::string& format, const std::string& relname) = 0;

    /**
     * Repack the file relname, so that it contains only the data in mds, in
     * the same order as in mds.
     *
     * The source metadata in mds will be updated to point to the new file.
     */
    virtual Pending repack(const std::string& relname, metadata::Collection& mds) = 0;

    /**
     * Check the given file against its expected set of contents.
     *
     * @returns the FileState with the state of the file
     */
    virtual FileState check(const std::string& relname, const metadata::Collection& mds, bool quick=true) = 0;

    /**
     * Remove a file, returning its size
     */
    virtual size_t remove(const std::string& relname) = 0;

    /**
     * Truncate a file at the given offset
     *
     * This function is useful for implementing unit tests.
     */
    virtual void truncate(const std::string& relname, size_t offset) = 0;

    /// Create a SegmentManager using default options
    static std::auto_ptr<SegmentManager> get(const std::string& root);

    /// Create a new SegmentManager given a dataset's configuration
    static std::auto_ptr<SegmentManager> get(const ConfigFile& cfg);
};

/**
 * Interface for managing a dataset segment.
 *
 * A dataset segment is a group of data elements stored on disk. It can be a
 * file with all data elements appended one after the other, for formats like
 * BUFR or GRIB that support it; it can be a text file with one data item per
 * line, for line based formats like VM2, or it can be a tar file or a
 * directory with each data item in a different file, for formats like HDF5
 * that cannot be trivially concatenated in the same file.
 */
struct Segment
{
public:
    std::string relname;
    std::string absname;

    // TODO: document
    struct Payload
    {
        virtual ~Payload() {}
    };
    Payload* payload;

    Segment(const std::string& relname, const std::string& absname);
    virtual ~Segment();


    /**
     * Append the data, updating md's source information.
     *
     * In case of write errors (for example, disk full) it tries to truncate
     * the file as it was before, before raising an exception.
     */
    virtual void append(Metadata& md) = 0;

    /**
     * Append raw data to the file, wrapping it with the right envelope if
     * needed.
     *
     * All exceptions are propagated upwards without special handling. If this
     * operation fails, the file should be considered invalid.
     *
     * This function is intended to be used by low-level maintenance operations,
     * like a file repack.
     *
     * @return the offset at which the buffer is written
     */
    virtual off_t append(const wibble::sys::Buffer& buf) = 0;

    /**
     * Append the data, in a transaction, updating md's source information.
     *
     * At the beginning of the transaction, the file is locked and the write
     * offset is read. Committing the transaction actually writes the data to
     * the file.
     */
    virtual Pending append(Metadata& md, off_t* ofs) = 0;

    virtual FileState check(const metadata::Collection& mds, bool quick=true) = 0;
    virtual size_t remove() = 0;
    virtual void truncate(size_t offset) = 0;
    virtual Pending repack(const std::string& rootdir, metadata::Collection& mds) = 0;
};

/**
 * Interface for writing metadata and data to output streams.
 */
class OstreamWriter
{
public:
    virtual ~OstreamWriter();

    virtual size_t stream(Metadata& md, std::ostream& out) const = 0;
    virtual size_t stream(Metadata& md, int out) const = 0;

    /**
     * Returns a pointer to a static instance of the appropriate OstreamWriter
     * for the given format
     */
    static const OstreamWriter* get(const std::string& format);
};

}
}
}

#endif
