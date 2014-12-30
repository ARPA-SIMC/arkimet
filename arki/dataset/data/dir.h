#ifndef ARKI_DATASET_DATA_DIR_H
#define ARKI_DATASET_DATA_DIR_H

/*
 * data/dir - Directory based data collection
 *
 * Copyright (C) 2014  ARPA-SIM <urpsim@smr.arpa.emr.it>
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
#include <arki/dataset/data.h>

namespace arki {
class Metadata;

namespace dataset {
namespace data {
namespace dir {

struct SequenceFile
{
    std::string dirname;
    std::string pathname;
    int fd;

    SequenceFile(const std::string& pathname);
    ~SequenceFile();

    /**
     * Open the sequence file.
     *
     * This is not automatically done in the constructor, because Writer, when
     * truncating, needs to have the time to recreate its directory before
     * creating the sequence file.
     */
    void open();

    /**
     * Increment the sequence and get the pathname of the file corresponding
     * to the new value, and the corresponding 'offset', that is, the file
     * sequence number itself.
     */
    std::pair<std::string, size_t> next(const std::string& format);

    /**
     * Call next() then open its result with O_EXCL; retry with a higher number
     * if the file already exists.
     *
     * Returns the open file descriptor, and the corresponding 'offset', that
     * is, the file sequence number itself.
     */
    void open_next(const std::string& format, std::string& absname, size_t& pos, int& fd);

    static std::string data_fname(size_t pos, const std::string& format);
};

class Writer : public data::Writer
{
protected:
    std::string format;
    SequenceFile seqfile;

    /**
     * Write the data in md to a file. Delete the file in case of errors. Close fd
     * in any case. Return the size of the data that has been written.
     */
    virtual size_t write_file(const Metadata& md, int fd, const std::string& absname);

public:
    Writer(const std::string& format, const std::string& relname, const std::string& absname, bool truncate=false);
    ~Writer();

    virtual void append(Metadata& md);
    virtual off_t append(const wibble::sys::Buffer& buf);
    virtual Pending append(Metadata& md, off_t* ofs);
    /**
     * Append a hardlink to the data pointed by md.
     *
     * This of course only works if it is possible to hardlink from this
     * segment to the file pointed by md. That is, if they are in the same file
     * system.
     *
     * @returns the offset in the segment at which md was appended
     */
    off_t link(const std::string& absname);
};

/**
 * Same as Writer, but uses ftruncate to write dummy empty file with the
 * original size but that take up no blocks in the file system. This is used
 * only for testing.
 */
class HoleWriter : public Writer
{
protected:
    virtual size_t write_file(const Metadata& md, int fd, const std::string& absname);

public:
    HoleWriter(const std::string& format, const std::string& relname, const std::string& absname, bool truncate=false);
};

struct Maint : public data::Maint
{
    FileState check(const std::string& absname, const metadata::Collection& mds, bool quick=true);
    size_t remove(const std::string& absname);
    void truncate(const std::string& absname, size_t offset);
    Pending repack(const std::string& rootdir, const std::string& relname, metadata::Collection& mds);

protected:
    virtual std::auto_ptr<dir::Writer> make_writer(const std::string& format, const std::string& relname, const std::string& absname);
};

struct HoleMaint : public Maint
{
    FileState check(const std::string& absname, const metadata::Collection& mds, bool quick=true);

protected:
    virtual std::auto_ptr<dir::Writer> make_writer(const std::string& format, const std::string& relname, const std::string& absname);
};

class OstreamWriter : public data::OstreamWriter
{
protected:
    sigset_t blocked;

public:
    OstreamWriter();
    virtual ~OstreamWriter();

    virtual size_t stream(Metadata& md, std::ostream& out) const;
    virtual size_t stream(Metadata& md, int out) const;
};


}
}
}
}

#endif
