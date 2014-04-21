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
#include "dir.h"
#include "arki/metadata.h"
#include "arki/metadata/collection.h"
#include "arki/utils.h"
#include "arki/utils/fd.h"
#include "arki/utils/files.h"
#include "arki/scan/any.h"
#include <wibble/exception.h>
#include <wibble/string.h>
#include <wibble/sys/buffer.h>
#include <wibble/sys/fs.h>
#include <cerrno>
#include <stdint.h>
#include <sys/types.h>
#include <sys/stat.h>
#include <fcntl.h>
#include <unistd.h>
#include <fcntl.h>

using namespace std;
using namespace wibble;

namespace arki {
namespace dataset {
namespace data {
namespace dir {

namespace {

struct FdLock
{
    const std::string& pathname;
    int fd;
    struct flock lock;

    FdLock(const std::string& pathname, int fd) : pathname(pathname), fd(fd)
    {
        lock.l_type = F_WRLCK;
        lock.l_whence = SEEK_SET;
        lock.l_start = 0;
        lock.l_len = 0;
        // Use SETLKW, so that if it is already locked, we just wait
        if (fcntl(fd, F_SETLKW, &lock) == -1)
            throw wibble::exception::File(pathname, "locking the file for writing");
    }

    ~FdLock()
    {
        lock.l_type = F_UNLCK;
        fcntl(fd, F_SETLK, &lock);
    }
};

struct SequenceFile
{
    std::string pathname;
    int fd;

    SequenceFile(const std::string& pathname)
        : pathname(pathname), fd(-1)
    {
        fd = open(pathname.c_str(), O_RDWR | O_CREAT | O_CLOEXEC | O_NOATIME | O_NOFOLLOW, 0666);
        if (fd == -1)
            throw wibble::exception::File(pathname, "cannot open sequence file");
    }

    ~SequenceFile()
    {
        if (fd != -1)
            close(fd);
    }

    size_t next()
    {
        FdLock lock(pathname, fd);
        uint64_t cur;

        // Read the value in the sequence file
        ssize_t count = pread(fd, &cur, sizeof(cur), 0);
        if (count < 0)
            throw wibble::exception::File(pathname, "cannot read sequence file");

        if ((size_t)count < sizeof(cur))
            cur = 0;
        else
            ++cur;

        // Write it out
        count = pwrite(fd, &cur, sizeof(cur), 0);
        if (count < 0)
            throw wibble::exception::File(pathname, "cannot write sequence file");

        return (size_t)cur;
    }
};

/// Write the buffer to a file. If the file already exists, does nothing and returns false
bool add_file(const std::string& absname, const wibble::sys::Buffer& buf)
{
    int fd = open(absname.c_str(), O_WRONLY | O_CLOEXEC | O_CREAT | O_EXCL, 0666);
    if (fd == -1)
    {
        if (errno == EEXIST) return false;
        throw wibble::exception::File(absname, "cannot create file");
    }

    utils::fd::HandleWatch hw(absname, fd);

    ssize_t count = pwrite(fd, buf.data(), buf.size(), 0);
    if (count < 0)
        throw wibble::exception::File(absname, "cannot write file");

    if (fdatasync(fd) < 0)
        throw wibble::exception::File(absname, "flushing write");

    return true;
}

std::string data_fname(size_t pos, const std::string& format)
{
    return str::fmtf("%06zd.%s", pos, format.c_str());
}

struct Append : public Transaction
{
    bool fired;
    Metadata& md;
    string absname;
    size_t pos;
    size_t size;

    Append(Metadata& md, std::string& absname, size_t pos, size_t size)
        : fired(false), md(md), absname(absname), pos(pos), size(size)
    {
    }

    virtual ~Append()
    {
        if (!fired) rollback();
    }

    virtual void commit()
    {
        if (fired) return;

        // Set the source information that we are writing in the metadata
        md.source = types::source::Blob::create(md.source->format, "", absname, pos, size);

        fired = true;
    }

    virtual void rollback()
    {
        if (fired) return;

        // If we had a problem, remove the file that we have created. The
        // sequence will have skipped one number, but we do not need to
        // guarantee consecutive numbers, so it is just a cosmetic issue. This
        // case should be rare enough that even the cosmetic issue should
        // rarely be noticeable.
        string target_name = str::joinpath(absname, data_fname(pos, md.source->format));
        unlink(target_name.c_str());

        fired = true;
    }
};

}

Writer::Writer(const std::string& format, const std::string& relname, const std::string& absname, bool truncate)
    : data::Writer(relname, absname), format(format), seqfile(str::joinpath(absname, ".sequence"))
{
    if (truncate && sys::fs::exists(absname)) sys::fs::rmtree(absname);

    // Ensure that the directory 'absname' exists
    wibble::sys::fs::mkpath(absname);
}

Writer::~Writer()
{
}

void Writer::append(Metadata& md)
{
    SequenceFile sf(seqfile);
    sys::Buffer buf = md.getData();
    size_t pos = 0;
    while (true)
    {
        pos = sf.next();
        string fname = str::joinpath(absname, data_fname(pos, format));
        if (add_file(fname, buf))
            break;
    }

    // Set the source information that we are writing in the metadata
    md.source = types::source::Blob::create(md.source->format, "", absname, pos, buf.size());
}

off_t Writer::append(const wibble::sys::Buffer& buf)
{
    throw wibble::exception::Consistency("dir::Writer::append not implemented");
}

Pending Writer::append(Metadata& md, off_t* ofs)
{
    SequenceFile sf(seqfile);
    sys::Buffer buf = md.getData();
    string target_name;
    size_t pos = 0;
    size_t size = buf.size();
    while (true)
    {
        pos = sf.next();
        target_name = str::joinpath(absname, data_fname(pos, format));
        if (add_file(target_name, buf))
            break;
    }

    *ofs = pos;

    return new Append(md, absname, pos, size);
}

off_t Writer::link(const std::string& srcabsname)
{
    SequenceFile sf(seqfile);
    std::string target_name;
    size_t pos = 0;
    while (true)
    {
        pos = sf.next();
        target_name = str::joinpath(absname, data_fname(pos, format));
        if (::link(srcabsname.c_str(), target_name.c_str()) == 0)
            break;
        if (errno != EEXIST)
            throw wibble::exception::System("cannot link " + absname + " as " + target_name);
    }
    return pos;
}

FileState Writer::check(const std::string& absname, const metadata::Collection& mds, bool quick)
{
    size_t next_sequence_expected(0);
    const scan::Validator* validator(0);
    bool out_of_order(false);
    std::string format = utils::require_format(absname);

    if (!quick)
        validator = &scan::Validator::by_filename(absname.c_str());

    // List the dir elements we know about
    set<size_t> expected;
    sys::fs::Directory dir(absname);
    for (sys::fs::Directory::const_iterator i = dir.begin(); i != dir.end(); ++i)
    {
        if (!i.isreg()) continue;
        if (!str::endsWith(*i, format)) continue;
        expected.insert((size_t)strtoul((*i).c_str(), 0, 10));
    }

    // Check the list of elements we expect for sort order, gaps, and match
    // with what is in the file system
    for (metadata::Collection::const_iterator i = mds.begin(); i != mds.end(); ++i)
    {
        if (validator)
        {
            try {
                validator->validate(*i);
            } catch (std::exception& e) {
                string source = str::fmt(i->source);
                nag::warning("%s: validation failed at %s: %s", absname.c_str(), source.c_str(), e.what());
                return FILE_TO_RESCAN;
            }
        }

        Item<types::source::Blob> source = i->source.upcast<types::source::Blob>();

        if (source->offset != next_sequence_expected)
            out_of_order = true;

        set<size_t>::const_iterator ei = expected.find(source->offset);
        if (ei == expected.end())
        {
            nag::warning("%s: expected file %zd not found in the file system", absname.c_str(), (size_t)source->offset);
            return FILE_TO_RESCAN;
        } else
            expected.erase(ei);

        ++next_sequence_expected;
    }

    if (!expected.empty())
    {
        nag::warning("%s: found %zd file(s) that the index does now know about", absname.c_str(), expected.size());
        return FILE_TO_PACK;
    }

    // Take note of files with holes
    if (out_of_order)
    {
        nag::verbose("%s: contains deleted data or data to be reordered", absname.c_str());
        return FILE_TO_PACK;
    } else {
        return FILE_OK;
    }
}

size_t Writer::remove(const std::string& absname)
{
    std::string format = utils::require_format(absname);

    size_t size = 0;
    sys::fs::Directory dir(absname);
    for (sys::fs::Directory::const_iterator i = dir.begin(); i != dir.end(); ++i)
    {
        if (!i.isreg()) continue;
        if (*i != ".sequence" && !str::endsWith(*i, format)) continue;
        string pathname = str::joinpath(absname, *i);
        size += sys::fs::size(pathname);
        if (unlink(pathname.c_str()) < 0)
            throw wibble::exception::System("removing " + pathname);
    }
    return size;
}

void Writer::truncate(const std::string& absname, size_t offset)
{
    utils::files::PreserveFileTimes pft(absname);

    // Truncate dir segment
    string format = utils::require_format(absname);
    sys::fs::Directory dir(absname);
    for (sys::fs::Directory::const_iterator i = dir.begin(); i != dir.end(); ++i)
    {
        if (!i.isreg()) continue;
        if (*i != ".sequence" && !str::endsWith(*i, format)) continue;
        if (strtoul((*i).c_str(), 0, 10) >= offset)
        {
            //cerr << "UNLINK " << absname << " -- " << *i << endl;
            sys::fs::unlink(str::joinpath(absname, *i));
        }
    }
}

Pending Writer::repack(const std::string& rootdir, const std::string& relname, metadata::Collection& mds)
{
    struct Rename : public Transaction
    {
        std::string tmpabsname;
        std::string absname;
        std::string tmppos;
        bool fired;

        Rename(const std::string& tmpabsname, const std::string& absname)
            : tmpabsname(tmpabsname), absname(absname), tmppos(absname + ".pre-repack"), fired(false)
        {
        }

        virtual ~Rename()
        {
            if (!fired) rollback();
        }

        virtual void commit()
        {
            if (fired) return;

            // It is impossible to make this atomic, so we just try to make it as quick as possible

            // Move the old directory inside the emp dir, to free the old directory name
            if (rename(absname.c_str(), tmppos.c_str()))
                throw wibble::exception::System("cannot rename " + absname + " to " + tmppos);

            // Rename the data file to its final name
            if (rename(tmpabsname.c_str(), absname.c_str()) < 0)
                throw wibble::exception::System("cannot rename " + tmpabsname + " to " + absname
                    + " (ATTENTION: please check if you need to rename " + tmppos + " to " + absname
                    + " manually to restore the dataset as it was before the repack)");

            // Remove the old data
            sys::fs::rmtree(tmppos);

            fired = true;
        }

        virtual void rollback()
        {
            if (fired) return;

            try
            {
                sys::fs::rmtree(tmpabsname);
            } catch (std::exception& e) {
                nag::warning("Failed to remove %s while recovering from a failed repack: %s", tmpabsname.c_str(), e.what());
            }

            rename(tmppos.c_str(), absname.c_str());

            fired = true;
        }
    };

    string format = utils::require_format(relname);
    string absname = str::joinpath(rootdir, relname);
    string tmprelname = relname + ".repack";
    string tmpabsname = absname + ".repack";

    // Create a writer for the temp dir
    auto_ptr<dir::Writer> writer(new dir::Writer(format, tmprelname, tmpabsname, true));

    // Fill the temp file with all the data in the right order
    for (metadata::Collection::iterator i = mds.begin(); i != mds.end(); ++i)
    {
        UItem<types::source::Blob> source = i->source.upcast<types::source::Blob>();

        // Make a hardlink in the target directory for the file pointed by *i
        off_t pos = writer->link(str::joinpath(source->absolutePathname(), data_fname(source->offset, source->format)));

        // Update the source information in the metadata
        i->source = types::source::Blob::create(source->format, rootdir, relname, pos, source->size);
    }

    // Close the temp writer
    writer.release();

    return new Rename(tmpabsname, absname);
}

OstreamWriter::OstreamWriter()
{
    throw wibble::exception::Consistency("dir::OstreamWriter not implemented");
}

OstreamWriter::~OstreamWriter()
{
}

size_t OstreamWriter::stream(Metadata& md, std::ostream& out) const
{
    throw wibble::exception::Consistency("dir::OstreamWriter::stream not implemented");
}

size_t OstreamWriter::stream(Metadata& md, int out) const
{
    throw wibble::exception::Consistency("dir::OstreamWriter::stream not implemented");
}

}
}
}
}
