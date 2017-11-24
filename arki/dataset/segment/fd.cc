#include "fd.h"
#include "arki/exceptions.h"
#include "arki/metadata.h"
#include "arki/metadata/collection.h"
#include "arki/types/source/blob.h"
#include "arki/scan/any.h"
#include "arki/utils/compress.h"
#include "arki/utils/files.h"
#include "arki/utils/string.h"
#include "arki/utils/sys.h"
#include "arki/utils/lock.h"
#include "arki/utils.h"
#include "arki/dataset/reporter.h"
#include "arki/nag.h"
#include <algorithm>
#include <system_error>
#include <sys/types.h>
#include <sys/stat.h>
#include <sys/mman.h>
#include <fcntl.h>
#include <unistd.h>
#include <cstring>

using namespace std;
using namespace arki::types;
using namespace arki::utils;

namespace arki {
namespace dataset {
namespace segment {
namespace fd {

void File::fdtruncate(off_t pos)
{
    if (::ftruncate(*this, pos) == -1)
        nag::warning("truncating %s to previous size %zd (rollback of append operation): %m", name().c_str(), pos);
}


namespace {

struct Append : public Transaction
{
    fd::Writer& w;
    File& fd;
    Metadata& md;
    bool fired = false;
    const std::vector<uint8_t>& buf;
    off_t pos;
    std::unique_ptr<types::source::Blob> new_source;

    Append(fd::Writer& w, Metadata& md)
        : w(w), fd(*w.fd), md(md), buf(md.getData()), pos(fd.lseek(0, SEEK_END))
    {
        new_source = source::Blob::create_unlocked(md.source().format, w.root, w.relname, pos, buf.size());
    }

    virtual ~Append()
    {
        if (!fired) rollback();
    }

    void commit() override
    {
        if (fired) return;

        // Append the data
        fd.write_data(pos, buf);

        // Set the source information that we are writing in the metadata
        md.set_source(move(new_source));

        fired = true;
    }

    void rollback() override
    {
        if (fired) return;

        // If we had a problem, attempt to truncate the file to the original size
        fd.fdtruncate(pos);
        fired = true;
    }
};

}


Writer::Writer(const std::string& root, const std::string& relname, std::unique_ptr<File> fd)
    : segment::Writer(root, relname, fd->name()), fd(fd.release())
{
}

Writer::~Writer()
{
    delete fd;
}

Pending Writer::append(Metadata& md, const types::source::Blob** new_source)
{
    Append* res = new Append(*this, md);
    if (new_source) *new_source = res->new_source.get();
    return res;
}

off_t Writer::append(const std::vector<uint8_t>& buf)
{
    off_t pos = fd->lseek(0, SEEK_END);
    try {
        fd->write_data(pos, buf);
    } catch (...) {
        fd->fdtruncate(pos);
        throw;
    }
    return pos;
}


State Checker::check_fd(dataset::Reporter& reporter, const std::string& ds, const metadata::Collection& mds, unsigned max_gap, bool quick)
{
    // Check the data if requested
    if (!quick)
    {
        const scan::Validator* validator = &scan::Validator::by_filename(absname.c_str());

        for (const auto& i: mds)
        {
            try {
                validate(*i, *validator);
            } catch (std::exception& e) {
                stringstream out;
                out << "validation failed at " << i->source() << ": " << e.what();
                reporter.segment_info(ds, relname, out.str());
                return SEGMENT_UNALIGNED;
            }
        }
    }

    // Create the list of data (offset, size) sorted by offset
    vector< pair<off_t, size_t> > spans;
    for (metadata::Collection::const_iterator i = mds.begin(); i != mds.end(); ++i)
    {
        const source::Blob& source = (*i)->sourceBlob();
        spans.push_back(make_pair(source.offset, source.size));
    }
    std::sort(spans.begin(), spans.end());

    // Check for overlaps
    off_t end_of_last_data_checked = 0;
    for (vector< pair<off_t, size_t> >::const_iterator i = spans.begin(); i != spans.end(); ++i)
    {
        // If an item begins after the end of another, they overlap and the file needs rescanning
        if (i->first < end_of_last_data_checked)
            return SEGMENT_UNALIGNED;

        end_of_last_data_checked = i->first + i->second;
    }

    // Check for truncation
    off_t file_size = utils::compress::filesize(absname);
    if (file_size < end_of_last_data_checked)
    {
        stringstream ss;
        ss << "file looks truncated: its size is " << file_size << " but data is known to exist until " << end_of_last_data_checked << " bytes";
        reporter.segment_info(ds, relname, ss.str());
        return SEGMENT_UNALIGNED;
    }

    // Check if file_size matches the expected file size
    bool has_hole = false;
    if (file_size > end_of_last_data_checked + max_gap)
        has_hole = true;

    // Check for holes or elements out of order
    end_of_last_data_checked = 0;
    for (metadata::Collection::const_iterator i = mds.begin(); i != mds.end(); ++i)
    {
        const source::Blob& source = (*i)->sourceBlob();
        if (source.offset < (size_t)end_of_last_data_checked || source.offset > (size_t)end_of_last_data_checked + max_gap)
            has_hole = true;

        end_of_last_data_checked = max(end_of_last_data_checked, (off_t)(source.offset + source.size));
    }

    // Take note of files with holes
    if (has_hole)
    {
        nag::verbose("%s: contains deleted data or data to be reordered", absname.c_str());
        return SEGMENT_DIRTY;
    } else {
        return SEGMENT_OK;
    }
}

void Checker::validate(Metadata& md, const scan::Validator& v)
{
    if (const types::source::Blob* blob = md.has_source_blob()) {
        if (blob->filename != relname)
            throw std::runtime_error("metadata to validate does not appear to be from this segment");

        if (sys::exists(absname))
        {
            sys::File fd(absname, O_RDONLY);
            v.validate_file(fd, blob->offset, blob->size);
            return;
        }
        // If the file does not exist, it may be a compressed segment.
        // TODO: Until handling of compressed segments is incorporated here, we
        // just let Metadata::getData see if it can load the data somehow.
    }
    const auto& buf = md.getData();
    v.validate_buf(buf.data(), buf.size());
}

size_t Checker::remove()
{
    size_t size = sys::size(absname);
    sys::unlink(absname.c_str());
    return size;
}

Pending Checker::repack_impl(
        const std::string& rootdir,
        metadata::Collection& mds,
        bool skip_validation,
        unsigned test_flags)
{
    struct Rename : public Transaction
    {
        sys::File src;
        std::string tmpabsname;
        std::string absname;
        bool fired = false;

        Rename(const std::string& tmpabsname, const std::string& absname)
            : src(absname, O_RDWR), tmpabsname(tmpabsname), absname(absname)
        {
            repack_lock();
        }

        virtual ~Rename()
        {
            if (!fired) rollback();
        }

        /**
         * Lock the whole segment for repacking
         */
        void repack_lock()
        {
            Lock lock;
            lock.l_type = F_WRLCK;
            lock.l_whence = SEEK_SET;
            lock.l_start = 0;
            lock.l_len = 0;
            lock.ofd_setlkw(src);
        }

        /**
         * Unlock the segment after repacking
         */
        void repack_unlock()
        {
            Lock lock;
            lock.l_type = F_UNLCK;
            lock.l_whence = SEEK_SET;
            lock.l_start = 0;
            lock.l_len = 0;
            lock.ofd_setlk(src);
        }

        virtual void commit()
        {
            if (fired) return;
            // Rename the data file to its final name
            try {
                if (rename(tmpabsname.c_str(), absname.c_str()) < 0)
                    throw_system_error("cannot rename " + tmpabsname + " to " + absname);
            } catch (...) {
                repack_unlock();
                throw;
            }
            repack_unlock();
            fired = true;
        }

        virtual void rollback()
        {
            if (fired) return;
            unlink(tmpabsname.c_str());
            repack_unlock();
            fired = true;
        }
    };

    string tmprelname = relname + ".repack";
    string tmpabsname = absname + ".repack";

    // Make sure mds are not holding a read lock on the file to repack
    for (auto& md: mds) md->sourceBlob().unlock();

    // Get a validator for this file
    const scan::Validator& validator = scan::Validator::by_filename(absname);

    // Reacquire the lock here for writing
    Rename* rename;
    Pending p(rename = new Rename(tmpabsname, absname));

    // Create a writer for the temp file
    auto writer(make_tmp_segment(tmprelname, tmpabsname));

    if (test_flags & TEST_MISCHIEF_MOVE_DATA)
        writer->fd->test_add_padding(1);

    // Fill the temp file with all the data in the right order
    for (metadata::Collection::const_iterator i = mds.begin(); i != mds.end(); ++i)
    {
        // Read the data
        auto buf = (*i)->sourceBlob().read_data(rename->src, false);
        // Validate it
        if (!skip_validation)
            validator.validate_buf(buf.data(), buf.size());
        // Append it to the new file
        off_t w_off = writer->append(buf);
        // Update the source information in the metadata
        (*i)->set_source(Source::createBlobUnlocked((*i)->source().format, rootdir, relname, w_off, buf.size()));
        // Drop the cached data, to prevent ending up with the whole segment
        // sitting in memory
        (*i)->drop_cached_data();
    }

    return p;
}

void Checker::test_truncate(size_t offset)
{
    if (!sys::exists(absname))
        utils::createFlagfile(absname);

    utils::files::PreserveFileTimes pft(absname);
    if (::truncate(absname.c_str(), offset) < 0)
    {
        stringstream ss;
        ss << "cannot truncate " << absname << " at " << offset;
        throw std::system_error(errno, std::system_category(), ss.str());
    }
}

void Checker::test_make_hole(metadata::Collection& mds, unsigned hole_size, unsigned data_idx)
{
    arki::File fd(absname, O_RDWR);
    sys::PreserveFileTimes pt(fd);
    off_t end = fd.lseek(0, SEEK_END);
    if (data_idx >= mds.size())
    {
        fd.ftruncate(end + hole_size);
    } else {
        off_t start_ofs = mds[data_idx].sourceBlob().offset;
        std::vector<uint8_t> buf(end - start_ofs);
        fd.lseek(start_ofs);
        fd.read_all_or_throw(buf.data(), buf.size());
        fd.lseek(start_ofs + hole_size);
        fd.write_all_or_throw(buf.data(), buf.size());

        for (unsigned i = data_idx; i < mds.size(); ++i)
        {
            unique_ptr<source::Blob> source(mds[i].sourceBlob().clone());
            source->offset += hole_size;
            mds[i].set_source(move(source));
        }
    }
}

void Checker::test_make_overlap(metadata::Collection& mds, unsigned overlap_size, unsigned data_idx)
{
    arki::File fd(absname, O_RDWR);
    sys::PreserveFileTimes pt(fd);
    off_t start_ofs = mds[data_idx].sourceBlob().offset;
    off_t end = fd.lseek(0, SEEK_END);
    std::vector<uint8_t> buf(end - start_ofs);
    fd.lseek(start_ofs);
    fd.read_all_or_throw(buf.data(), buf.size());
    fd.lseek(start_ofs - overlap_size);
    fd.write_all_or_throw(buf.data(), buf.size());
    fd.ftruncate(end - overlap_size);

    for (unsigned i = data_idx; i < mds.size(); ++i)
    {
        unique_ptr<source::Blob> source(mds[i].sourceBlob().clone());
        source->offset -= overlap_size;
        mds[i].set_source(move(source));
    }
}

void Checker::test_corrupt(const metadata::Collection& mds, unsigned data_idx)
{
    const auto& s = mds[data_idx].sourceBlob();
    arki::File fd(absname, O_RDWR);
    sys::PreserveFileTimes pt(fd);
    fd.lseek(s.offset);
    fd.write_all_or_throw("\0", 1);
}


bool can_store(const std::string& format)
{
    return format == "grib" || format == "grib1" || format == "grib2"
        || format == "bufr" || format == "vm2";
}

}
}
}
}
