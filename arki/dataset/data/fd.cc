#include "fd.h"
#include "arki/metadata.h"
#include "arki/metadata/collection.h"
#include "arki/types/source/blob.h"
#include "arki/scan/any.h"
#include "arki/utils/compress.h"
#include "arki/utils/files.h"
#include "arki/utils/string.h"
#include "arki/utils/sys.h"
#include "arki/utils.h"
#include "arki/nag.h"
#include <algorithm>
#include <sys/types.h>
#include <sys/stat.h>
#include <fcntl.h>
#include <unistd.h>

using namespace std;
using namespace arki::types;
using namespace arki::utils;

namespace arki {
namespace dataset {
namespace data {
namespace fd {

Segment::Segment(const std::string& relname, const std::string& absname)
    : data::Segment(relname, absname), fd(-1)
{
}

Segment::~Segment()
{
    close();
}

void Segment::open()
{
    if (fd != -1) return;

    // Open the data file
    fd = ::open(absname.c_str(), O_WRONLY | O_CREAT | O_APPEND, 0666);
    if (fd == -1)
        throw wibble::exception::File(absname, "cannot open file for appending data");
}

void Segment::truncate_and_open()
{
    if (fd != -1) return;

    // Open the data file
    fd = ::open(absname.c_str(), O_WRONLY | O_CREAT | O_TRUNC, 0666);
    if (fd == -1)
        throw wibble::exception::File(absname, "cannot truncate and open file for appending data");
}

void Segment::close()
{
    //if (fdatasync(fd_dst) != 0) throw wibble::exception::File(dst, "flushing data to file");
    if (fd != -1) ::close(fd);
    fd = -1;
}

void Segment::lock()
{
    struct flock lock;
    lock.l_type = F_WRLCK;
    lock.l_whence = SEEK_SET;
    lock.l_start = 0;
    lock.l_len = 0;
    // Use SETLKW, so that if it is already locked, we just wait
    if (fcntl(fd, F_SETLKW, &lock) == -1)
        throw wibble::exception::System("locking the file " + absname + " for writing");
}

void Segment::unlock()
{
    struct flock lock;
    lock.l_type = F_UNLCK;
    lock.l_whence = SEEK_SET;
    lock.l_start = 0;
    lock.l_len = 0;
    fcntl(fd, F_SETLK, &lock);
}

off_t Segment::wrpos()
{
    // Get the write position in the data file
    off_t size = lseek(fd, 0, SEEK_END);
    if (size == (off_t)-1)
        throw wibble::exception::File(absname, "reading the current position");
    return size;
}

void Segment::fdtruncate(off_t pos)
{
    open();

    if (ftruncate(fd, pos) == -1)
        nag::warning("truncating %s to previous size %zd (rollback of append operation): %m", absname.c_str(), pos);
}

void Segment::write(const std::vector<uint8_t>& buf)
{
    // Prevent caching (ignore function result)
    //(void)posix_fadvise(df.fd, pos, buf.size(), POSIX_FADV_DONTNEED);

    // Append the data
    ssize_t res = ::write(fd, buf.data(), buf.size());
    if (res < 0 || (unsigned)res != buf.size())
    {
        stringstream ss;
        ss << "cannot write " << buf.size() << " bytes to " << absname;
        throw std::system_error(errno, std::system_category(), ss.str());
    }

    if (fdatasync(fd) < 0)
    {
        stringstream ss;
        ss << "cannot flush write to " << absname;
        throw std::system_error(errno, std::system_category(), ss.str());
    }
}

FileState Segment::check(const metadata::Collection& mds, unsigned max_gap, bool quick)
{
    close();

    // Check the data if requested
    if (!quick)
    {
        const scan::Validator* validator = &scan::Validator::by_filename(absname.c_str());

        for (metadata::Collection::const_iterator i = mds.begin(); i != mds.end(); ++i)
        {
            try {
                validator->validate(**i);
            } catch (std::exception& e) {
                stringstream out;
                out << (*i)->source();
                nag::warning("%s: validation failed at %s: %s", absname.c_str(), out.str().c_str(), e.what());
                return FILE_TO_RESCAN;
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
    size_t end_of_last_data_checked = 0;
    for (vector< pair<off_t, size_t> >::const_iterator i = spans.begin(); i != spans.end(); ++i)
    {
        // If an item begins after the end of another, they overlap and the file needs rescanning
        if (i->first < end_of_last_data_checked)
            return FILE_TO_RESCAN;

        end_of_last_data_checked = i->first + i->second;
    }

    // Check for truncation
    size_t file_size = utils::compress::filesize(absname);
    if (file_size < end_of_last_data_checked)
    {
        nag::warning("%s: file looks truncated: its size is %zd but data is known to exist until %zd bytes", absname.c_str(), file_size, (size_t)end_of_last_data_checked);
        return FILE_TO_RESCAN;
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

        if (source.offset < end_of_last_data_checked || source.offset > end_of_last_data_checked + max_gap)
            has_hole = true;

        end_of_last_data_checked = max(end_of_last_data_checked, (size_t)(source.offset + source.size));
    }

    // Take note of files with holes
    if (has_hole)
    {
        nag::verbose("%s: contains deleted data or data to be reordered", absname.c_str());
        return FILE_TO_PACK;
    } else {
        return FILE_OK;
    }
}

size_t Segment::remove()
{
    close();

    size_t size = sys::size(absname);
    if (unlink(absname.c_str()) < 0)
        throw wibble::exception::System("removing " + absname);
    return size;
}

void Segment::truncate(size_t offset)
{
    close();

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

Pending Segment::repack(
        const std::string& rootdir,
        const std::string& relname,
        metadata::Collection& mds,
        data::Segment* make_repack_segment(const std::string&, const std::string&),
        bool skip_validation)
{
    struct Rename : public Transaction
    {
        std::string tmpabsname;
        std::string absname;
        bool fired;

        Rename(const std::string& tmpabsname, const std::string& absname)
            : tmpabsname(tmpabsname), absname(absname), fired(false)
        {
        }

        virtual ~Rename()
        {
            if (!fired) rollback();
        }

        virtual void commit()
        {
            if (fired) return;
            // Rename the data file to its final name
            if (rename(tmpabsname.c_str(), absname.c_str()) < 0)
                throw wibble::exception::System("renaming " + tmpabsname + " to " + absname);
            fired = true;
        }

        virtual void rollback()
        {
            if (fired) return;
            unlink(tmpabsname.c_str());
            fired = true;
        }
    };

    string absname = str::joinpath(rootdir, relname);
    string tmprelname = relname + ".repack";
    string tmpabsname = absname + ".repack";

    // Get a validator for this file
    const scan::Validator& validator = scan::Validator::by_filename(absname);

    // Create a writer for the temp file
    unique_ptr<data::Segment> writer(make_repack_segment(tmprelname, tmpabsname));

    // Fill the temp file with all the data in the right order
    for (metadata::Collection::const_iterator i = mds.begin(); i != mds.end(); ++i)
    {
        // Read the data
        const auto& buf = (*i)->getData();
        // Validate it
        if (!skip_validation)
            validator.validate(buf.data(), buf.size());
        // Append it to the new file
        off_t w_off = writer->append(buf);
        // Update the source information in the metadata
        (*i)->set_source(Source::createBlob((*i)->source().format, rootdir, relname, w_off, buf.size()));
        // Drop the cached data, to prevent ending up with the whole segment
        // sitting in memory
        (*i)->drop_cached_data();
    }

    // Close the temp file
    writer.release();

    return new Rename(tmpabsname, absname);
}

}
}
}
}
