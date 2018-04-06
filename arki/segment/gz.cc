#include "gz.h"
#include "arki/exceptions.h"
#include "arki/metadata.h"
#include "arki/metadata/collection.h"
#include "arki/types/source/blob.h"
#include "arki/scan/any.h"
#include "arki/utils/compress.h"
#include "arki/utils/files.h"
#include "arki/utils/string.h"
#include "arki/utils/sys.h"
#include "arki/utils/tar.h"
#include "arki/utils.h"
#include "arki/dataset/reporter.h"
#include "arki/nag.h"
#include <fcntl.h>
#include <vector>
#include <algorithm>

using namespace std;
using namespace arki::core;
using namespace arki::types;
using namespace arki::utils;

namespace arki {
namespace segment {
namespace gz {

Checker::Checker(const std::string& root, const std::string& relpath, const std::string& abspath)
    : segment::Checker(root, relpath, abspath), gzabspath(abspath + ".gz")
{
}

const char* Checker::type() const { return "gz"; }
bool Checker::single_file() const { return true; }

bool Checker::exists_on_disk()
{
    return sys::exists(gzabspath);
}

time_t Checker::timestamp()
{
    return sys::timestamp(gzabspath);
}

size_t Checker::size()
{
    return sys::size(gzabspath);
}

void Checker::move_data(const std::string& new_root, const std::string& new_relpath, const std::string& new_abspath)
{
    sys::rename(gzabspath, new_abspath + ".gz");
}

State Checker::check(dataset::Reporter& reporter, const std::string& ds, const metadata::Collection& mds, bool quick)
{
    std::unique_ptr<struct stat> st = sys::stat(gzabspath);
    if (st.get() == nullptr)
        return SEGMENT_DELETED;

    // Decompress all the segment in memory
    std::vector<uint8_t> all_data = compress::gunzip(gzabspath);

    if (!quick)
    {
        const scan::Validator* validator = &scan::Validator::by_filename(abspath.c_str());

        for (const auto& i: mds)
        {
            if (const types::source::Blob* blob = i->has_source_blob())
            {
                if (blob->filename != relpath)
                    throw std::runtime_error("metadata to validate does not appear to be from this segment");

                if (blob->offset + blob->size > all_data.size())
                {
                    reporter.segment_info(ds, relpath, "data is supposed to be past the end of the uncompressed segment");
                    return SEGMENT_UNALIGNED;
                }

                try {
                    validator->validate_buf(all_data.data() + blob->offset, blob->size);
                } catch (std::exception& e) {
                    stringstream out;
                    out << "validation failed at " << i->source() << ": " << e.what();
                    reporter.segment_info(ds, relpath, out.str());
                    return SEGMENT_UNALIGNED;
                }
            } else {
                try {
                    const auto& buf = i->getData();
                    validator->validate_buf(buf.data(), buf.size());
                } catch (std::exception& e) {
                    stringstream out;
                    out << "validation failed at " << i->source() << ": " << e.what();
                    reporter.segment_info(ds, relpath, out.str());
                    return SEGMENT_UNALIGNED;
                }
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
    off_t file_size = all_data.size();
    if (file_size < end_of_last_data_checked)
    {
        stringstream ss;
        ss << "file looks truncated: its size is " << file_size << " but data is known to exist until " << end_of_last_data_checked << " bytes";
        reporter.segment_info(ds, relpath, ss.str());
        return SEGMENT_UNALIGNED;
    }

    // Check if file_size matches the expected file size
    bool has_hole = false;
    if (file_size > end_of_last_data_checked) // last padding plus two trailing zero blocks
        has_hole = true;

    // Check for holes or elements out of order
    end_of_last_data_checked = 0;
    for (metadata::Collection::const_iterator i = mds.begin(); i != mds.end(); ++i)
    {
        const source::Blob& source = (*i)->sourceBlob();
        if (source.offset < (size_t)end_of_last_data_checked || source.offset > (size_t)end_of_last_data_checked) // last padding plus file header
            has_hole = true;

        end_of_last_data_checked = max(end_of_last_data_checked, (off_t)(source.offset + source.size));
    }

    // Take note of files with holes
    if (has_hole)
    {
        nag::verbose("%s: contains deleted data or data to be reordered", abspath.c_str());
        return SEGMENT_DIRTY;
    } else {
        return SEGMENT_OK;
    }
}

size_t Checker::remove()
{
    size_t size = sys::size(gzabspath);
    sys::unlink(gzabspath);
    return size;
}

Pending Checker::repack(const std::string& rootdir, metadata::Collection& mds, unsigned test_flags)
{
    struct Rename : public Transaction
    {
        sys::File src;
        std::string tmpabspath;
        std::string abspath;
        bool fired = false;

        Rename(const std::string& tmpabspath, const std::string& abspath)
            : src(abspath, O_RDWR), tmpabspath(tmpabspath), abspath(abspath)
        {
        }

        virtual ~Rename()
        {
            if (!fired) rollback();
        }

        void commit() override
        {
            if (fired) return;
            // Rename the data file to its final name
            if (rename(tmpabspath.c_str(), abspath.c_str()) < 0)
                throw_system_error("cannot rename " + tmpabspath + " to " + abspath);
            fired = true;
        }

        void rollback() override
        {
            if (fired) return;
            sys::unlink(tmpabspath);
            fired = true;
        }

        void rollback_nothrow() noexcept override
        {
            if (fired) return;
            sys::unlink(tmpabspath);
            fired = true;
        }
    };

    string tmpabspath = gzabspath + ".repack";

    // Make sure mds are not holding a read lock on the file to repack
    for (auto& md: mds) md->sourceBlob().unlock();

    // Get a validator for this file
    const scan::Validator& validator = scan::Validator::by_filename(abspath);

    // Reacquire the lock here for writing
    Rename* rename;
    Pending p(rename = new Rename(tmpabspath, abspath));

    // Create a writer for the temp file
    File fd(tmpabspath, O_WRONLY | O_CREAT | O_TRUNC, 0666);
    TarOutput tarfd(fd);

    if (test_flags & TEST_MISCHIEF_MOVE_DATA)
        tarfd.append("mischief", "test data intended to create a gap");

    // Fill the temp file with all the data in the right order
    size_t idx = 0;
    char fname[100];
    std::string tarrelpath = relpath + ".tar";
    for (metadata::Collection::const_iterator i = mds.begin(); i != mds.end(); ++i)
    {
        // Read the data
        auto buf = (*i)->sourceBlob().read_data(rename->src, false);
        // Validate it
        validator.validate_buf(buf.data(), buf.size());
        // Append it to the new file
        snprintf(fname, 99, "%06zd.%s", idx, (*i)->source().format.c_str());
        off_t wrpos = tarfd.append(fname, buf);
        auto new_source = Source::createBlobUnlocked((*i)->source().format, rootdir, tarrelpath, wrpos, buf.size());
        // Update the source information in the metadata
        (*i)->set_source(std::move(new_source));
        // Drop the cached data, to prevent ending up with the whole segment
        // sitting in memory
        (*i)->drop_cached_data();
    }

    tarfd.end();
    fd.fdatasync();
    fd.close();

    return p;
}

void Checker::create(const std::string& rootdir, const std::string& relpath, const std::string& abspath, metadata::Collection& mds, unsigned test_flags)
{
    string gzabspath = abspath + ".gz";
    File fd(gzabspath, O_WRONLY | O_CREAT | O_TRUNC, 0666);
    compress::GzipWriter gzout(fd);

    // Fill the temp file with all the data in the right order
    size_t ofs = 0;
    for (auto& md: mds)
    {
        // Read the data
        auto buf = md->sourceBlob().read_data();
        gzout.add(buf);
        auto new_source = Source::createBlobUnlocked(md->source().format, rootdir, relpath, ofs, buf.size());
        // Update the source information in the metadata
        md->set_source(std::move(new_source));
        // Drop the cached data, to prevent ending up with the whole segment
        // sitting in memory
        md->drop_cached_data();
        ofs += buf.size();
    }

    gzout.flush();
    fd.fdatasync();
    fd.close();
}

void Checker::test_truncate(size_t offset)
{
    if (!sys::exists(abspath))
        utils::createFlagfile(abspath);

    if (offset % 512 != 0)
        offset += 512 - (offset % 512);

    utils::files::PreserveFileTimes pft(abspath);
    if (::truncate(abspath.c_str(), offset) < 0)
    {
        stringstream ss;
        ss << "cannot truncate " << abspath << " at " << offset;
        throw std::system_error(errno, std::system_category(), ss.str());
    }
}

void Checker::test_make_hole(metadata::Collection& mds, unsigned hole_size, unsigned data_idx)
{
    throw std::runtime_error("test_make_hole not implemented");
}

void Checker::test_make_overlap(metadata::Collection& mds, unsigned overlap_size, unsigned data_idx)
{
    throw std::runtime_error("test_make_overlap not implemented");
}

void Checker::test_corrupt(const metadata::Collection& mds, unsigned data_idx)
{
    const auto& s = mds[data_idx].sourceBlob();
    sys::File fd(abspath, O_RDWR);
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

namespace gzlines
{

void Checker::create(const std::string& rootdir, const std::string& relpath, const std::string& abspath, metadata::Collection& mds, unsigned test_flags)
{
    string gzabspath = abspath + ".gz";
    File fd(gzabspath, O_WRONLY | O_CREAT | O_TRUNC, 0666);
    compress::GzipWriter gzout(fd);

    std::vector<uint8_t> newline(1, '\n');

    // Fill the temp file with all the data in the right order
    size_t ofs = 0;
    for (auto& md: mds)
    {
        // Read the data
        auto buf = md->sourceBlob().read_data();
        gzout.add(buf);
        gzout.add(newline);
        auto new_source = Source::createBlobUnlocked(md->source().format, rootdir, relpath, ofs, buf.size());
        // Update the source information in the metadata
        md->set_source(std::move(new_source));
        // Drop the cached data, to prevent ending up with the whole segment
        // sitting in memory
        md->drop_cached_data();
        ofs += buf.size() + 1;
    }

    gzout.flush();
    fd.fdatasync();
    fd.close();
}

bool can_store(const std::string& format)
{
    return format == "vm2";
}

}

}
}


