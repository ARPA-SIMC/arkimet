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

namespace {

struct Creator
{
    const std::string& root;
    const std::string& relpath;
    const std::string& abspath;
    metadata::Collection& mds;
    string gzabspath;
    std::vector<uint8_t> padding;
    const scan::Validator* validator = nullptr;

    Creator(const std::string& root, const std::string& relpath, const std::string& abspath, metadata::Collection& mds)
        : root(root), relpath(relpath), abspath(abspath), mds(mds), gzabspath(abspath + ".gz")
    {
    }

    void create()
    {
        File fd(gzabspath, O_WRONLY | O_CREAT | O_TRUNC, 0666);
        compress::GzipWriter gzout(fd);

        // Fill the temp file with all the data in the right order
        size_t ofs = 0;
        for (auto& md: mds)
        {
            bool had_cached_data = md->has_cached_data();
            // Read the data
            auto buf = md->getData();
            // Validate it if requested
            if (validator)
                validator->validate_buf(buf.data(), buf.size());
            gzout.add(buf);
            if (!padding.empty())
                gzout.add(padding);
            auto new_source = Source::createBlobUnlocked(md->source().format, root, relpath, ofs, buf.size());
            // Update the source information in the metadata
            md->set_source(std::move(new_source));
            // Drop the cached data, to prevent accidentally loading the whole segment in memory
            if (!had_cached_data) md->drop_cached_data();
            ofs += buf.size() + padding.size();
        }

        gzout.flush();
        fd.fdatasync();
        fd.close();
    }
};

struct CheckBackend
{
    const std::string& gzabspath;
    const std::string& relpath;
    std::function<void(const std::string&)> reporter;
    const metadata::Collection& mds;
    bool accurate = false;
    unsigned pad_size = 0;

    CheckBackend(const std::string& gzabspath, const std::string& relpath, std::function<void(const std::string&)> reporter, const metadata::Collection& mds)
        : gzabspath(gzabspath), relpath(relpath), reporter(reporter), mds(mds)
    {
    }

    State check()
    {
        std::unique_ptr<struct stat> st = sys::stat(gzabspath);
        if (st.get() == nullptr)
            return SEGMENT_DELETED;

        // Decompress all the segment in memory
        std::vector<uint8_t> all_data = compress::gunzip(gzabspath);

        if (accurate && !mds.empty())
        {
            const scan::Validator* validator = &scan::Validator::by_format(mds[0].source().format);

            for (const auto& i: mds)
            {
                if (const types::source::Blob* blob = i->has_source_blob())
                {
                    if (blob->filename != relpath)
                        throw std::runtime_error("metadata to validate does not appear to be from this segment");

                    if (blob->offset + blob->size > all_data.size())
                    {
                        reporter("data is supposed to be past the end of the uncompressed segment");
                        return SEGMENT_UNALIGNED;
                    }

                    try {
                        validator->validate_buf(all_data.data() + blob->offset, blob->size);
                    } catch (std::exception& e) {
                        stringstream out;
                        out << "validation failed at " << i->source() << ": " << e.what();
                        reporter(out.str());
                        return SEGMENT_UNALIGNED;
                    }
                } else {
                    try {
                        const auto& buf = i->getData();
                        validator->validate_buf(buf.data(), buf.size());
                    } catch (std::exception& e) {
                        stringstream out;
                        out << "validation failed at " << i->source() << ": " << e.what();
                        reporter(out.str());
                        return SEGMENT_UNALIGNED;
                    }
                }
            }
        }

        // Create the list of data (offset, size) sorted by offset, to detect overlaps
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
            {
                stringstream out;
                out << "item at offset " << i->first << " starts before the end of the previous item at offset " << end_of_last_data_checked;
                reporter(out.str());
                return SEGMENT_UNALIGNED;
            }

            end_of_last_data_checked = i->first + i->second + pad_size;
        }

        // Check for truncation
        off_t file_size = all_data.size();
        if (file_size < end_of_last_data_checked)
        {
            stringstream ss;
            ss << "file looks truncated: its size is " << file_size << " but data is known to exist until " << end_of_last_data_checked << " bytes";
            reporter(ss.str());
            return SEGMENT_UNALIGNED;
        }

        // Check if file_size matches the expected file size
        bool has_hole = false;
        if (file_size > end_of_last_data_checked)
            has_hole = true;

        // Check for holes or elements out of order
        end_of_last_data_checked = 0;
        for (metadata::Collection::const_iterator i = mds.begin(); i != mds.end(); ++i)
        {
            const source::Blob& source = (*i)->sourceBlob();
            if (source.offset < (size_t)end_of_last_data_checked || source.offset > (size_t)end_of_last_data_checked) // last padding plus file header
                has_hole = true;

            end_of_last_data_checked = max(end_of_last_data_checked, (off_t)(source.offset + source.size + pad_size));
        }

        // Take note of files with holes
        if (has_hole)
        {
            reporter("segments contains deleted data or needs reordering");
            return SEGMENT_DIRTY;
        } else
            return SEGMENT_OK;
    }
};

}

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

State Checker::check(std::function<void(const std::string&)> reporter, const metadata::Collection& mds, bool quick)
{
    CheckBackend checker(gzabspath, relpath, reporter, mds);
    checker.accurate = !quick;
    return checker.check();
}

size_t Checker::remove()
{
    size_t size = sys::size(gzabspath);
    sys::unlink(gzabspath);
    return size;
}

Pending Checker::repack(const std::string& rootdir, metadata::Collection& mds, unsigned test_flags)
{
    string tmpabspath = gzabspath + ".repack";

    files::RenameTransaction* rename;
    Pending p(rename = new files::RenameTransaction(tmpabspath, abspath));

    Creator creator(rootdir, relpath, abspath, mds);
    creator.gzabspath = tmpabspath;
    creator.validator = &scan::Validator::by_filename(abspath);
    creator.create();

    // Make sure mds are not holding a reader on the file to repack, because it
    // will soon be invalidated
    for (auto& md: mds) md->sourceBlob().unlock();

    return p;
}

std::shared_ptr<Checker> Checker::create(const std::string& rootdir, const std::string& relpath, const std::string& abspath, metadata::Collection& mds, unsigned test_flags)
{
    Creator creator(rootdir, relpath, abspath, mds);
    creator.create();
    return make_shared<Checker>(rootdir, relpath, abspath);
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


bool Checker::can_store(const std::string& format)
{
    return format == "grib" || format == "grib1" || format == "grib2"
        || format == "bufr";
}

}

namespace gzlines
{

State Checker::check(std::function<void(const std::string&)> reporter, const metadata::Collection& mds, bool quick)
{
    gz::CheckBackend checker(gzabspath, relpath, reporter, mds);
    checker.accurate = !quick;
    checker.pad_size = 1;
    return checker.check();
}

Pending Checker::repack(const std::string& rootdir, metadata::Collection& mds, unsigned test_flags)
{
    string tmpabspath = gzabspath + ".repack";

    files::RenameTransaction* rename;
    Pending p(rename = new files::RenameTransaction(tmpabspath, abspath));

    gz::Creator creator(rootdir, relpath, abspath, mds);
    creator.gzabspath = tmpabspath;
    creator.validator = &scan::Validator::by_filename(abspath);
    creator.padding.push_back('\n');
    creator.create();

    // Make sure mds are not holding a reader on the file to repack, because it
    // will soon be invalidated
    for (auto& md: mds) md->sourceBlob().unlock();

    return p;
}

std::shared_ptr<Checker> Checker::create(const std::string& rootdir, const std::string& relpath, const std::string& abspath, metadata::Collection& mds, unsigned test_flags)
{
    gz::Creator creator(rootdir, relpath, abspath, mds);
    creator.padding.push_back('\n');
    creator.create();
    return make_shared<Checker>(rootdir, relpath, abspath);
}

bool Checker::can_store(const std::string& format)
{
    return format == "vm2";
}

}

}
}


