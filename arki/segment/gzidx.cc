#include "gzidx.h"
#include "common.h"
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
namespace gzidx {

struct Creator : public AppendCreator
{
    std::vector<uint8_t> padding;
    File out;
    File outidx;
    compress::GzipIndexingWriter gzout;

    Creator(const std::string& root, const std::string& relpath, const std::string& abspath, metadata::Collection& mds)
        : AppendCreator(root, relpath, abspath, mds), out(abspath + ".gz"), outidx(abspath + ".gz.idx"), gzout(out, outidx)
    {
    }

    size_t append(const std::vector<uint8_t>& data) override
    {
        gzout.add(data);
        if (!padding.empty())
            gzout.add(padding);
        gzout.close_entry();
        return data.size() + padding.size();
    }

    void create()
    {
        out.open(O_WRONLY | O_CREAT | O_TRUNC, 0666);
        outidx.open(O_WRONLY | O_CREAT | O_TRUNC, 0666);
        AppendCreator::create();
        gzout.flush();
        out.fdatasync();
        outidx.fdatasync();
        out.close();
        outidx.close();
    }
};


Checker::Checker(const std::string& root, const std::string& relpath, const std::string& abspath)
    : segment::Checker(root, relpath, abspath), gzabspath(abspath + ".gz"), gzidxabspath(abspath + ".gz.idx")
{
}

const char* Checker::type() const { return "gzidx"; }
bool Checker::single_file() const { return true; }

bool Checker::exists_on_disk()
{
    return sys::exists(gzabspath) && sys::exists(gzidxabspath);
}

time_t Checker::timestamp()
{
    return max(sys::timestamp(gzabspath), sys::timestamp(gzidxabspath));
}

size_t Checker::size()
{
    return sys::size(gzabspath) + sys::size(gzidxabspath);
}

void Checker::move_data(const std::string& new_root, const std::string& new_relpath, const std::string& new_abspath)
{
    sys::rename(gzabspath, new_abspath + ".gz");
    sys::rename(gzidxabspath, new_abspath + ".gz.idx");
}

State Checker::check(std::function<void(const std::string&)> reporter, const metadata::Collection& mds, bool quick)
{
    std::unique_ptr<struct stat> st = sys::stat(gzabspath);
    if (st.get() == nullptr)
        return SEGMENT_DELETED;

    if (!quick)
    {
        const scan::Validator* validator = &scan::Validator::by_filename(abspath.c_str());

        // Decompress all the segment in memory
        std::vector<uint8_t> all_data = compress::gunzip(gzabspath);

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

    // Get the uncompressed size via the index
    off_t file_size;
    compress::SeekIndex idx;
    if (idx.read(gzidxabspath))
        file_size = idx.ofs_unc.back();
    else
    {
        stringstream ss;
        ss << "missing " << gzidxabspath;
        reporter(ss.str());
        return SEGMENT_CORRUPTED;
    }

    // Check for truncation
    if (file_size < end_of_last_data_checked)
    {
        stringstream ss;
        ss << "file looks truncated: its size is " << file_size << " but data is known to exist until " << end_of_last_data_checked << " bytes";
        reporter(ss.str());
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
    string tmpabspath = gzabspath + ".repack";
    string tmpidxabspath = gzidxabspath + ".repack";

    Pending p(new files::Rename2Transaction(tmpabspath, gzabspath, tmpidxabspath, gzidxabspath));

    Creator creator(rootdir, relpath, abspath, mds);
    creator.out = sys::File(tmpabspath);
    creator.outidx = sys::File(tmpidxabspath);
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
        || format == "bufr" || format == "vm2";
}

}

namespace gzidxlines
{

std::shared_ptr<Checker> Checker::create(const std::string& rootdir, const std::string& relpath, const std::string& abspath, metadata::Collection& mds, unsigned test_flags)
{
    gzidx::Creator creator(rootdir, relpath, abspath, mds);
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


