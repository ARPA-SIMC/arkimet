#include "gz.h"
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
namespace gz {

namespace {

struct Creator : public AppendCreator
{
    std::vector<uint8_t> padding;
    File out;
    compress::GzipWriter gzout;

    Creator(const std::string& root, const std::string& relpath, const std::string& abspath, metadata::Collection& mds)
        : AppendCreator(root, relpath, abspath, mds), out(abspath + ".gz"), gzout(out)
    {
    }

    size_t append(const std::vector<uint8_t>& data) override
    {
        gzout.add(data);
        if (!padding.empty())
            gzout.add(padding);
        return data.size() + padding.size();
    }

    void create()
    {
        out.open(O_WRONLY | O_CREAT | O_TRUNC, 0666);
        AppendCreator::create();
        gzout.flush();
        out.fdatasync();
        out.close();
    }
};

struct CheckBackend : public AppendCheckBackend
{
    const std::string& gzabspath;
    const std::string& relpath;

    CheckBackend(const std::string& gzabspath, const std::string& relpath, std::function<void(const std::string&)> reporter, const metadata::Collection& mds)
        : AppendCheckBackend(reporter, mds), gzabspath(gzabspath), relpath(relpath)
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

        State state = check_contiguous();
        if (!state.is_ok())
            return state;

        // Check the match between end of data and end of file
        size_t file_size = all_data.size();
        if (file_size < end_of_known_data)
        {
            stringstream ss;
            ss << "file looks truncated: its size is " << file_size << " but data is known to exist until " << end_of_known_data << " bytes";
            reporter(ss.str());
            return SEGMENT_UNALIGNED;
        }
        if (file_size > end_of_known_data)
        {
            reporter("segment contains possibly deleted data at the end");
            return SEGMENT_DIRTY;
        }

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

    Pending p(new files::RenameTransaction(tmpabspath, abspath));

    Creator creator(rootdir, relpath, abspath, mds);
    creator.out = sys::File(tmpabspath);
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

struct CheckBackend : public gz::CheckBackend
{
    using gz::CheckBackend::CheckBackend;

    size_t compute_padding(off_t offset, size_t size) const override
    {
        return 1;
    }
};


State Checker::check(std::function<void(const std::string&)> reporter, const metadata::Collection& mds, bool quick)
{
    CheckBackend checker(gzabspath, relpath, reporter, mds);
    checker.accurate = !quick;
    return checker.check();
}

Pending Checker::repack(const std::string& rootdir, metadata::Collection& mds, unsigned test_flags)
{
    string tmpabspath = gzabspath + ".repack";

    files::RenameTransaction* rename;
    Pending p(rename = new files::RenameTransaction(tmpabspath, abspath));

    gz::Creator creator(rootdir, relpath, abspath, mds);
    creator.out = sys::File(tmpabspath);
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


