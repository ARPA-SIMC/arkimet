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
    size_t written = 0;

    Creator(const std::string& root, const std::string& relpath, metadata::Collection& mds, const std::string& dest_abspath)
        : AppendCreator(root, relpath, mds), out(dest_abspath), gzout(out)
    {
    }

    size_t append(const std::vector<uint8_t>& data) override
    {
        size_t wrpos = written;
        gzout.add(data);
        if (!padding.empty())
            gzout.add(padding);
        written += data.size() + padding.size();
        return wrpos;
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
    std::vector<uint8_t> all_data;

    CheckBackend(const std::string& gzabspath, const std::string& relpath, std::function<void(const std::string&)> reporter, const metadata::Collection& mds)
        : AppendCheckBackend(reporter, relpath, mds), gzabspath(gzabspath)
    {
    }

    void validate(Metadata& md, const types::source::Blob& source) override
    {
        validator->validate_buf(all_data.data() + source.offset, source.size);
    }

    size_t offset_end() const { return all_data.size(); }

    State check()
    {
        std::unique_ptr<struct stat> st = sys::stat(gzabspath);
        if (st.get() == nullptr)
            return SEGMENT_DELETED;

        // Decompress all the segment in memory
        all_data = compress::gunzip(gzabspath);

        return AppendCheckBackend::check();
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

    Creator creator(rootdir, relpath, mds, tmpabspath);
    creator.validator = &scan::Validator::by_filename(abspath);
    creator.create();

    // Make sure mds are not holding a reader on the file to repack, because it
    // will soon be invalidated
    for (auto& md: mds) md->sourceBlob().unlock();

    return p;
}

std::shared_ptr<Checker> Checker::create(const std::string& rootdir, const std::string& relpath, const std::string& abspath, metadata::Collection& mds, unsigned test_flags)
{
    Creator creator(rootdir, relpath, mds, abspath + ".gz");
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

namespace gzlines {

namespace {

struct CheckBackend : public gz::CheckBackend
{
    using gz::CheckBackend::CheckBackend;

    size_t actual_end(off_t offset, size_t size) const override
    {
        return offset + size + 1;
    }
};

}


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

    gz::Creator creator(rootdir, relpath, mds, tmpabspath);
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
    gz::Creator creator(rootdir, relpath, mds, abspath + ".gz");
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


