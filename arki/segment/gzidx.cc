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

namespace {

struct Creator : public AppendCreator
{
    std::vector<uint8_t> padding;
    File out;
    File outidx;
    compress::GzipIndexingWriter gzout;
    size_t written = 0;

    Creator(const std::string& root, const std::string& relpath, const std::string& abspath, metadata::Collection& mds)
        : AppendCreator(root, relpath, abspath, mds), out(abspath + ".gz"), outidx(abspath + ".gz.idx"), gzout(out, outidx)
    {
    }

    size_t append(const std::vector<uint8_t>& data) override
    {
        size_t wrpos = written;
        gzout.add(data);
        if (!padding.empty())
            gzout.add(padding);
        gzout.close_entry();
        written += data.size() + padding.size();
        return wrpos;
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

struct CheckBackend : public AppendCheckBackend
{
    const std::string& gzabspath;
    std::vector<uint8_t> all_data;

    CheckBackend(const std::string& gzabspath, std::string& relpath, std::function<void(const std::string&)> reporter, const metadata::Collection& mds)
        : AppendCheckBackend(reporter, relpath, mds), gzabspath(gzabspath)
    {
    }

    size_t offset_end() const override { return all_data.size(); }

    void validate(Metadata& md, const types::source::Blob& source) const override
    {
        validator->validate_buf(all_data.data() + source.offset, source.size);
    }

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
    CheckBackend checker(gzabspath, relpath, reporter, mds);
    checker.accurate = !quick;
    return checker.check();
}

size_t Checker::remove()
{
    size_t res = size();
    sys::unlink(gzabspath);
    sys::unlink(gzidxabspath);
    return res;
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

namespace gzidxlines {

namespace {

struct CheckBackend : public gzidx::CheckBackend
{
    using gzidx::CheckBackend::CheckBackend;

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
    string tmpidxabspath = gzidxabspath + ".repack";

    Pending p(new files::Rename2Transaction(tmpabspath, gzabspath, tmpidxabspath, gzidxabspath));

    gzidx::Creator creator(rootdir, relpath, abspath, mds);
    creator.padding.push_back('\n');
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


