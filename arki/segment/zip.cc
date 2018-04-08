#include "zip.h"
#include "common.h"
#include "arki/exceptions.h"
#include "arki/metadata.h"
#include "arki/metadata/collection.h"
#include "arki/metadata/libarchive.h"
#include "arki/types/source/blob.h"
#include "arki/scan/any.h"
#include "arki/utils/compress.h"
#include "arki/utils/files.h"
#include "arki/utils/string.h"
#include "arki/utils/sys.h"
//#include "arki/utils/zip.h"
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
namespace zip {

namespace {

struct Creator : public AppendCreator
{
    std::string format;
    File out;
    metadata::LibarchiveOutput zipout;
    size_t idx = 0;
    char fname[100];

    Creator(const std::string& root, const std::string& relpath, metadata::Collection& mds, const std::string& dest_abspath)
        : AppendCreator(root, relpath, mds), out(dest_abspath), zipout("zip", out)
    {
        zipout.with_metadata = false;
        if (!mds.empty())
            format = mds[0].source().format;
    }

    Span append_md(Metadata& md)
    {
        Span res;
        res.offset = zipout.append(md);
        res.size = md.getData().size();
        return res;
    }

    void create()
    {
        out.open(O_WRONLY | O_CREAT | O_TRUNC, 0666);
        AppendCreator::create();
        zipout.flush();
        out.fdatasync();
        out.close();
    }
};

struct CheckBackend : public AppendCheckBackend
{
    core::File data;
    struct stat st;

    CheckBackend(const std::string& zipabspath, const std::string& relpath, std::function<void(const std::string&)> reporter, const metadata::Collection& mds)
        : AppendCheckBackend(reporter, relpath, mds), data(zipabspath)
    {
    }

    void validate(Metadata& md, const types::source::Blob& source) override
    {
        validator->validate_file(data, source.offset, source.size);
    }

    size_t offset_end() const override { return st.st_size - 1024; }

    size_t actual_start(off_t offset, size_t size) const override
    {
        return offset - 512;
    }

    size_t actual_end(off_t offset, size_t size) const override
    {
        return offset + size + 512 - (size % 512);
    }

    State check()
    {
        if (!data.open_ifexists(O_RDONLY))
        {
            reporter(data.name() + " not found on disk");
            return SEGMENT_DELETED;
        }
        data.fstat(st);
        return AppendCheckBackend::check();
    }
};

}


Checker::Checker(const std::string& root, const std::string& relpath, const std::string& abspath)
    : segment::Checker(root, relpath, abspath), zipabspath(abspath + ".zip")
{
}

const char* Checker::type() const { return "zip"; }
bool Checker::single_file() const { return true; }

bool Checker::exists_on_disk()
{
    return sys::exists(zipabspath);
}

time_t Checker::timestamp()
{
    return sys::timestamp(zipabspath);
}

size_t Checker::size()
{
    return sys::size(zipabspath);
}

void Checker::move_data(const std::string& new_root, const std::string& new_relpath, const std::string& new_abspath)
{
    string new_zipabspath = new_abspath + ".zip";
    if (rename(zipabspath.c_str(), new_zipabspath.c_str()) < 0)
    {
        stringstream ss;
        ss << "cannot rename " << zipabspath << " to " << new_zipabspath;
        throw std::system_error(errno, std::system_category(), ss.str());
    }
}

State Checker::check(std::function<void(const std::string&)> reporter, const metadata::Collection& mds, bool quick)
{
    CheckBackend checker(zipabspath, relpath, reporter, mds);
    checker.accurate = !quick;
    return checker.check();
}

void Checker::validate(Metadata& md, const scan::Validator& v)
{
    if (const types::source::Blob* blob = md.has_source_blob())
    {
        if (blob->filename != relpath)
            throw std::runtime_error("metadata to validate does not appear to be from this segment");

        sys::File fd(zipabspath, O_RDONLY);
        v.validate_file(fd, blob->offset, blob->size);
        return;
    }
    const auto& buf = md.getData();
    v.validate_buf(buf.data(), buf.size());
}

size_t Checker::remove()
{
    size_t size = sys::size(zipabspath);
    sys::unlink(zipabspath);
    return size;
}

Pending Checker::repack(const std::string& rootdir, metadata::Collection& mds, unsigned test_flags)
{
    string tmpabspath = abspath + ".repack";

    Pending p(new files::RenameTransaction(tmpabspath, zipabspath));

    Creator creator(rootdir, relpath, mds, abspath + ".zip");
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
    Creator creator(rootdir, relpath, mds, abspath + ".zip");
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
    return true;
}

}
}
}
