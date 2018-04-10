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
#include "arki/utils/zip.h"
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
        : AppendCreator(root, relpath, mds), out(dest_abspath, O_WRONLY | O_CREAT | O_TRUNC), zipout("zip", out)
    {
        zipout.with_metadata = false;
        zipout.subdir.clear();
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
    const std::string& format;
    ZipReader reader;
    std::unique_ptr<struct stat> st;
    // File size by offset
    map<size_t, size_t> on_disk;
    size_t max_sequence = 0;

    CheckBackend(const std::string& format, const std::string& abspath, const std::string& relpath, std::function<void(const std::string&)> reporter, const metadata::Collection& mds)
        : AppendCheckBackend(reporter, relpath, mds), format(format), reader(format, core::File(abspath, O_RDONLY))
    {
    }

    void validate(Metadata& md, const types::source::Blob& source) override
    {
        std::vector<uint8_t> buf = reader.get(Span(source.offset, source.size));
        validator->validate_buf(buf.data(), buf.size());
    }

    size_t actual_start(off_t offset, size_t size) const override { return offset - 1; }
    size_t actual_end(off_t offset, size_t size) const override { return offset; }
    size_t offset_end() const override { return max_sequence; }

    State check_source(const types::source::Blob& source) override
    {
        auto si = on_disk.find(source.offset);
        if (si == on_disk.end())
        {
            stringstream ss;
            ss << "expected file " << source.offset << " not found in the zip archive";
            reporter(ss.str());
            return SEGMENT_CORRUPTED;
        }
        if (source.size != si->second)
        {
            stringstream ss;
            ss << "expected file " << source.offset << " has size " << si->second << " instead of expected " << source.size;
            reporter(ss.str());
            return SEGMENT_CORRUPTED;
        }
        on_disk.erase(si);
        return SEGMENT_OK;
    }

    State check()
    {
/*
        st = sys::stat(abspath);
        if (st.get() == nullptr)
        {
            reporter(abspath + " not found on disk");
            return SEGMENT_DELETED;
        }
        if (!S_ISDIR(st->st_mode))
        {
            reporter(abspath + " is not a directory");
            return SEGMENT_CORRUPTED;
        }
*/

        std::vector<segment::Span> spans = reader.list_data();
        for (const auto& span: spans)
        {
            on_disk.insert(make_pair(span.offset, span.size));
            max_sequence = max(max_sequence, span.offset);
        }

        bool dirty = false;
        State state = AppendCheckBackend::check();
        if (!state.is_ok())
        {
            if (state.value == SEGMENT_DIRTY)
                dirty = true;
            else
                return state;
        }

        // Check files on disk that were not accounted for
        // TODO: we need to implement scanning from a memory buffer, or write
        // the decompressed data to a temp file for scanning
        /*
        for (const auto& di: on_disk)
        {
            auto idx = di.first;
            if (accurate)
            {
                string fname = str::joinpath(abspath, ZipReader::data_fname(idx, format));
                metadata::Collection mds;
                try {
                    scan::scan(fname, std::make_shared<core::lock::Null>(), format, [&](unique_ptr<Metadata> md) {
                        mds.acquire(std::move(md));
                        return true;
                    });
                } catch (std::exception& e) {
                    stringstream out;
                    out << "unexpected data file " << idx << " fails to scan (" << e.what() << ")";
                    reporter(out.str());
                    dirty = true;
                    continue;
                }

                if (mds.size() == 0)
                {
                    stringstream ss;
                    ss << "unexpected data file " << idx << " does not contain any " << format << " items";
                    reporter(ss.str());
                    dirty = true;
                    continue;
                }

                if (mds.size() > 1)
                {
                    stringstream ss;
                    ss << "unexpected data file " << idx << " contains " << mds.size() << " " << format << " items instead of 1";
                    reporter(ss.str());
                    return SEGMENT_CORRUPTED;
                }
            }
        }
        */

        if (!on_disk.empty())
        {
            stringstream ss;
            ss << "segment contains " << on_disk.size() << " file(s) that the index does now know about";
            reporter(ss.str());
            dirty = true;
        }

        return dirty ? SEGMENT_DIRTY : SEGMENT_OK;
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
    // TODO: require format as a checker constructor argument, like we have with dir
    string format;
    if (!mds.empty())
        format = mds[0].source().format;
    CheckBackend checker(format, zipabspath, relpath, reporter, mds);
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
