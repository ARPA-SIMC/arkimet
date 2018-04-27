#include "zip.h"
#include "common.h"
#include "arki/exceptions.h"
#include "arki/metadata.h"
#include "arki/metadata/collection.h"
#include "arki/metadata/libarchive.h"
#include "arki/types/source/blob.h"
#include "arki/scan/validator.h"
#include "arki/utils/files.h"
#include "arki/utils/string.h"
#include "arki/utils/sys.h"
#include "arki/utils/zip.h"
#include "arki/scan.h"
#include "arki/nag.h"
#include "arki/utils/accounting.h"
#include "arki/iotrace.h"
#include <fcntl.h>
#include <vector>
#include <algorithm>
#include <sys/uio.h>
#include <system_error>

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

#if 0
    std::unique_ptr<types::Source> create_source(const Metadata& md, const Span& span) override
    {
        return types::Source::createBlobUnlocked(md.source().format, root, relpath + ".zip", span.offset, span.size);
    }
#endif

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

const char* Segment::type() const { return "zip"; }
bool Segment::single_file() const { return true; }
time_t Segment::timestamp() const { return sys::timestamp(abspath + ".zip"); }
std::shared_ptr<segment::Reader> Segment::reader(std::shared_ptr<core::Lock> lock) const
{
    return make_shared<Reader>(format, root, relpath, abspath, lock);
}
std::shared_ptr<segment::Checker> Segment::checker() const
{
    return make_shared<Checker>(format, root, relpath, abspath);
}
bool Segment::can_store(const std::string& format)
{
    return true;
}
std::shared_ptr<segment::Checker> Segment::make_checker(const std::string& format, const std::string& rootdir, const std::string& relpath, const std::string& abspath)
{
    return make_shared<Checker>(format, rootdir, relpath, abspath);
}
std::shared_ptr<segment::Checker> Segment::create(const std::string& format, const std::string& rootdir, const std::string& relpath, const std::string& abspath, metadata::Collection& mds, unsigned test_flags)
{
    Creator creator(rootdir, relpath, mds, abspath + ".zip");
    creator.create();
    return make_shared<Checker>(format, rootdir, relpath, abspath);
}



Reader::Reader(const std::string& format, const std::string& root, const std::string& relpath, const std::string& abspath, std::shared_ptr<core::Lock> lock)
    : segment::BaseReader<Segment>(format, root, relpath, abspath, lock), zip(format, core::File(abspath + ".zip", O_RDONLY | O_CLOEXEC))
{
}

bool Reader::scan_data(metadata_dest_func dest)
{
    // Collect all file names in the directory
    std::vector<Span> spans = zip.list_data();

    // Sort them numerically
    std::sort(spans.begin(), spans.end());

    // Scan them one by one
    auto scanner = scan::Scanner::get_scanner(m_segment.format);
    for (const auto& span : spans)
    {
        std::vector<uint8_t> data = zip.get(span);
        auto md = scanner->scan_data(data);
        if (!dest(move(md)))
            return false;
    }

    return true;
}

std::vector<uint8_t> Reader::read(const types::source::Blob& src)
{
    vector<uint8_t> buf = zip.get(Span(src.offset, src.size));
    acct::plain_data_read_count.incr();
    iotrace::trace_file(zip.zipname, src.offset, src.size, "read data");
    return buf;
}

size_t Reader::stream(const types::source::Blob& src, core::NamedFileDescriptor& out)
{
    vector<uint8_t> buf = read(src);
    if (src.format == "vm2")
    {
        struct iovec todo[2] = {
            { (void*)buf.data(), buf.size() },
            { (void*)"\n", 1 },
        };
        ssize_t res = ::writev(out, todo, 2);
        if (res < 0 || (unsigned)res != buf.size() + 1)
            throw_system_error("cannot write " + to_string(buf.size() + 1) + " bytes to " + out.name());
        return buf.size() + 1;
    } else {
        out.write_all_or_throw(buf);
        return buf.size();
    }
}


Checker::Checker(const std::string& format, const std::string& root, const std::string& relpath, const std::string& abspath)
    : BaseChecker<Segment>(format, root, relpath, abspath), zipabspath(abspath + ".zip")
{
}

bool Checker::exists_on_disk()
{
    return sys::exists(zipabspath);
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
    CheckBackend checker(segment().format, zipabspath, segment().relpath, reporter, mds);
    checker.accurate = !quick;
    return checker.check();
}

void Checker::validate(Metadata& md, const scan::Validator& v)
{
    if (const types::source::Blob* blob = md.has_source_blob())
    {
        if (blob->filename != segment().relpath)
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
    string tmpabspath = segment().abspath + ".repack";

    Pending p(new files::RenameTransaction(tmpabspath, zipabspath));

    Creator creator(rootdir, segment().relpath, mds, segment().abspath + ".zip");
    creator.out = sys::File(tmpabspath);
    creator.validator = &scan::Validator::by_filename(segment().abspath);
    creator.create();

    // Make sure mds are not holding a reader on the file to repack, because it
    // will soon be invalidated
    for (auto& md: mds) md->sourceBlob().unlock();

    return p;
}

void Checker::test_truncate(size_t offset)
{
    if (!sys::exists(segment().abspath))
        sys::write_file(segment().abspath, "");

    if (offset % 512 != 0)
        offset += 512 - (offset % 512);

    utils::files::PreserveFileTimes pft(segment().abspath);
    if (::truncate(segment().abspath.c_str(), offset) < 0)
    {
        stringstream ss;
        ss << "cannot truncate " << segment().abspath << " at " << offset;
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
    sys::File fd(segment().abspath, O_RDWR);
    sys::PreserveFileTimes pt(fd);
    fd.lseek(s.offset);
    fd.write_all_or_throw("\0", 1);
}


}
}
}
#include "base.tcc"
