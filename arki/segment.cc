#include "segment.h"
#include "segment/missing.h"
#include "segment/fd.h"
#include "segment/dir.h"
#include "segment/tar.h"
#include "segment/zip.h"
#include "segment/gz.h"
#include "arki/exceptions.h"
#include "arki/stream.h"
#include "arki/metadata/collection.h"
#include "arki/metadata.h"
#include "arki/types/source/blob.h"
#include "arki/utils/string.h"
#include "arki/utils/sys.h"
#include "arki/scan.h"

using namespace std;
using namespace std::string_literals;
using namespace arki::utils;

namespace arki {

Segment::Segment(const std::string& format, const std::filesystem::path& root, const std::filesystem::path& relpath, const std::filesystem::path& abspath)
    : format(format), root(root), relpath(relpath), abspath(abspath)
{
}

Segment::~Segment()
{
}

std::filesystem::path Segment::basename(const std::filesystem::path& pathname)
{
    const auto& native = pathname.native();
    if (str::endswith(native, ".gz"))
        return native.substr(0, native.size() - 3);
    if (str::endswith(native, ".tar"))
        return native.substr(0, native.size() - 4);
    if (str::endswith(native, ".zip"))
        return native.substr(0, native.size() - 4);
    return pathname;
}

bool Segment::is_segment(const std::filesystem::path& abspath)
{
    std::unique_ptr<struct stat> st = sys::stat(abspath);
    if (!st.get())
        return false;

    auto extension = abspath.extension();

    if (extension == ".metadata")
        return false;

    if (extension == ".summary")
        return false;

    if (extension == ".gz.idx")
        return false;

    if (extension == ".zip")
    {
        if (S_ISDIR(st->st_mode))
            return false;
        string format = scan::Scanner::format_from_filename(abspath.stem(), "");
        return segment::zip::Segment::can_store(format);
    }

    if (extension == ".gz")
    {
        if (S_ISDIR(st->st_mode))
            return false;
        string format = scan::Scanner::format_from_filename(abspath.stem(), "");
        return segment::gz::Segment::can_store(format);
    }

    if (extension == ".tar")
    {
        if (S_ISDIR(st->st_mode))
            return false;
        string format = scan::Scanner::format_from_filename(abspath.stem(), "");
        return segment::tar::Segment::can_store(format);
    }

    string format = scan::Scanner::format_from_filename(abspath, "");
    if (format.empty())
        return false;

    if (!S_ISDIR(st->st_mode))
        return segment::fd::Segment::can_store(format);

    if (!std::filesystem::exists(abspath / ".sequence"))
        return false;

    return segment::dir::Segment::can_store(format);
}

std::shared_ptr<segment::Reader> Segment::detect_reader(const std::string& format, const std::filesystem::path& root, const std::filesystem::path& relpath, const std::filesystem::path& abspath, std::shared_ptr<core::Lock> lock)
{
    std::shared_ptr<segment::Reader> res;

    std::unique_ptr<struct stat> st = sys::stat(abspath);
    if (st.get())
    {
        if (S_ISDIR(st->st_mode))
        {
            res.reset(new segment::dir::Reader(format, root, relpath, abspath, lock));
        } else {
            if (format == "grib" || format == "bufr")
            {
                res.reset(new segment::concat::Reader(format, root, relpath, abspath, lock));
            } else if (format == "vm2") {
                res.reset(new segment::lines::Reader(format, root, relpath, abspath, lock));
            } else if (format == "odimh5" || format == "nc" || format == "jpeg") {
                res.reset(new segment::concat::Reader(format, root, relpath, abspath, lock));
            } else {
                throw_consistency_error(
                        "getting segment for "s + format + " file " + relpath.native(),
                        "format not supported");
            }
        }
        return res;
    }

    st = sys::stat(sys::with_suffix(abspath, ".gz"));
    if (st.get())
    {
        if (S_ISDIR(st->st_mode))
            throw std::runtime_error(
                    "cannot get a reader for " + format + " directory " + relpath.native() + ": cannot handle a directory with a .gz extension");
        else
        {
            if (format == "grib" || format == "bufr")
            {
                res.reset(new segment::gzconcat::Reader(format, root, relpath, abspath, lock));
            } else if (format == "vm2") {
                res.reset(new segment::gzlines::Reader(format, root, relpath, abspath, lock));
            } else if (format == "odimh5" || format == "nc" || format == "jpeg") {
                res.reset(new segment::gzconcat::Reader(format, root, relpath, abspath, lock));
            } else {
                throw_consistency_error(
                        "getting segment for "s + format + " file " + relpath.native(),
                        "format not supported");
            }
        }
        return res;
    }

    st = sys::stat(sys::with_suffix(abspath, ".tar"));
    if (st.get())
    {
        res.reset(new segment::tar::Reader(format, root, relpath, abspath, lock));
        return res;
    }

    st = sys::stat(sys::with_suffix(abspath, ".zip"));
    if (st.get())
    {
        res.reset(new segment::zip::Reader(format, root, relpath, abspath, lock));
        return res;
    }

    res.reset(new segment::missing::Reader(format, root, relpath, abspath, lock));
    return res;
}

std::shared_ptr<segment::Writer> Segment::detect_writer(const segment::WriterConfig& config, const std::string& format, const std::filesystem::path& root, const std::filesystem::path& relpath, const std::filesystem::path& abspath, bool mock_data)
{
    std::shared_ptr<segment::Writer> res;

    std::unique_ptr<struct stat> st = sys::stat(abspath);
    if (st.get())
    {
        if (S_ISDIR(st->st_mode))
        {
            if (segment::dir::Segment::can_store(format))
            {
                if (mock_data)
                    res.reset(new segment::dir::HoleWriter(config, format, root, relpath, abspath));
                else
                    res.reset(new segment::dir::Writer(config, format, root, relpath, abspath));
            } else {
                throw_consistency_error(
                        "getting writer for "s + format + " file " + relpath.native(),
                        "format not supported");
            }
        } else {
            if (format == "grib" || format == "bufr")
            {
                if (mock_data)
                    res.reset(new segment::concat::HoleWriter(config, format, root, relpath, abspath));
                else
                    res.reset(new segment::concat::Writer(config, format, root, relpath, abspath));
            } else if (format == "vm2") {
                if (mock_data)
                    throw_consistency_error("mock_data single-file line-based segments are not implemented");
                else
                    res.reset(new segment::lines::Writer(config, format, root, relpath, abspath));
            } else if (format == "odimh5") {
                throw_consistency_error("segment is a file, but odimh5 data can only be stored into directory segments");
            } else if (format == "nc") {
                throw_consistency_error("segment is a file, but netcdf data can only be stored into directory segments");
            } else if (format == "jpeg") {
                throw_consistency_error("segment is a file, but jpeg data can only be stored into directory segments");
            } else {
                throw_consistency_error(
                        "getting segment for " + format + " file " + relpath.native(),
                        "format not supported");
            }
        }
        return res;
    }

    st = sys::stat(sys::with_suffix(abspath, ".gz"));
    if (st.get())
    {
        if (S_ISDIR(st->st_mode))
            throw_consistency_error(
                    "getting writer for "s + format + " file " + relpath.native(),
                    "cannot handle a directory with a .gz extension");
        else
            throw_consistency_error(
                    "getting writer for "s + format + " file " + relpath.native(),
                    "cannot write to .gz segments");
    }

    st = sys::stat(sys::with_suffix(abspath, ".tar"));
    if (st.get())
        throw_consistency_error(
                "getting writer for "s + format + " file " + relpath.native(),
                "cannot write to .tar segments");

    st = sys::stat(sys::with_suffix(abspath, ".zip"));
    if (st.get())
        throw_consistency_error(
                "getting writer for "s + format + " file " + relpath.native(),
                "cannot write to .zip segments");

    return res;
}

std::shared_ptr<segment::Checker> Segment::detect_checker(const std::string& format, const std::filesystem::path& root, const std::filesystem::path& relpath, const std::filesystem::path& abspath, bool mock_data)
{
    std::shared_ptr<segment::Checker> res;

    std::unique_ptr<struct stat> st = sys::stat(abspath);
    if (st.get())
    {
        if (S_ISDIR(st->st_mode))
        {
            if (segment::dir::Segment::can_store(format))
            {
                if (mock_data)
                    res.reset(new segment::dir::HoleChecker(format, root, relpath, abspath));
                else
                    res.reset(new segment::dir::Checker(format, root, relpath, abspath));
            } else {
                throw_consistency_error(
                        "getting segment for " + format + " file " + relpath.native(),
                        "format not supported");
            }
        } else {
            if (format == "grib" || format == "bufr")
            {
                if (mock_data)
                    res.reset(new segment::concat::HoleChecker(format, root, relpath, abspath));
                else
                    res.reset(new segment::concat::Checker(format, root, relpath, abspath));
            } else if (format == "vm2") {
                if (mock_data)
                    throw_consistency_error("mockdata single-file line-based segments not implemented");
                else
                    res.reset(new segment::lines::Checker(format, root, relpath, abspath));
            } else if (format == "odimh5" || format == "nc" || format == "jpeg") {
                // If it's a file and we need a directory, still get a checker
                // so it can deal with it
                if (mock_data)
                    res.reset(new segment::dir::HoleChecker(format, root, relpath, abspath));
                else
                    res.reset(new segment::dir::Checker(format, root, relpath, abspath));
            } else {
                throw_consistency_error(
                        "getting segment for "s + format + " file " + relpath.native(),
                        "format not supported");
            }
        }
        return res;
    }

    st = sys::stat(sys::with_suffix(abspath, ".gz"));
    if (st.get())
    {
        if (S_ISDIR(st->st_mode))
            throw_consistency_error(
                    "getting checker for "s + format + " file " + relpath.native(),
                    "cannot handle a directory with a .gz extension");

        if (format == "grib" || format == "bufr")
        {
            res.reset(new segment::gzconcat::Checker(format, root, relpath, abspath));
        } else if (format == "vm2") {
            res.reset(new segment::gzlines::Checker(format, root, relpath, abspath));
        } else if (format == "odimh5") {
            throw_consistency_error(
                    "getting checker for "s + format + " file " + relpath.native(),
                    "cannot handle a gzipped odim file as a segment");
        } else if (format == "nc") {
            throw_consistency_error(
                    "getting checker for "s + format + " file " + relpath.native(),
                    "cannot handle a gzipped netcdf file as a segment");
        } else if (format == "jpeg") {
            throw_consistency_error(
                    "getting checker for "s + format + " file " + relpath.native(),
                    "cannot handle a gzipped jpeg file as a segment");
        } else {
            throw_consistency_error(
                    "getting segment for "s + format + " file " + relpath.native(),
                    "format not supported");
        }
    }

    st = sys::stat(sys::with_suffix(abspath, ".tar"));
    if (st.get())
    {
        if (S_ISDIR(st->st_mode))
            throw_consistency_error(
                    "getting checker for "s + format + " file " + relpath.native(),
                    "cannot handle a directory with a .tar extension");
        res.reset(new segment::tar::Checker(format, root, relpath, abspath));
        return res;
    }

    st = sys::stat(sys::with_suffix(abspath, ".zip"));
    if (st.get())
    {
        if (S_ISDIR(st->st_mode))
            throw_consistency_error(
                    "getting checker for "s + format + " file " + relpath.native(),
                    "cannot handle a directory with a .zip extension");
        res.reset(new segment::zip::Checker(format, root, relpath, abspath));
        return res;
    }

    return res;
}


namespace segment {

Reader::Reader(std::shared_ptr<core::Lock> lock)
    : lock(lock)
{
}

Reader::~Reader()
{
}

bool Reader::scan(metadata_dest_func dest)
{
    // stat the metadata file, if it exists
    auto md_abspath = sys::with_suffix(segment().abspath, ".metadata");
    unique_ptr<struct stat> st_md = sys::stat(md_abspath);
    // If it exists and it looks new enough, use it
    if (st_md.get() && st_md->st_mtime >= segment().timestamp())
    {
        std::filesystem::path root(segment().abspath.parent_path());
        return Metadata::read_file(metadata::ReadContext(md_abspath, root), [&](std::shared_ptr<Metadata> md) {
            md->sourceBlob().lock(shared_from_this());
            return dest(md);
        });
    }

    // Else scan the file as usual
    return scan_data(dest);
}

stream::SendResult Reader::stream(const types::source::Blob& src, StreamOutput& out)
{
    vector<uint8_t> buf = read(src);
    if (src.format == "vm2")
        return out.send_line(buf.data(), buf.size());
    else
        return out.send_buffer(buf.data(), buf.size());
}


Writer::PendingMetadata::PendingMetadata(const WriterConfig& config, Metadata& md, std::unique_ptr<types::source::Blob> new_source)
    : config(config), md(md), new_source(new_source.release())
{
}

Writer::PendingMetadata::PendingMetadata(PendingMetadata&& o)
    : config(o.config), md(o.md), new_source(o.new_source)
{
    o.new_source = nullptr;
}

Writer::PendingMetadata::~PendingMetadata()
{
    delete new_source;
}

void Writer::PendingMetadata::set_source()
{
    std::unique_ptr<types::source::Blob> source(new_source);
    new_source = 0;
    md.set_source(move(source));
    if (config.drop_cached_data_on_commit)
        md.drop_cached_data();
}


RepackConfig::RepackConfig() {}
RepackConfig::RepackConfig(unsigned gz_group_size, unsigned test_flags)
    : gz_group_size(gz_group_size), test_flags(test_flags)
{
}


std::shared_ptr<segment::Checker> Checker::tar(metadata::Collection& mds)
{
    segment::tar::Segment::create(segment().format, segment().root, segment().relpath, segment().abspath, mds);
    remove();
    return make_shared<segment::tar::Checker>(segment().format, segment().root, segment().relpath, segment().abspath);
}

std::shared_ptr<segment::Checker> Checker::zip(metadata::Collection& mds)
{
    segment::zip::Segment::create(segment().format, segment().root, segment().relpath, segment().abspath, mds);
    remove();
    return make_shared<segment::zip::Checker>(segment().format, segment().root, segment().relpath, segment().abspath);
}

std::shared_ptr<segment::Checker> Checker::compress(metadata::Collection& mds, unsigned groupsize)
{
    std::shared_ptr<segment::Checker> res;
    if (segment().format == "vm2")
        res = segment::gzlines::Segment::create(segment().format, segment().root, segment().relpath, segment().abspath, mds, RepackConfig(groupsize));
    else
        res = segment::gzconcat::Segment::create(segment().format, segment().root, segment().relpath, segment().abspath, mds, RepackConfig(groupsize));
    remove();
    return res;
}

void Checker::test_truncate_by_data(const metadata::Collection& mds, unsigned data_idx)
{
    const auto& s = mds[data_idx].sourceBlob();
    test_truncate(s.offset);
}

std::string State::to_string() const
{
    vector<const char*> res;
    if (value == SEGMENT_OK)         res.push_back("OK");
    if (value & SEGMENT_DIRTY)       res.push_back("DIRTY");
    if (value & SEGMENT_UNALIGNED)   res.push_back("UNALIGNED");
    if (value & SEGMENT_MISSING)     res.push_back("MISSING");
    if (value & SEGMENT_DELETED)     res.push_back("DELETED");
    if (value & SEGMENT_CORRUPTED)   res.push_back("CORRUPTED");
    if (value & SEGMENT_ARCHIVE_AGE) res.push_back("ARCHIVE_AGE");
    if (value & SEGMENT_DELETE_AGE)  res.push_back("DELETE_AGE");
    return str::join(",", res.begin(), res.end());
}

std::ostream& operator<<(std::ostream& o, const State& s)
{
    return o << s.to_string();
}

}
}
