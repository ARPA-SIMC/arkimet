#include "segment.h"
#include "segment/concat.h"
#include "segment/lines.h"
#include "segment/dir.h"
#include "segment/tar.h"
#include "segment/zip.h"
#include "segment/gz.h"
#include "segment/gzidx.h"
#include "arki/configfile.h"
#include "arki/exceptions.h"
#include "arki/scan/any.h"
#include "arki/metadata/collection.h"
#include "arki/metadata.h"
#include "arki/types/source/blob.h"
#include "arki/utils.h"
#include "arki/utils/files.h"
#include "arki/utils/string.h"
#include "arki/utils/sys.h"

using namespace std;
using namespace arki::utils;

namespace arki {

Segment::Segment(const std::string& root, const std::string& relpath, const std::string& abspath)
    : root(root), relpath(relpath), abspath(abspath)
{
}

Segment::~Segment()
{
}


namespace segment {

Reader::Reader(const std::string& root, const std::string& relpath, const std::string& abspath, std::shared_ptr<core::Lock> lock)
    : Segment(root, relpath, abspath), lock(lock)
{
}

std::shared_ptr<Reader> Reader::for_pathname(const std::string& format, const std::string& root, const std::string& relpath, const std::string& abspath, std::shared_ptr<core::Lock> lock)
{
    std::shared_ptr<segment::Reader> res;

    std::unique_ptr<struct stat> st = sys::stat(abspath);
    if (st.get())
    {
        if (S_ISDIR(st->st_mode))
        {
            res.reset(new segment::dir::Reader(format, root, relpath, abspath, lock));
        } else {
            res.reset(new segment::fd::Reader(root, relpath, abspath, lock));
        }
        return res;
    }

    st = sys::stat(abspath + ".gz");
    if (st.get())
    {
        if (S_ISDIR(st->st_mode))
            throw std::runtime_error(
                    "cannot get a reader for " + format + " directory " + relpath + ": cannot handle a directory with a .gz extension");
        else
        {
            if (sys::exists(abspath + ".gz.idx"))
                res.reset(new segment::gzidx::Reader(root, relpath, abspath, lock));
            else
                res.reset(new segment::gz::Reader(root, relpath, abspath, lock));
        }
    }

    st = sys::stat(abspath + ".tar");
    if (st.get())
        res.reset(new segment::tar::Reader(root, relpath, abspath, lock));

    st = sys::stat(abspath + ".zip");
    if (st.get())
        throw std::runtime_error("getting reader for " + format + " .zip file " + relpath + " is not yet implemented");

    throw std::runtime_error("cannot get reader for nonexisting segment " + relpath + " (tried also .gz, .tar, .zip)");
}


Writer::PendingMetadata::PendingMetadata(Metadata& md, std::unique_ptr<types::source::Blob> new_source)
    : md(md), new_source(new_source.release())
{
}

Writer::PendingMetadata::PendingMetadata(PendingMetadata&& o)
    : md(o.md), new_source(o.new_source)
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
}

std::shared_ptr<Writer> Writer::for_pathname(const std::string& format, const std::string& root, const std::string& relpath, const std::string& abspath, bool mock_data)
{
    std::shared_ptr<segment::Writer> res;

    std::unique_ptr<struct stat> st = sys::stat(abspath);
    if (st.get())
    {
        if (S_ISDIR(st->st_mode))
        {
            if (segment::dir::Checker::can_store(format))
            {
                if (mock_data)
                    res.reset(new segment::dir::HoleWriter(format, root, relpath, abspath));
                else
                    res.reset(new segment::dir::Writer(format, root, relpath, abspath));
            } else {
                throw_consistency_error(
                        "getting writer for " + format + " file " + relpath,
                        "format not supported");
            }
        } else {
            if (format == "grib" || format == "grib1" || format == "grib2")
            {
                if (mock_data)
                    res.reset(new segment::concat::HoleWriter(root, relpath, abspath));
                else
                    res.reset(new segment::concat::Writer(root, relpath, abspath));
            } else if (format == "bufr") {
                if (mock_data)
                    res.reset(new segment::concat::HoleWriter(root, relpath, abspath));
                else
                    res.reset(new segment::concat::Writer(root, relpath, abspath));
            } else if (format == "vm2") {
                if (mock_data)
                    throw_consistency_error("mock_data single-file line-based segments are not implemented");
                else
                    res.reset(new segment::lines::Writer(root, relpath, abspath));
            } else if (format == "odimh5" || format == "h5" || format == "odim") {
                throw_consistency_error("segment is a file, but odimh5 data can only be stored into directory segments");
            } else {
                throw_consistency_error(
                        "getting segment for " + format + " file " + relpath,
                        "format not supported");
            }
        }
        return res;
    }

    st = sys::stat(abspath + ".gz");
    if (st.get())
    {
        if (S_ISDIR(st->st_mode))
            throw_consistency_error(
                    "getting writer for " + format + " file " + relpath,
                    "cannot handle a directory with a .gz extension");
        else
            throw_consistency_error(
                    "getting writer for " + format + " file " + relpath,
                    "cannot write to .gz segments");
    }

    st = sys::stat(abspath + ".tar");
    if (st.get())
        throw_consistency_error(
                "getting writer for " + format + " file " + relpath,
                "cannot write to .tar segments");

    st = sys::stat(abspath + ".zip");
    if (st.get())
        throw_consistency_error(
                "getting writer for " + format + " file " + relpath,
                "cannot write to .zip segments");

    return res;
}


std::shared_ptr<segment::Checker> Checker::tar(metadata::Collection& mds)
{
    segment::tar::Checker::create(root, relpath, abspath, mds);
    remove();
    return make_shared<segment::tar::Checker>(root, relpath, abspath);
}

std::shared_ptr<segment::Checker> Checker::zip(metadata::Collection& mds)
{
    segment::zip::Checker::create(root, relpath, abspath, mds);
    remove();
    return make_shared<segment::zip::Checker>(root, relpath, abspath);
}

std::shared_ptr<segment::Checker> Checker::compress(metadata::Collection& mds)
{
    segment::gzidx::Checker::create(root, relpath, abspath, mds);
    remove();
    return make_shared<segment::gzidx::Checker>(root, relpath, abspath);
}

void Checker::move(const std::string& new_root, const std::string& new_relpath, const std::string& new_abspath)
{
    sys::makedirs(str::dirname(new_abspath));

    // Sanity checks: avoid conflicts
    if (sys::exists(new_abspath) || sys::exists(new_abspath + ".tar") || sys::exists(new_abspath + ".gz") || sys::exists(new_abspath + ".zip"))
    {
        stringstream ss;
        ss << "cannot archive " << abspath << " to " << new_abspath << " because the destination already exists";
        throw runtime_error(ss.str());
    }

    // Remove stale metadata and summaries that may have been left around
    sys::unlink_ifexists(new_abspath + ".metadata");
    sys::unlink_ifexists(new_abspath + ".summary");

    move_data(new_root, new_relpath, new_abspath);

    // Move metadata to archive
    sys::rename_ifexists(abspath + ".metadata", new_abspath + ".metadata");
    sys::rename_ifexists(abspath + ".summary", new_abspath + ".summary");

    root = new_root;
    relpath = new_relpath;
    abspath = new_abspath;
}

void Checker::test_truncate(const metadata::Collection& mds, unsigned data_idx)
{
    const auto& s = mds[data_idx].sourceBlob();
    test_truncate(s.offset);
}

std::shared_ptr<Checker> Checker::for_pathname(const std::string& format, const std::string& root, const std::string& relpath, const std::string& abspath, bool mock_data)
{
    std::shared_ptr<segment::Checker> res;

    std::unique_ptr<struct stat> st = sys::stat(abspath);
    if (st.get())
    {
        if (S_ISDIR(st->st_mode))
        {
            if (segment::dir::Checker::can_store(format))
            {
                if (mock_data)
                    res.reset(new segment::dir::HoleChecker(format, root, relpath, abspath));
                else
                    res.reset(new segment::dir::Checker(format, root, relpath, abspath));
            } else {
                throw_consistency_error(
                        "getting segment for " + format + " file " + relpath,
                        "format not supported");
            }
        } else {
            if (format == "grib" || format == "grib1" || format == "grib2")
            {
                if (mock_data)
                    res.reset(new segment::concat::HoleChecker(root, relpath, abspath));
                else
                    res.reset(new segment::concat::Checker(root, relpath, abspath));
            } else if (format == "bufr") {
                if (mock_data)
                    res.reset(new segment::concat::HoleChecker(root, relpath, abspath));
                else
                    res.reset(new segment::concat::Checker(root, relpath, abspath));
            } else if (format == "vm2") {
                if (mock_data)
                    throw_consistency_error("mockdata single-file line-based segments not implemented");
                else
                    res.reset(new segment::lines::Checker(root, relpath, abspath));
            } else if (format == "odimh5" || format == "h5" || format == "odim") {
                // If it's a file and we need a directory, still get a checker
                // so it can deal with it
                if (mock_data)
                    res.reset(new segment::dir::HoleChecker(format, root, relpath, abspath));
                else
                    res.reset(new segment::dir::Checker(format, root, relpath, abspath));
            } else {
                throw_consistency_error(
                        "getting segment for " + format + " file " + relpath,
                        "format not supported");
            }
        }
        return res;
    }

    st = sys::stat(abspath + ".gz");
    if (st.get())
    {
        if (S_ISDIR(st->st_mode))
            throw_consistency_error(
                    "getting checker for " + format + " file " + relpath,
                    "cannot handle a directory with a .gz extension");

        if (format == "grib" || format == "grib1" || format == "grib2" || format == "bufr")
        {
            if (sys::exists(abspath + ".gz.idx"))
                res.reset(new segment::gzidx::Checker(root, relpath, abspath));
            else
                res.reset(new segment::gz::Checker(root, relpath, abspath));
        } else if (format == "vm2") {
            if (sys::exists(abspath + ".gz.idx"))
                res.reset(new segment::gzidxlines::Checker(root, relpath, abspath));
            else
                res.reset(new segment::gzlines::Checker(root, relpath, abspath));
        } else if (format == "odimh5" || format == "h5" || format == "odim") {
            throw_consistency_error(
                    "getting checker for " + format + " file " + relpath,
                    "cannot handle a gzipped odim file as a segment");
        } else {
            throw_consistency_error(
                    "getting segment for " + format + " file " + relpath,
                    "format not supported");
        }
    }

    st = sys::stat(abspath + ".tar");
    if (st.get())
    {
        if (S_ISDIR(st->st_mode))
            throw_consistency_error(
                    "getting checker for " + format + " file " + relpath,
                    "cannot handle a directory with a .tar extension");
        res.reset(new segment::tar::Checker(root, relpath, abspath));
        return res;
    }

    st = sys::stat(abspath + ".zip");
    if (st.get())
    {
        if (S_ISDIR(st->st_mode))
            throw_consistency_error(
                    "getting checker for " + format + " file " + relpath,
                    "cannot handle a directory with a .zip extension");
        res.reset(new segment::zip::Checker(root, relpath, abspath));
        return res;
    }

    return res;
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
