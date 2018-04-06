#include "segment.h"
#include "segment/concat.h"
#include "segment/lines.h"
#include "segment/dir.h"
#include "segment/tar.h"
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

Segment::Segment(const std::string& root, const std::string& relname, const std::string& abspath)
    : root(root), relname(relname), absname(abspath)
{
}

Segment::~Segment()
{
}


namespace segment {

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
    std::unique_ptr<struct stat> st = sys::stat(abspath);
    std::shared_ptr<segment::Writer> res;
    if (!st.get())
        st = sys::stat(abspath + ".gz");
    if (!st.get())
    {
        st = sys::stat(abspath + ".tar");
        if (st.get())
            throw_consistency_error(
                    "getting writer for " + format + " file " + relpath,
                    "cannot write to .tar segments");
    }
    if (!st.get())
        return res;

    if (S_ISDIR(st->st_mode))
    {
        if (segment::dir::can_store(format))
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


std::shared_ptr<segment::Checker> Checker::tar(metadata::Collection& mds)
{
    segment::tar::Checker::create(root, relname + ".tar", absname + ".tar", mds);
    remove();
    return make_shared<segment::tar::Checker>(root, relname, absname);
}

void Checker::move(const std::string& new_root, const std::string& new_relname, const std::string& new_absname)
{
    sys::makedirs(str::dirname(new_absname));

    // Sanity checks: avoid conflicts
    if (sys::exists(new_absname) || sys::exists(new_absname + ".tar") || sys::exists(new_absname + ".gz"))
    {
        stringstream ss;
        ss << "cannot archive " << absname << " to " << new_absname << " because the destination already exists";
        throw runtime_error(ss.str());
    }

    // Remove stale metadata and summaries that may have been left around
    sys::unlink_ifexists(new_absname + ".metadata");
    sys::unlink_ifexists(new_absname + ".summary");

    move_data(new_root, new_relname, new_absname);

    // Move metadata to archive
    sys::rename_ifexists(absname + ".metadata", new_absname + ".metadata");
    sys::rename_ifexists(absname + ".summary", new_absname + ".summary");

    root = new_root;
    relname = new_relname;
    absname = new_absname;
}

void Checker::test_truncate(const metadata::Collection& mds, unsigned data_idx)
{
    const auto& s = mds[data_idx].sourceBlob();
    test_truncate(s.offset);
}

std::shared_ptr<Checker> Checker::for_pathname(const std::string& format, const std::string& root, const std::string& relpath, const std::string& abspath, bool mock_data)
{
    std::unique_ptr<struct stat> st = sys::stat(abspath);
    std::shared_ptr<segment::Checker> res;
    if (!st.get())
        st = sys::stat(abspath + ".gz");
    if (!st.get())
    {
        st = sys::stat(abspath + ".tar");
        if (st.get())
        {
            res.reset(new segment::tar::Checker(root, relpath, abspath));
            return res;
        }
    }
    if (!st.get())
        return res;

    if (S_ISDIR(st->st_mode))
    {
        if (segment::dir::can_store(format))
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
