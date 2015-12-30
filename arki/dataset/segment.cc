#include "segment.h"
#include "segment/concat.h"
#include "segment/lines.h"
#include "segment/dir.h"
#include "arki/configfile.h"
#include "arki/scan/any.h"
#include "arki/metadata/collection.h"
#include "arki/utils.h"
#include "arki/utils/string.h"
#include "arki/utils/sys.h"
#include <arki/wibble/exception.h>

using namespace std;
using namespace arki::utils;

namespace arki {
namespace dataset {
namespace segment {

std::string State::to_string() const
{
    vector<const char*> res;
    if (value == SEGMENT_OK)         res.push_back("OK");
    if (value & SEGMENT_DIRTY)       res.push_back("DIRTY");
    if (value & SEGMENT_UNALIGNED)   res.push_back("UNALIGNED");
    if (value & SEGMENT_DELETED)     res.push_back("DELETED");
    if (value & SEGMENT_NEW)         res.push_back("NEW");
    if (value & SEGMENT_CORRUPTED)   res.push_back("CORRUPTED");
    if (value & SEGMENT_ARCHIVE_AGE) res.push_back("ARCHIVE_AGE");
    if (value & SEGMENT_DELETE_AGE)  res.push_back("DELETE_AGE");
    return str::join(",", res.begin(), res.end());
}

}

Segment::Segment(const std::string& relname, const std::string& absname)
    : relname(relname), absname(absname), payload(0)
{
}

Segment::~Segment()
{
    if (payload) delete payload;
}

namespace segment {

OstreamWriter::~OstreamWriter()
{
}

const OstreamWriter* OstreamWriter::get(const std::string& format)
{
    static concat::OstreamWriter* ow_concat = 0;
    static lines::OstreamWriter* ow_lines = 0;

    if (format == "grib" || format == "grib1" || format == "grib2")
    {
        if (!ow_concat)
            ow_concat = new concat::OstreamWriter;
        return ow_concat;
    } else if (format == "bufr") {
        if (!ow_concat)
            ow_concat = new concat::OstreamWriter;
        return ow_concat;
    } else if (format == "odimh5" || format == "h5" || format == "odim") {
        if (!ow_concat)
            ow_concat = new concat::OstreamWriter;
        return ow_concat;
    } else if (format == "vm2") {
        if (!ow_lines)
            ow_lines = new lines::OstreamWriter;
        return ow_lines;
    } else {
        throw wibble::exception::Consistency(
                "getting ostream writer for " + format,
                "format not supported");
    }
}


namespace {

struct BaseSegmentManager : public SegmentManager
{
    std::string root;
    bool mockdata;

    BaseSegmentManager(const std::string& root, bool mockdata=false) : root(root), mockdata(mockdata) {}

    Segment* get_segment(const std::string& relname) override
    {
        return get_segment(utils::get_format(relname), relname);
    }

    Segment* get_segment(const std::string& format, const std::string& relname) override
    {
        // Try to reuse an existing instance
        Segment* res = segments.get(relname);
        if (res) return res;

        // Ensure that the directory for 'relname' exists
        string absname = str::joinpath(root, relname);
        size_t pos = absname.rfind('/');
        if (pos != string::npos)
            sys::makedirs(absname.substr(0, pos));

        // Refuse to write to compressed files
        if (scan::isCompressed(absname))
            throw wibble::exception::Consistency("accessing data file " + relname,
                    "cannot update compressed data files: please manually uncompress it first");

        // Else we need to create an appropriate one
        unique_ptr<Segment> new_writer(create_for_format(format, relname, absname));
        return segments.add(move(new_writer));
    }

    // Instantiate the right Segment implementation for a segment that already
    // exists. Returns 0 if the segment does not exist.
    unique_ptr<Segment> create_for_existing_segment(const std::string& format, const std::string& relname, const std::string& absname)
    {
        std::unique_ptr<struct stat> st = sys::stat(absname);
        unique_ptr<Segment> res;
        if (!st.get())
            return res;

        if (S_ISDIR(st->st_mode))
        {
            if (mockdata)
                res.reset(new dir::HoleSegment(format, relname, absname));
            else
                res.reset(new dir::Segment(format, relname, absname));
        } else {
            if (format == "grib" || format == "grib1" || format == "grib2")
            {
                if (mockdata)
                    res.reset(new concat::HoleSegment(relname, absname));
                else
                    res.reset(new concat::Segment(relname, absname));
            } else if (format == "bufr") {
                if (mockdata)
                    res.reset(new concat::HoleSegment(relname, absname));
                else
                    res.reset(new concat::Segment(relname, absname));
            } else if (format == "vm2") {
                if (mockdata)
                    throw wibble::exception::Consistency("mockdata single-file line-based segments not implemented");
                else
                    res.reset(new lines::Segment(relname, absname));
            } else {
                throw wibble::exception::Consistency(
                        "getting segment for " + format + " file " + relname,
                        "format not supported");
            }
        }
        return res;
    }

    virtual unique_ptr<Segment> create_for_format(const std::string& format, const std::string& relname, const std::string& absname) = 0;

    Pending repack(const std::string& relname, metadata::Collection& mds)
    {
        string format = utils::get_format(relname);
        string absname = str::joinpath(root, relname);
        unique_ptr<Segment> maint(create_for_format(format, relname, absname));
        return maint->repack(root, mds);
    }

    State check(const std::string& relname, const metadata::Collection& mds, bool quick=true)
    {
        string format = utils::get_format(relname);
        string absname = str::joinpath(root, relname);
        unique_ptr<Segment> maint(create_for_format(format, relname, absname));
        return maint->check(mds, quick);
    }

    size_t remove(const std::string& relname)
    {
        string format = utils::get_format(relname);
        string absname = str::joinpath(root, relname);
        unique_ptr<Segment> maint(create_for_format(format, relname, absname));
        return maint->remove();
    }

    void truncate(const std::string& relname, size_t offset)
    {
        string format = utils::get_format(relname);
        string absname = str::joinpath(root, relname);
        unique_ptr<Segment> maint(create_for_format(format, relname, absname));
        return maint->truncate(offset);
    }
};

/// Segment manager that picks the right readers/writers based on file types
struct AutoSegmentManager : public BaseSegmentManager
{
    AutoSegmentManager(const std::string& root, bool mockdata=false)
        : BaseSegmentManager(root, mockdata) {}

    unique_ptr<Segment> create_for_format(const std::string& format, const std::string& relname, const std::string& absname)
    {
        unique_ptr<Segment> res(create_for_existing_segment(format, relname, absname));
        if (res.get()) return res;

        if (format == "grib" || format == "grib1" || format == "grib2")
        {
            if (mockdata)
                res.reset(new concat::HoleSegment(relname, absname));
            else
                res.reset(new concat::Segment(relname, absname));
        } else if (format == "bufr") {
            if (mockdata)
                res.reset(new concat::HoleSegment(relname, absname));
            else
                res.reset(new concat::Segment(relname, absname));
        } else if (format == "odimh5" || format == "h5" || format == "odim") {
            if (mockdata)
                res.reset(new dir::HoleSegment(format, relname, absname));
            else
                res.reset(new dir::Segment(format, relname, absname));
        } else if (format == "vm2") {
            if (mockdata)
                throw wibble::exception::Consistency("mockdata single-file line-based segments not implemented");
            else
                res.reset(new lines::Segment(relname, absname));
        } else {
            throw wibble::exception::Consistency(
                    "getting writer for " + format + " file " + relname,
                    "format not supported");
        }
        return res;
    }
};

/// Segment manager that always picks directory segments
struct ForceDirSegmentManager : public BaseSegmentManager
{
    ForceDirSegmentManager(const std::string& root) : BaseSegmentManager(root) {}

    unique_ptr<Segment> create_for_format(const std::string& format, const std::string& relname, const std::string& absname)
    {
        unique_ptr<Segment> res(create_for_existing_segment(format, relname, absname));
        if (res.get()) return res;
        return unique_ptr<Segment>(new dir::Segment(format, relname, absname));
    }
};

/// Segment manager that always uses hole file segments
struct HoleDirSegmentManager : public BaseSegmentManager
{
    HoleDirSegmentManager(const std::string& root) : BaseSegmentManager(root) {}

    unique_ptr<Segment> create_for_format(const std::string& format, const std::string& relname, const std::string& absname)
    {
        return unique_ptr<Segment>(new dir::HoleSegment(format, relname, absname));
    }
};

}

SegmentManager::~SegmentManager()
{
}

void SegmentManager::flush_writers()
{
    segments.clear();
}

std::unique_ptr<SegmentManager> SegmentManager::get(const std::string& root)
{
    return unique_ptr<SegmentManager>(new AutoSegmentManager(root));
}

std::unique_ptr<SegmentManager> SegmentManager::get(const ConfigFile& cfg)
{
    string root = cfg.value("path");
    bool mockdata = cfg.value("mockdata") == "true";
    if (cfg.value("segments") == "dir")
    {
        if (mockdata)
            return unique_ptr<SegmentManager>(new HoleDirSegmentManager(root));
        else
            return unique_ptr<SegmentManager>(new ForceDirSegmentManager(root));
    }
    else
        return unique_ptr<SegmentManager>(new AutoSegmentManager(root, mockdata));
}


}
}
}
