#include "segment.h"
#include "segment/concat.h"
#include "segment/lines.h"
#include "segment/dir.h"
#include "arki/configfile.h"
#include "arki/exceptions.h"
#include "arki/scan/any.h"
#include "arki/metadata/collection.h"
#include "arki/metadata.h"
#include "arki/types/source/blob.h"
#include "arki/utils.h"
#include "arki/utils/string.h"
#include "arki/utils/sys.h"

using namespace std;
using namespace arki::utils;

namespace arki {
namespace dataset {

Segment::Segment(const std::string& relname, const std::string& absname)
    : relname(relname), absname(absname), payload(0)
{
}

Segment::~Segment()
{
    if (payload) delete payload;
}

void Segment::test_truncate(const metadata::Collection& mds, unsigned data_idx)
{
    const auto& s = mds[data_idx].sourceBlob();
    truncate(s.offset);
}


namespace segment {

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

namespace {

struct BaseSegmentManager : public segment::SegmentManager
{
    bool mockdata;

    BaseSegmentManager(const std::string& root, bool mockdata=false) : SegmentManager(root), mockdata(mockdata) {}

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
                    throw_consistency_error("mockdata single-file line-based segments not implemented");
                else
                    res.reset(new lines::Segment(relname, absname));
            } else {
                throw_consistency_error(
                        "getting segment for " + format + " file " + relname,
                        "format not supported");
            }
        }
        return res;
    }

    Pending repack(const std::string& relname, metadata::Collection& mds, unsigned test_flags=0)
    {
        string format = utils::get_format(relname);
        string absname = str::joinpath(root, relname);
        unique_ptr<Segment> maint(create_for_format(format, relname, absname));
        return maint->repack(root, mds, test_flags);
    }

    State check(dataset::Reporter& reporter, const std::string& ds, const std::string& relname, const metadata::Collection& mds, bool quick=true)
    {
        string format = utils::get_format(relname);
        string absname = str::joinpath(root, relname);
        unique_ptr<Segment> maint(create_for_format(format, relname, absname));
        return maint->check(reporter, ds, mds, quick);
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

    bool _is_segment(const std::string& format, const std::string& relname) override
    {
        string absname = str::joinpath(root, relname);

        if (sys::isdir(absname))
            // If it's a directory, it must be a dir segment
            return sys::exists(str::joinpath(absname, ".sequence"));

        // If it's not a directory, it must exist in thee file system,
        // compressed or not
        if (!sys::exists(absname) && !sys::exists(absname + ".gz"))
            return false;

        if (format == "grib" || format == "grib1" || format == "grib2")
        {
            return true;
        } else if (format == "bufr") {
            return true;
        } else if (format == "odimh5" || format == "h5" || format == "odim") {
            // HDF5 files cannot be appended, and they cannot be a segment of
            // their own
            return false;
        } else if (format == "vm2") {
            return true;
        } else {
            return false;
        }
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
                throw_consistency_error("mockdata single-file line-based segments not implemented");
            else
                res.reset(new lines::Segment(relname, absname));
        } else {
            throw_consistency_error(
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


SegmentManager::SegmentManager(const std::string& root)
    : root(root)
{
}

SegmentManager::~SegmentManager()
{
}

void SegmentManager::flush_writers()
{
    segments.clear();
}

void SegmentManager::foreach_cached(std::function<void(Segment&)> func)
{
    segments.foreach_cached(func);
}

Segment* SegmentManager::get_segment(const std::string& relname)
{
    return get_segment(utils::get_format(relname), relname);
}

Segment* SegmentManager::get_segment(const std::string& format, const std::string& relname)
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
        throw_consistency_error("accessing data file " + relname,
                "cannot update compressed data files: please manually uncompress it first");

    // Else we need to create an appropriate one
    unique_ptr<Segment> new_writer(create_for_format(format, relname, absname));
    return segments.add(move(new_writer));
}

bool SegmentManager::is_segment(const std::string& relname)
{
    return _is_segment(utils::get_format(relname), relname);
}

bool SegmentManager::is_segment(const std::string& format, const std::string& relname)
{
    return _is_segment(format, relname);
}

std::unique_ptr<SegmentManager> SegmentManager::get(const std::string& root, bool force_dir, bool mock_data)
{
    if (force_dir)
        if (mock_data)
            return unique_ptr<SegmentManager>(new HoleDirSegmentManager(root));
        else
            return unique_ptr<SegmentManager>(new ForceDirSegmentManager(root));
    else
        return unique_ptr<SegmentManager>(new AutoSegmentManager(root, mock_data));
}

}
}
}
