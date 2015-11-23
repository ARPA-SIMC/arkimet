/*
 * data - Read/write functions for data blobs
 *
 * Copyright (C) 2012--2015  ARPA-SIM <urpsim@smr.arpa.emr.it>
 *
 * This program is free software; you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation; either version 2 of the License, or
 * (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License along
 * with this program; if not, write to the Free Software Foundation, Inc.,
 * 51 Franklin Street, Fifth Floor, Boston, MA 02110-1301 USA.
 *
 * Author: Enrico Zini <enrico@enricozini.com>
 */

#include "data.h"
#include "data/concat.h"
#include "data/lines.h"
#include "data/dir.h"
#include "arki/configfile.h"
#include "arki/scan/any.h"
#include "arki/metadata/collection.h"
#include "arki/utils.h"
#include <wibble/exception.h>
#include <wibble/string.h>
#include <wibble/sys/fs.h>

using namespace std;
using namespace wibble;

namespace arki {
namespace dataset {
namespace data {

std::string FileState::to_string() const
{
    vector<const char*> res;
    if (value == FILE_OK)        res.push_back("OK");
    if (value & FILE_TO_ARCHIVE) res.push_back("TO_ARCHIVE");
    if (value & FILE_TO_DELETE)  res.push_back("TO_DELETE");
    if (value & FILE_TO_PACK)    res.push_back("TO_PACK");
    if (value & FILE_TO_INDEX)   res.push_back("TO_INDEX");
    if (value & FILE_TO_RESCAN)  res.push_back("TO_RESCAN");
    if (value & FILE_TO_DEINDEX) res.push_back("TO_DEINDEX");
    if (value & FILE_ARCHIVED)   res.push_back("ARCHIVED");
    return str::join(res.begin(), res.end(), ",");
}

Segment::Segment(const std::string& relname, const std::string& absname)
    : relname(relname), absname(absname), payload(0)
{
}

Segment::~Segment()
{
    if (payload) delete payload;
}

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
            wibble::sys::fs::mkpath(absname.substr(0, pos));

        // Refuse to write to compressed files
        if (scan::isCompressed(absname))
            throw wibble::exception::Consistency("accessing data file " + relname,
                    "cannot update compressed data files: please manually uncompress it first");

        // Else we need to create an appropriate one
        unique_ptr<data::Segment> new_writer(create_for_format(format, relname, absname));
        return segments.add(move(new_writer));
    }

    // Instantiate the right Segment implementation for a segment that already
    // exists. Returns 0 if the segment does not exist.
    unique_ptr<data::Segment> create_for_existing_segment(const std::string& format, const std::string& relname, const std::string& absname)
    {
        std::unique_ptr<struct stat> st = sys::fs::stat(absname);
        unique_ptr<data::Segment> res;
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

    virtual unique_ptr<data::Segment> create_for_format(const std::string& format, const std::string& relname, const std::string& absname) = 0;

    Pending repack(const std::string& relname, metadata::Collection& mds)
    {
        string format = utils::get_format(relname);
        string absname = str::joinpath(root, relname);
        unique_ptr<data::Segment> maint(create_for_format(format, relname, absname));
        return maint->repack(root, mds);
    }

    FileState check(const std::string& relname, const metadata::Collection& mds, bool quick=true)
    {
        string format = utils::get_format(relname);
        string absname = str::joinpath(root, relname);
        unique_ptr<data::Segment> maint(create_for_format(format, relname, absname));
        return maint->check(mds, quick);
    }

    size_t remove(const std::string& relname)
    {
        string format = utils::get_format(relname);
        string absname = str::joinpath(root, relname);
        unique_ptr<data::Segment> maint(create_for_format(format, relname, absname));
        return maint->remove();
    }

    void truncate(const std::string& relname, size_t offset)
    {
        string format = utils::get_format(relname);
        string absname = str::joinpath(root, relname);
        unique_ptr<data::Segment> maint(create_for_format(format, relname, absname));
        return maint->truncate(offset);
    }
};

/// Segment manager that picks the right readers/writers based on file types
struct AutoSegmentManager : public BaseSegmentManager
{
    AutoSegmentManager(const std::string& root, bool mockdata=false)
        : BaseSegmentManager(root, mockdata) {}

    unique_ptr<data::Segment> create_for_format(const std::string& format, const std::string& relname, const std::string& absname)
    {
        unique_ptr<data::Segment> res(create_for_existing_segment(format, relname, absname));
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

    unique_ptr<data::Segment> create_for_format(const std::string& format, const std::string& relname, const std::string& absname)
    {
        unique_ptr<data::Segment> res(create_for_existing_segment(format, relname, absname));
        if (res.get()) return res;
        return unique_ptr<data::Segment>(new dir::Segment(format, relname, absname));
    }
};

/// Segment manager that always uses hole file segments
struct HoleDirSegmentManager : public BaseSegmentManager
{
    HoleDirSegmentManager(const std::string& root) : BaseSegmentManager(root) {}

    unique_ptr<data::Segment> create_for_format(const std::string& format, const std::string& relname, const std::string& absname)
    {
        return unique_ptr<data::Segment>(new dir::HoleSegment(format, relname, absname));
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
