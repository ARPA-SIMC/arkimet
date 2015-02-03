/*
 * data - Read/write functions for data blobs
 *
 * Copyright (C) 2012--2014  ARPA-SIM <urpsim@smr.arpa.emr.it>
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

Reader::~Reader() {}

Writer::Writer(const std::string& relname, const std::string& absname)
    : relname(relname), absname(absname), payload(0)
{
}

Writer::~Writer()
{
    if (payload) delete payload;
}

Maint::~Maint()
{
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

    BaseSegmentManager(const std::string& root) : root(root) {}

    Reader* get_reader(const std::string& relname)
    {
        return get_reader(utils::get_format(relname), relname);
    }

    Reader* get_reader(const std::string& format, const std::string& relname)
    {
        // TODO: readers not yet implemented
        return 0;
    }

    Writer* get_writer(const std::string& relname)
    {
        return get_writer(utils::get_format(relname), relname);
    }

    // Create the appropriate Writer for a segment that already exists. Returns
    // 0 if the segment does not exist.
    auto_ptr<data::Writer> create_writer_for_existing_segment(const std::string& format, const std::string& relname, const std::string& absname, bool truncate=false)
    {
        std::auto_ptr<struct stat> st = sys::fs::stat(absname);
        auto_ptr<data::Writer> new_writer;
        if (!st.get())
            return new_writer;

        if (S_ISDIR(st->st_mode))
        {
            new_writer.reset(new dir::Writer(format, relname, absname, truncate));
        } else {
            if (format == "grib" || format == "grib1" || format == "grib2")
            {
                new_writer.reset(new concat::Writer(relname, absname, truncate));
            } else if (format == "bufr") {
                new_writer.reset(new concat::Writer(relname, absname, truncate));
            } else if (format == "vm2") {
                new_writer.reset(new lines::Writer(relname, absname, truncate));
            } else {
                throw wibble::exception::Consistency(
                        "getting writer for " + format + " file " + relname,
                        "format not supported");
            }
        }
        return new_writer;
    }

    // Create the appropriate Maint for a segment that already exists. Retursn
    // 0 if the segment does not exist.
    auto_ptr<data::Maint> create_maint_for_existing_segment(const std::string& format, const std::string& relname, const std::string& absname)
    {
        std::auto_ptr<struct stat> st = sys::fs::stat(absname);
        auto_ptr<data::Maint> new_maint;
        if (!st.get())
            return new_maint;

        if (S_ISDIR(st->st_mode))
        {
            new_maint.reset(new dir::Maint);
        } else {
            if (format == "grib" || format == "grib1" || format == "grib2")
            {
                new_maint.reset(new concat::Maint);
            } else if (format == "bufr") {
                new_maint.reset(new concat::Maint);
            } else if (format == "vm2") {
                new_maint.reset(new lines::Maint);
            } else {
                throw wibble::exception::Consistency(
                        "preparing maintenance for " + format + " file " + relname,
                        "format not supported");
            }
        }
        return new_maint;
    }

    virtual auto_ptr<data::Writer> create_writer_for_format(const std::string& format, const std::string& relname, const std::string& absname, bool truncate=false) = 0;
    virtual auto_ptr<data::Maint> create_maint_for_format(const std::string& format, const std::string& relname, const std::string& absname) = 0;

    Writer* get_writer(const std::string& format, const std::string& relname)
    {
        // Try to reuse an existing instance
        Writer* res = writers.get(relname);
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
        auto_ptr<data::Writer> new_writer(create_writer_for_format(format, relname, absname));
        return writers.add(new_writer);
    }

    Pending repack(const std::string& relname, metadata::Collection& mds)
    {
        string format = utils::get_format(relname);
        string absname = str::joinpath(root, relname);
        auto_ptr<data::Maint> maint(create_maint_for_format(format, relname, absname));
        return maint->repack(root, relname, mds);
    }

    FileState check(const std::string& relname, const metadata::Collection& mds, bool quick=true)
    {
        string format = utils::get_format(relname);
        string absname = str::joinpath(root, relname);
        auto_ptr<data::Maint> maint(create_maint_for_format(format, relname, absname));
        return maint->check(absname, mds, quick);
    }

    size_t remove(const std::string& relname)
    {
        string format = utils::get_format(relname);
        string absname = str::joinpath(root, relname);
        auto_ptr<data::Maint> maint(create_maint_for_format(format, relname, absname));
        return maint->remove(absname);
    }

    void truncate(const std::string& relname, size_t offset)
    {
        string format = utils::get_format(relname);
        string absname = str::joinpath(root, relname);
        auto_ptr<data::Maint> maint(create_maint_for_format(format, relname, absname));
        return maint->truncate(absname, offset);
    }
};

/// Segment manager that picks the right readers/writers based on file types
struct AutoSegmentManager : public BaseSegmentManager
{
    bool mockdata;

    AutoSegmentManager(const std::string& root, bool mockdata=false)
        : BaseSegmentManager(root), mockdata(mockdata) {}

    auto_ptr<data::Writer> create_writer_for_format(const std::string& format, const std::string& relname, const std::string& absname, bool truncate=false)
    {
        auto_ptr<data::Writer> new_writer(create_writer_for_existing_segment(format, relname, absname, truncate));
        if (new_writer.get()) return new_writer;

        if (format == "grib" || format == "grib1" || format == "grib2")
        {
            if (mockdata)
                new_writer.reset(new concat::HoleWriter(relname, absname, truncate));
            else
                new_writer.reset(new concat::Writer(relname, absname, truncate));
        } else if (format == "bufr") {
            if (mockdata)
                new_writer.reset(new concat::HoleWriter(relname, absname, truncate));
            else
                new_writer.reset(new concat::Writer(relname, absname, truncate));
        } else if (format == "odimh5" || format == "h5" || format == "odim") {
            if (mockdata)
                new_writer.reset(new dir::HoleWriter(format, relname, absname, truncate));
            else
                new_writer.reset(new dir::Writer(format, relname, absname, truncate));
        } else if (format == "vm2") {
            if (mockdata)
                throw wibble::exception::Consistency("mockdata single-file line-based segments not implemented");
            else
                new_writer.reset(new lines::Writer(relname, absname, truncate));
        } else {
            throw wibble::exception::Consistency(
                    "getting writer for " + format + " file " + relname,
                    "format not supported");
        }
        return new_writer;
    }

    auto_ptr<data::Maint> create_maint_for_format(const std::string& format, const std::string& relname, const std::string& absname)
    {
        auto_ptr<data::Maint> new_maint(create_maint_for_existing_segment(format, relname, absname));
        if (new_maint.get()) return new_maint;

        if (format == "grib" || format == "grib1" || format == "grib2")
        {
            if (mockdata)
                new_maint.reset(new concat::HoleMaint);
            else
                new_maint.reset(new concat::Maint);
        } else if (format == "bufr") {
            if (mockdata)
                new_maint.reset(new concat::HoleMaint);
            else
                new_maint.reset(new concat::Maint);
        } else if (format == "odimh5" || format == "h5" || format == "odim") {
            if (mockdata)
                new_maint.reset(new dir::HoleMaint);
            else
                new_maint.reset(new dir::Maint);
        } else if (format == "vm2") {
            if (mockdata)
                throw wibble::exception::Consistency("mockdata single-file line-based maintenance not implemented");
            else
                new_maint.reset(new lines::Maint);
        } else {
            throw wibble::exception::Consistency(
                    "preparing maintenance for " + format + " file " + relname,
                    "format not supported");
        }
        return new_maint;
    }

};

/// Segment manager that always picks directory segments
struct ForceDirSegmentManager : public BaseSegmentManager
{
    ForceDirSegmentManager(const std::string& root) : BaseSegmentManager(root) {}

    auto_ptr<data::Writer> create_writer_for_format(const std::string& format, const std::string& relname, const std::string& absname, bool truncate=false)
    {
        auto_ptr<data::Writer> new_writer(create_writer_for_existing_segment(format, relname, absname, truncate));
        if (new_writer.get()) return new_writer;
        return auto_ptr<data::Writer>(new dir::Writer(format, relname, absname, truncate));
    }

    auto_ptr<data::Maint> create_maint_for_format(const std::string& format, const std::string& relname, const std::string& absname)
    {
        auto_ptr<data::Maint> new_maint(create_maint_for_existing_segment(format, relname, absname));
        if (new_maint.get()) return new_maint;
        return auto_ptr<data::Maint>(new dir::Maint);
    }
};

/// Segment manager that always uses hole file segments
struct HoleDirSegmentManager : public BaseSegmentManager
{
    HoleDirSegmentManager(const std::string& root) : BaseSegmentManager(root) {}

    auto_ptr<data::Writer> create_writer_for_format(const std::string& format, const std::string& relname, const std::string& absname, bool truncate=false)
    {
        return auto_ptr<data::Writer>(new dir::HoleWriter(format, relname, absname, truncate));
    }

    auto_ptr<data::Maint> create_maint_for_format(const std::string& format, const std::string& relname, const std::string& absname)
    {
        return auto_ptr<data::Maint>(new dir::HoleMaint);
    }
};

}

SegmentManager::~SegmentManager()
{
}

void SegmentManager::flush_writers()
{
    writers.clear();
}

std::auto_ptr<SegmentManager> SegmentManager::get(const std::string& root)
{
    return auto_ptr<SegmentManager>(new AutoSegmentManager(root));
}

std::auto_ptr<SegmentManager> SegmentManager::get(const ConfigFile& cfg)
{
    string root = cfg.value("path");
    bool mockdata = cfg.value("mockdata") == "true";
    if (cfg.value("segments") == "dir")
    {
        if (mockdata)
            return auto_ptr<SegmentManager>(new HoleDirSegmentManager(root));
        else
            return auto_ptr<SegmentManager>(new ForceDirSegmentManager(root));
    }
    else
        return auto_ptr<SegmentManager>(new AutoSegmentManager(root, mockdata));
}


}
}
}
