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

/// Segment manager that picks the right readers/writers based on file types
struct AutoSegmentManager : public SegmentManager
{
    std::string root;

    AutoSegmentManager(const std::string& root) : root(root) {}

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

    auto_ptr<data::Writer> create_for_format(const std::string& format, const std::string& relname, const std::string& absname, bool truncate=false)
    {
        auto_ptr<data::Writer> new_writer;
        if (format == "grib" || format == "grib1" || format == "grib2")
        {
            new_writer.reset(new concat::Writer(relname, absname, truncate));
        } else if (format == "bufr") {
            new_writer.reset(new concat::Writer(relname, absname, truncate));
        } else if (format == "odimh5" || format == "h5" || format == "odim") {
            new_writer.reset(new dir::Writer(relname, absname, truncate));
        } else if (format == "vm2") {
            new_writer.reset(new lines::Writer(relname, absname, truncate));
        } else {
            throw wibble::exception::Consistency(
                    "getting writer for " + format + " file " + relname,
                    "format not supported");
        }
        return new_writer;
    }

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
        auto_ptr<data::Writer> new_writer(create_for_format(format, relname, absname));
        return writers.add(new_writer);
    }

    Pending repack(const std::string& relname, metadata::Collection& mds)
    {
        struct Rename : public Transaction
        {
            std::string tmpabsname;
            std::string absname;
            bool fired;

            Rename(const std::string& tmpabsname, const std::string& absname)
                : tmpabsname(tmpabsname), absname(absname), fired(false)
            {
            }

            virtual ~Rename()
            {
                if (!fired) rollback();
            }

            virtual void commit()
            {
                if (fired) return;
                // Rename the data file to its final name
                if (rename(tmpabsname.c_str(), absname.c_str()) < 0)
                    throw wibble::exception::System("renaming " + tmpabsname + " to " + absname);
                fired = true;
            }

            virtual void rollback()
            {
                if (fired) return;
                unlink(tmpabsname.c_str());
                fired = true;
            }
        };

        string absname = str::joinpath(root, relname);
        string tmprelname = relname + ".repack";
        string tmpabsname = str::joinpath(root, tmprelname);

        // Get a validator for this file
        const scan::Validator& validator = scan::Validator::by_filename(absname);

        // Create a writer for the temp file
        auto_ptr<data::Writer> writer(create_for_format(utils::get_format(relname), tmprelname, tmpabsname, true));

        // Fill the temp file with all the data in the right order
        for (metadata::Collection::iterator i = mds.begin(); i != mds.end(); ++i)
        {
            // Read the data
            wibble::sys::Buffer buf = i->getData();
            // Validate it
            validator.validate(buf.data(), buf.size());
            // Append it to the new file
            off_t w_off = writer->append(buf);
            // Update the source information in the metadata
            i->source = types::source::Blob::create(i->source->format, root, relname, w_off, buf.size());
        }

        // Close the temp file
        writer.release();

        return new Rename(tmpabsname, absname);
    }

    FileState check(const std::string& relname, const metadata::Collection& mds, bool quick=true)
    {
        string format = utils::get_format(relname);
        string absname = str::joinpath(root, relname);

        if (format == "grib" || format == "grib1" || format == "grib2")
        {
            return concat::Writer::check(absname, mds, quick);
        } else if (format == "bufr") {
            return concat::Writer::check(absname, mds, quick);
        } else if (format == "odimh5" || format == "h5" || format == "odim") {
            return dir::Writer::check(absname, mds, quick);
        } else if (format == "vm2") {
            return lines::Writer::check(absname, mds, quick);
        } else {
            throw wibble::exception::Consistency(
                    "checking " + format + " file " + relname,
                    "format not supported");
        }
    }

    size_t remove(const std::string& relname)
    {
        string format = utils::get_format(relname);
        string absname = str::joinpath(root, relname);

        if (format == "grib" || format == "grib1" || format == "grib2")
        {
            return concat::Writer::remove(absname);
        } else if (format == "bufr") {
            return concat::Writer::remove(absname);
        } else if (format == "odimh5" || format == "h5" || format == "odim") {
            return dir::Writer::remove(absname);
        } else if (format == "vm2") {
            return lines::Writer::remove(absname);
        } else {
            throw wibble::exception::Consistency(
                    "removing " + format + " file " + absname,
                    "format not supported");
        }
    }

    void truncate(const std::string& relname, size_t offset)
    {
        string format = utils::get_format(relname);
        string absname = str::joinpath(root, relname);

        if (format == "grib" || format == "grib1" || format == "grib2")
        {
            return fd::Writer::truncate(absname, offset);
        } else if (format == "bufr") {
            return fd::Writer::truncate(absname, offset);
        } else if (format == "odimh5" || format == "h5" || format == "odim") {
            return dir::Writer::truncate(absname, offset);
        } else if (format == "vm2") {
            return fd::Writer::truncate(absname, offset);
        } else {
            throw wibble::exception::Consistency(
                    "truncating " + format + " file " + absname,
                    "format not supported");
        }
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
    return auto_ptr<SegmentManager>(new AutoSegmentManager(root));
}


}
}
}
