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

#include "arki/data.h"
#include "arki/data/concat.h"
#include "arki/data/lines.h"
#include "arki/configfile.h"
#include "arki/scan/any.h"
#include "arki/metadata/collection.h"
#include <wibble/exception.h>
#include <wibble/string.h>
#include <wibble/sys/fs.h>

using namespace std;
using namespace wibble;

namespace arki {
namespace data {


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

Info::~Info()
{
}

const Info* Info::get(const std::string& format)
{
    static concat::Info* i_concat = 0;
    static lines::Info* i_lines = 0;

    if (format == "grib" || format == "grib1" || format == "grib2")
    {
        if (!i_concat)
            i_concat = new concat::Info;
        return i_concat;
    } else if (format == "bufr") {
        if (!i_concat)
            i_concat = new concat::Info;
        return i_concat;
    } else if (format == "odimh5" || format == "h5" || format == "odim") {
        if (!i_concat)
            i_concat = new concat::Info;
        return i_concat;
    } else if (format == "vm2") {
        if (!i_lines)
            i_lines = new lines::Info;
        return i_lines;
    } else {
        throw wibble::exception::Consistency(
                "getting ostream writer for " + format,
                "format not supported");
    }
}

namespace {

// Get format from file extension
std::string get_format(const std::string& fname)
{
    std::string fmt;
    size_t pos;
    if ((pos = fname.rfind('.')) != std::string::npos)
        fmt = fname.substr(pos + 1);
    return fmt;
}

/// Segment manager that picks the right readers/writers based on file types
struct AutoSegmentManager : public SegmentManager
{
    std::string root;

    AutoSegmentManager(const std::string& root) : root(root) {}

    Reader* get_reader(const std::string& relname)
    {
        return get_reader(get_format(relname), relname);
    }

    Reader* get_reader(const std::string& format, const std::string& relname)
    {
        // TODO: readers not yet implemented
        return 0;
    }

    Writer* get_writer(const std::string& relname)
    {
        return get_writer(get_format(relname), relname);
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
            new_writer.reset(new concat::Writer(relname, absname, truncate));
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
        auto_ptr<data::Writer> writer(create_for_format(get_format(relname), tmprelname, tmpabsname, true));

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
#if 0

void FileCopier::operator()(const std::string& file, const metadata::Collection& mdc)
{
    // Deindex the file
    m_idx.reset(file);
    for (metadata::Collection::const_iterator i = mdc.begin(); i != mdc.end(); ++i)
    {
        // Read the data
        wibble::sys::Buffer buf = i->getData();
        // Validate it
        m_val.validate(buf.data(), buf.size());
        // Append it to the new file
        off_t w_off = writer.append(buf);
        // Reindex it
        m_idx.index(*i, file, w_off);
    }
}

bool FileCopier::operator()(Metadata& md)
{
	// Read the data
	wibble::sys::Buffer buf = md.getData();

	// Check it for corruption
	m_val.validate(buf.data(), buf.size());

	// Write it out
    data::Writer writer = data::Writer::get(md.source->format, dst);
    w_off = writer.append(buf);

	// Update the Blob source using the new position
	md.source = types::source::Blob::create(md.source->format, finalbasedir, finalname, w_off, buf.size());

	return true;
}
#endif
        throw wibble::exception::Consistency("SegmentManager::repack not implemented");
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

std::auto_ptr<SegmentManager> SegmentManager::get(const ConfigFile& cfg)
{
    string root = cfg.value("path");
    return auto_ptr<SegmentManager>(new AutoSegmentManager(root));
}


}
}
