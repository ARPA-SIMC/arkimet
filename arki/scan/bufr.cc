/*
 * scan/bufr - Scan a BUFR file for metadata.
 *
 * Copyright (C) 2007--2015  ARPA-SIM <urpsim@smr.arpa.emr.it>
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

#include "config.h"

#include <arki/scan/bufr.h>
#include <arki/utils/files.h>
#include <dballe/file.h>
#include <dballe/msg/msgs.h>
#include <dballe/msg/codec.h>
#include <dballe/core/csv.h>
#include <wreport/bulletin.h>
#include <wreport/options.h>
#include <arki/metadata.h>
#include <arki/types/origin.h>
#include <arki/types/product.h>
#include <arki/types/reftime.h>
#include <arki/types/run.h>
#include <arki/types/timerange.h>
#include <arki/scan/any.h>
#include <wibble/exception.h>
#include <wibble/sys/fs.h>
#include <wibble/grcal/grcal.h>
#include <sstream>
#include <cstring>
#include <stdint.h>
#include <arpa/inet.h>

#ifdef HAVE_LUA
#include <arki/scan/bufrlua.h>
#endif

using namespace std;
using namespace wibble;
using namespace wreport;
using namespace dballe;
using namespace arki::types;

namespace arki {
namespace scan {

namespace bufr {

struct BufrValidator : public Validator
{
	virtual ~BufrValidator() {}

	// Validate data found in a file
	virtual void validate(int fd, off_t offset, size_t size, const std::string& fname) const
	{
		char buf[4];
		ssize_t res;
		if (size < 8)
			throw wibble::exception::Consistency("checking BUFR segment in file " + fname, "buffer is shorter than 8 bytes");
		if ((res = pread(fd, buf, 4, offset)) == -1)
			throw wibble::exception::System("reading 4 bytes of BUFR header from " + fname);
		if (res != 4)
			throw wibble::exception::Consistency("reading 4 bytes of BUFR header from " + fname, "partial read");
		if (memcmp(buf, "BUFR", 4) != 0)
			throw wibble::exception::Consistency("checking BUFR segment in file " + fname, "segment does not start with 'BUFR'");
		if ((res = pread(fd, buf, 4, offset + size - 4)) == -1)
			throw wibble::exception::System("reading 4 bytes of BUFR trailer from " + fname);
		if (res != 4)
			throw wibble::exception::Consistency("reading 4 bytes of BUFR trailer from " + fname, "partial read");
		if (memcmp(buf, "7777", 4) != 0)
			throw wibble::exception::Consistency("checking BUFR segment in file " + fname, "segment does not end with 'BUFR'");
	}

	// Validate a memory buffer
	virtual void validate(const void* buf, size_t size) const
	{
		if (size < 8)
			throw wibble::exception::Consistency("checking BUFR buffer", "buffer is shorter than 8 bytes");
		if (memcmp(buf, "BUFR", 4) != 0)
			throw wibble::exception::Consistency("checking BUFR buffer", "buffer does not start with 'BUFR'");
		if (memcmp((const char*)buf + size - 4, "7777", 4) != 0)
			throw wibble::exception::Consistency("checking BUFR buffer", "buffer does not end with '7777'");
	}
};

static BufrValidator bufr_validator;

const Validator& validator() { return bufr_validator; }

}


Bufr::Bufr() : file(0), importer(0), extras(0)
{
	msg::Importer::Options opts;
	opts.simplified = true;
	importer = msg::Importer::create(File::BUFR, opts).release();

#ifdef HAVE_LUA
	extras = new bufr::BufrLua;
#endif
}

Bufr::~Bufr()
{
	if (importer) delete importer;
	if (file) delete file;
#ifdef HAVE_LUA
	if (extras) delete extras;
#endif
}

void Bufr::open(const std::string& filename)
{
    string basedir, relname;
    utils::files::resolve_path(filename, basedir, relname);
    open(sys::fs::abspath(filename), basedir, relname);
}

void Bufr::open(const std::string& filename, const std::string& basedir, const std::string& relname)
{
    // Close the previous file if needed
    close();
    this->filename = filename;
    this->basedir = basedir;
    this->relname = relname;
    if (filename == "-")
        file = File::create(File::BUFR, stdin, false, "standard input").release();
    else
        file = File::create(File::BUFR, filename.c_str(), "r").release();
}

void Bufr::close()
{
    filename.clear();
    basedir.clear();
    relname.clear();
    if (file)
    {
        delete file;
        file = 0;
    }
}

namespace {
class Harvest
{
protected:
    Msgs msgs;

public:
    dballe::msg::Importer& importer;
    auto_ptr<reftime::Position> reftime;
    auto_ptr<Origin> origin;
    auto_ptr<product::BUFR> product;
    Msg* msg;

    Harvest(dballe::msg::Importer& importer) : importer(importer), msg(0) {}

    void refine_reftime(const dballe::Msg& msg)
    {
        Datetime dt = msg.datetime();
        if (dt.is_missing()) return;
        reftime->time = types::Time(
                dt.year, dt.month, dt.day,
                dt.hour, dt.minute, dt.second);
    }

    void harvest_from_dballe(const BinaryMessage& rmsg, Metadata& md)
    {
        msg = 0;
        auto_ptr<BufrBulletin> bulletin(BufrBulletin::create());
        bulletin->decode_header(rmsg.data, rmsg.pathname.c_str(), rmsg.offset);

#if 0
        // Detect optional section and handle it
        switch (msg->opt.bufr.optional_section_length)
        {
            case 14: read_info_fixed(msg->opt.bufr.optional_section, md); break;
            case 11: read_info_mobile(msg->opt.bufr.optional_section, md); break;
            default: break;
        }
#endif

        // Set reference time
        // FIXME: WRONG! The header date should ALWAYS be ignored
        reftime = reftime::Position::create(types::Time(
                bulletin->rep_year, bulletin->rep_month, bulletin->rep_day,
                bulletin->rep_hour, bulletin->rep_minute, bulletin->rep_second));

        // Set origin from the bufr header
        switch (bulletin->edition)
        {
            case 2:
            case 3:
            case 4:
                // No process?
                origin = Origin::createBUFR(bulletin->centre, bulletin->subcentre);
                product = product::BUFR::create(bulletin->type, bulletin->subtype, bulletin->localsubtype);
                break;
            default:
                {
                    std::stringstream str;
                    str << "edition is " << bulletin->edition << " but I can only handle 3 and 4";
                    throw wibble::exception::Consistency("extracting metadata from BUFR message", str.str());
                }
        }

        // Default to a generic product unless we find more info later
        //md.set(types::product::BUFR::create("generic"));

        // Try to decode the data; if we fail, we are done
        try {
            // Ignore domain errors, it's ok if we lose some oddball data
            wreport::options::LocalOverride<bool> o(wreport::options::var_silent_domain_errors, true);

            bulletin->decode(rmsg.data);
        } catch (wreport::error& e) {
            // We can still try to handle partially decoded files

            // Not if the error was not a parse error
            if (e.code() != WR_ERR_PARSE) return;

            // Not if we didn't decode just one subset
            if (bulletin->subsets.size() != 1) return;

            // Not if the subset is empty
            if (bulletin->subsets[0].empty()) return;
        } catch (std::exception& e) {
            return;
        }

        // If there is more than one subset, we are done
        if (bulletin->subsets.size() != 1)
            return;

        // Try to parse as a dba_msg
        try {
            // Ignore domain errors, it's ok if we lose some oddball data
            wreport::options::LocalOverride<bool> o(wreport::options::var_silent_domain_errors, true);

            msgs = importer.from_bulletin(*bulletin);
        } catch (std::exception& e) {
            // If we cannot import it as a Msgs, we are done
            return;
        }

        // We decoded a different number of subsets than declared by the BUFR.
        // How could this happen? Treat it as an obscure BUFR and go no further
        if (msgs.size() != 1)
            return;

        msg = msgs[0];

        // Set the product from the msg type
        ValueBag newvals;
        newvals.set("t", Value::createString(msg_type_name(msg->type)));
        product->addValues(newvals);

        // Set reference time from date and time if available
        refine_reftime(*msg);
    }
};
}


bool Bufr::do_scan(Metadata& md)
{
    md.clear();

    BinaryMessage rmsg = file->read();
    if (!rmsg) return false;

    // Set source
    if (false)
        md.set_source_inline("bufr", wibble::sys::Buffer(rmsg.data.data(), rmsg.data.size()));
    else {
        auto_ptr<Source> source = Source::createBlob("bufr", basedir, relname, rmsg.offset, rmsg.data.size());
        md.set_source(source);
        md.set_cached_data(wibble::sys::Buffer(rmsg.data.data(), rmsg.data.size()));
    }

    Harvest harvest(*importer);
    harvest.harvest_from_dballe(rmsg, md);

    md.set(harvest.reftime);
    md.set(harvest.origin);
    md.set(harvest.product);

    if (extras && harvest.msg)
    {
        // DB-All.e managed to make sense of the message: hand it down to Lua
        // to extract further metadata
        extras->scan(*harvest.msg, md);
    }

    // Check that the date is a valid date, unset if it is rubbish
    const reftime::Position* rt = md.get<types::reftime::Position>();
    if (rt)
    {
        if (rt->time.vals[0] <= 0)
            md.unset(types::TYPE_REFTIME);
        else
        {
            types::Time t = rt->time;
            wibble::grcal::date::normalise(t.vals);
            if (t != rt->time) md.unset(types::TYPE_REFTIME);
        }
    }

    // Convert validity times to emission times
    // If md has a timedef, substract it from the reftime
    const timerange::Timedef* timedef = md.get<types::timerange::Timedef>();
    if (timedef)
        if (const reftime::Position* p = md.get<types::reftime::Position>())
            md.set(timedef->validity_time_to_emission_time(*p));

    return true;
}

bool Bufr::next(Metadata& md)
{
    return do_scan(md);
}

static inline void inplace_tolower(std::string& buf)
{
	for (std::string::iterator i = buf.begin(); i != buf.end(); ++i)
		*i = tolower(*i);
}

int Bufr::update_sequence_number(const std::string& buf)
{
    auto_ptr<BufrBulletin> bulletin(BufrBulletin::create());
    bulletin->decode_header(buf);
    return bulletin->update_sequence_number;
}

}
}
