/*
 * scan/bufr - Scan a BUFR file for metadata.
 *
 * Copyright (C) 2007--2009  ARPA-SIM <urpsim@smr.arpa.emr.it>
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

#include "bufr.h"
#include <dballe++/error.h>
#include <arki/metadata.h>
#include <arki/types/origin.h>
#include <arki/types/product.h>
#include <arki/types/reftime.h>
#include <arki/types/run.h>
#include <arki/scan/any.h>
#include <wibble/exception.h>
#include <wibble/sys/fs.h>
#include <sstream>

using namespace wibble;

namespace arki {
namespace scan {

namespace bufr {

struct BufrValidator : public Validator
{
	virtual ~BufrValidator() {}

	// Validate data found in a file
	virtual void validate(int fd, off_t offset, size_t size) const
	{
	}

	// Validate a memory buffer
	virtual void validate(const void* buf, size_t size) const
	{
	}
};

static BufrValidator bufr_validator;

const Validator& validator() { return bufr_validator; }

}


Bufr::Bufr(bool inlineData) : rmsg(0), msg(0), file(0), m_inline_data(inlineData)
{
	dballe::checked(dba_rawmsg_create(&rmsg));
	dballe::checked(bufrex_msg_create(BUFREX_BUFR, &msg));
}

Bufr::~Bufr()
{
	if (rmsg) dba_rawmsg_delete(rmsg);
	if (msg) bufrex_msg_delete(msg);
	if (file) dba_file_delete(file);
}

void Bufr::open(const std::string& filename)
{
	// Close the previous file if needed
	close();
	this->filename = sys::fs::abspath(filename);
	dballe::checked(dba_file_create(BUFR, filename.c_str(), "r", &file));
}

void Bufr::close()
{
	filename.clear();
	if (file)
	{
		dba_file_delete(file);
		file = 0;
	}
}

bool Bufr::next(Metadata& md)
{
	int found;
	dballe::checked(dba_file_read(file, rmsg, &found));
	if (!found) return false;

	dballe::checked(bufr_decoder_decode_header(rmsg, msg));

	md.create();

	// Set source
	if (m_inline_data)
		md.setInlineData("bufr", wibble::sys::Buffer(rmsg->buf, rmsg->len, false));
	else
		md.source = types::source::Blob::create("bufr", filename, rmsg->offset, rmsg->len);

	// Set reference time
	md.set(types::reftime::Position::create(new types::Time(msg->rep_year, msg->rep_month, msg->rep_day, msg->rep_hour, msg->rep_minute, msg->rep_second)));

	// Set run
	md.set(types::run::Minute::create(msg->rep_hour, msg->rep_minute));

	switch (msg->edition)
	{
		case 2:
		case 3:
		case 4:
			// No process?
			md.set(types::origin::BUFR::create(msg->opt.bufr.centre, msg->opt.bufr.subcentre));
			md.set(types::product::BUFR::create(msg->type, msg->subtype, msg->localsubtype));
			break;
		default:
		{
			std::stringstream str;
			str << "edition is " << msg->edition << " but I can only handle 3 and 4";
			throw wibble::exception::Consistency("extracting metadata from BUFR message", str.str());
		}
	}

	return true;
}

}
}
// vim:set ts=4 sw=4:
