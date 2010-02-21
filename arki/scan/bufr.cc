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
#include <arki/types/area.h>
#include <arki/types/run.h>
#include <arki/scan/any.h>
#include <wibble/exception.h>
#include <wibble/sys/fs.h>
#include <sstream>
#include <cstring>
#include <stdint.h>
#include <arpa/inet.h>

using namespace std;
using namespace wibble;

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

static void read_info_fixed(char* buf, Metadata& md)
{
	uint16_t rep_cod;
	uint32_t lat;
	uint32_t lon;
	uint8_t block;
	uint16_t station;

	memcpy(&rep_cod, buf +  0, 2);
	memcpy(&lat,     buf +  2, 4);
	memcpy(&lon,     buf +  6, 4);
	memcpy(&block,   buf + 10, 1);
	memcpy(&station, buf + 11, 2);

	rep_cod = ntohs(rep_cod);
	lat = ntohl(lat);
	lon = ntohl(lon);
	station = ntohs(station);

	ValueBag area;
	area.set("lat", Value::createInteger(lat * 10));
	area.set("lon", Value::createInteger(lon * 10));
	if (block) area.set("blo", Value::createInteger(block));
	if (station) area.set("sta", Value::createInteger(station));
	area.set("rep", Value::createString("TODO")); // reconstructed rep_memo here
	md.set(types::area::GRIB::create(area));
}

static void read_info_mobile(char* buf, Metadata& md)
{
	uint16_t rep_cod;
	uint32_t lat;
	uint32_t lon;
	string ident;

	memcpy(&rep_cod, buf +  0, 2);
	memcpy(&lat,     buf +  2, 4);
	memcpy(&lon,     buf +  6, 4);
	ident = string(buf + 10, 9);

	rep_cod = ntohs(rep_cod);
	lat = ntohl(lat);
	lon = ntohl(lon);

	ValueBag area;
	area.set("lat", Value::createInteger(lat * 10));
	area.set("lon", Value::createInteger(lon * 10));
	area.set("ide", Value::createString(ident));
	area.set("rep", Value::createString("TODO")); // reconstructed rep_memo here
	md.set(types::area::GRIB::create(area));
}


bool Bufr::next(Metadata& md)
{
	int found;
	dballe::checked(dba_file_read(file, rmsg, &found));
	if (!found) return false;

	dballe::checked(bufr_decoder_decode_header(rmsg, msg));

	md.create();

	// Detect optional section and handle it
	switch (msg->opt.bufr.optional_section_length)
	{
		case 14: read_info_fixed(msg->opt.bufr.optional_section, md); break;
		case 20: read_info_mobile(msg->opt.bufr.optional_section, md); break;
		default: break;
	}

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
