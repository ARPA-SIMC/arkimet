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
#include <dballe/msg/repinfo.h>
#include <dballe/core/csv.h>
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
	to_rep_memo = read_map_to_rep_memo();
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

void Bufr::read_info_base(char* buf, ValueBag& area)
{
	uint16_t rep_cod;
	uint32_t lat;
	uint32_t lon;

	memcpy(&rep_cod, buf +  0, 2);
	memcpy(&lat,     buf +  2, 4);
	memcpy(&lon,     buf +  6, 4);

	rep_cod = ntohs(rep_cod);
	lat = ntohl(lat);
	lon = ntohl(lon);

	area.set("lat", Value::createInteger(lat * 10));
	area.set("lon", Value::createInteger(lon * 10));
	std::map<int, std::string>::const_iterator rm = to_rep_memo.find(rep_cod);
	if (rm == to_rep_memo.end())
		area.set("rep", Value::createString(str::fmt(rep_cod)));
	else
		area.set("rep", Value::createString(rm->second));
}

void Bufr::read_info_fixed(char* buf, Metadata& md)
{
	uint8_t block;
	uint16_t station;

	memcpy(&block,   buf + 10, 1);
	memcpy(&station, buf + 11, 2);
	station = ntohs(station);

	ValueBag area;
	read_info_base(buf, area);
	if (block) area.set("blo", Value::createInteger(block));
	if (station) area.set("sta", Value::createInteger(station));
	md.set(types::area::GRIB::create(area));
}

void Bufr::read_info_mobile(char* buf, Metadata& md)
{
	string ident;
	ident = string(buf + 10, 9);

	ValueBag area;
	read_info_base(buf, area);
	area.set("ide", Value::createString(ident));
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

namespace {
	struct fill_to_repmemo
	{
		std::map<int, std::string>& dest;
		fill_to_repmemo(std::map<int, std::string>& dest) : dest(dest) {}
		void operator()(int cod, const std::string& memo)
		{
			dest.insert(make_pair(cod, memo));
		}
	};
	struct fill_to_repcod
	{
		std::map<std::string, int>& dest;
		fill_to_repcod(std::map<std::string, int>& dest) : dest(dest) {}
		void operator()(int cod, const char* memo)
		{
			dest.insert(make_pair(memo, cod));
		}
	};
}

static inline void inplace_tolower(char* buf)
{
	for ( ; *buf; ++buf)
		*buf = tolower(*buf);
}

template<typename T>
static dba_err read_repinfo(const std::string& fname, T& consumer)
{
	dba_err err = DBA_OK;
	FILE* in;
	const char* name;

	if (fname.empty())
		DBA_RUN_OR_RETURN(dba_repinfo_default_filename(&name));
	else
		name = fname.c_str();


	/* Open the input CSV file */
	in = fopen(name, "r");
	if (in == NULL)
		return dba_error_system("opening file %s", name);

	/* Read the CSV file */
	{
		char* columns[7];
		int line;
		int i;

		for (line = 0; (i = dba_csv_read_next(in, columns, 7)) != 0; line++)
		{
			int id;

			if (i != 6)
			{
				err = dba_error_parse(name, line, "Expected 6 columns, got %d", i);
				goto cleanup;
			}

			// Lowercase all rep_memos
			inplace_tolower(columns[1]);
			id = strtol(columns[0], 0, 10);

			// Hand it to consumer
			consumer(id, columns[1]);

			for (int j = 0; j < 6; ++j)
				free(columns[j]);
		}
	}

cleanup:
	fclose(in);
	return err == DBA_OK ? dba_error_ok() : err;
}

std::map<std::string, int> Bufr::read_map_to_rep_cod(const std::string& fname)
{
	std::map<std::string, int> res;
	fill_to_repcod consumer(res);
	dballe::checked(read_repinfo(fname, consumer));
	return res;
}

std::map<int, std::string> Bufr::read_map_to_rep_memo(const std::string& fname)
{
	std::map<int, std::string> res;
	fill_to_repmemo consumer(res);
	dballe::checked(read_repinfo(fname, consumer));
	return res;
}

}
}
// vim:set ts=4 sw=4:
