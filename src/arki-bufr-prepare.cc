/*
 * arki-bufr-prepare - Prepare BUFR messages for importing into arkimet
 *
 * Copyright (C) 2010  ARPA-SIM <urpsim@smr.arpa.emr.it>
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
 * You should have received a copy of the GNU General Public License
 * along with this program; if not, write to the Free Software
 * Foundation, Inc., 51 Franklin St, Fifth Floor, Boston, MA  02110-1301 USA
 *
 * Author: Enrico Zini <enrico@enricozini.com>
 */

#include <arki/runtime.h>
#include <arki/nag.h>
#include <arki/scan/bufr.h>

#if 0
#include <wibble/exception.h>
#include <wibble/commandline/parser.h>
#include <wibble/string.h>
#include <wibble/sys/fs.h>
#include <wibble/sys/exec.h>
#endif

#include <dballe/core/rawmsg.h>
#include <dballe/core/file.h>
#include <dballe/bufrex/msg.h>
#include <dballe/msg/msgs.h>
#include <dballe/msg/bufrex_codec.h>
#include <dballe++/error.h>

#include <arpa/inet.h>
#include <cstring>
#if 0
#include <cstdlib>
#include <unistd.h>
#endif

#include "config.h"

using namespace std;
using namespace arki;
using namespace wibble;

namespace wibble {
namespace commandline {

struct Options : public StandardParserWithManpage
{
	BoolOption* verbose;
	BoolOption* debug;
	StringOption* outfile;
	BoolOption* annotate;

	Options() : StandardParserWithManpage("arki-bufr-prepare", PACKAGE_VERSION, 1, PACKAGE_BUGREPORT)
	{
		usage = "[options] file1 file2...";
		description =
			"Read BUFR messages, encode each subsection in a "
			"separate message and add an optional section with "
			"information useful for arki-prepare";

		debug = add<BoolOption>("debug", 0, "debug", "", "debug output");
		verbose = add<BoolOption>("verbose", 0, "verbose", "", "verbose output");
		outfile = add<StringOption>("output", 'o', "output", "file",
				"write the output to the given file instead of standard output");
		annotate = add<BoolOption>("annotate", 0, "annotate", "", "add optional section with extra information from BUFR contents");
	}
};

}
}

static bool do_optional_section = false;

static std::map<std::string, int> to_rep_cod;

static dba_err copy_base_msg(bufrex_msg orig, bufrex_msg* newmsg)
{
	dba_err err = DBA_OK;
	bufrex_msg msg = NULL;

	DBA_RUN_OR_RETURN(bufrex_msg_create(BUFREX_BUFR, &msg));

	// Copy message attributes
	msg->type = orig->type;
	msg->subtype = orig->subtype;
	msg->localsubtype = orig->localsubtype;
	msg->edition = orig->edition;
	msg->rep_year = orig->rep_year;
	msg->rep_month = orig->rep_month;
	msg->rep_day = orig->rep_day;
	msg->rep_hour = orig->rep_hour;
	msg->rep_minute = orig->rep_minute;
	msg->rep_second = orig->rep_second;
	msg->opt.bufr.centre = orig->opt.bufr.centre;
	msg->opt.bufr.subcentre = orig->opt.bufr.subcentre;
	msg->opt.bufr.master_table = orig->opt.bufr.master_table;
	msg->opt.bufr.local_table = orig->opt.bufr.local_table;
	// Do not compress, since it only makes sense for multisubset messages
	msg->opt.bufr.compression = 0;

	// Copy data descriptor section
	for (bufrex_opcode i = orig->datadesc; i != NULL; i = i->next)
		DBA_RUN_OR_GOTO(cleanup, bufrex_msg_append_datadesc(msg, i->val));

	// Load encoding tables
	DBA_RUN_OR_GOTO(cleanup, bufrex_msg_load_tables(msg));

	// Pass new message ownership to caller
	*newmsg = msg;
	msg = NULL;

cleanup:
	if (msg != NULL) bufrex_msg_delete(msg);
	return err == DBA_OK ? dba_error_ok() : err;
}

static dba_err subset_to_msg(bufrex_msg dest, bufrex_msg orig, size_t subset_no)
{
	bufrex_subset sorig = orig->subsets[subset_no];
	bufrex_subset s;

	// Remove existing subsets
	bufrex_msg_reset_sections(dest);

	// Copy subset
	DBA_RUN_OR_RETURN(bufrex_msg_get_subset(dest, 0, &s));

	// Copy variables
	for (size_t i = 0; i < sorig->vars_count; ++i)
	{
		dba_var vorig = sorig->vars[i];
		DBA_RUN_OR_RETURN(bufrex_subset_store_variable_var(s, dba_var_code(vorig), vorig));
	}

	return dba_error_ok();
}

static int extract_rep_cod(dba_msg m)
{
	dba_var var;
	const char* rep_memo = NULL;
	if ((var = dba_msg_get_rep_memo_var(m)) != NULL)
		rep_memo = dba_var_value(var);
	else
		rep_memo = dba_msg_repmemo_from_type(m->type);

	// Convert rep_memo to rep_cod
	std::map<std::string, int>::const_iterator rc = to_rep_cod.find(rep_memo);
	if (rc == to_rep_cod.end())
		return 0;
	else
		return rc->second;
}

static void add_info_fixed(bufrex_msg newmsg, dba_msg m)
{
	uint16_t rep_cod;
	uint32_t lat;
	uint32_t lon;
	uint8_t block;
	uint16_t station;
	int ival;
	dba_var var;

	// rep_cod
	rep_cod = htons(extract_rep_cod(m));

	// Latitude
	var = dba_msg_get_latitude_var(m);
	if (var == NULL) throw wibble::exception::Consistency("creating fixed station info structure", "latitude not found");
	dballe::checked(dba_var_enqi(var, &ival));  // Get it unscaled
	lat = htonl(ival);

	// Longitude
	var = dba_msg_get_longitude_var(m);
	if (var == NULL) throw wibble::exception::Consistency("creating fixed station info structure", "longitude not found");
	dballe::checked(dba_var_enqi(var, &ival));  // Get it unscaled
	lon = htonl(ival);

	// Block number
	var = dba_msg_get_block_var(m);
	if (var == NULL)
		block = 0;
	else {
		dballe::checked(dba_var_enqi(var, &ival));
		block = ival;
	}

	// Station number
	var = dba_msg_get_station_var(m);
	if (var == NULL)
		station = 0;
	else {
		dballe::checked(dba_var_enqi(var, &ival));
		station = htons(ival);
	}

	char *buf = (char*)malloc(13);
	if (buf == NULL) throw wibble::exception::Consistency("creating buffer with metadata info", "allocation failed");
	memcpy(buf +  0, &rep_cod, 2);
	memcpy(buf +  2, &lat, 4);
	memcpy(buf +  6, &lon, 4);
	memcpy(buf + 10, &block, 1);
	memcpy(buf + 11, &station, 2);
	newmsg->opt.bufr.optional_section_length = 13;
	newmsg->opt.bufr.optional_section = buf;
}

static void add_info_mobile(bufrex_msg newmsg, dba_msg m)
{
//        - lat, lon, ident                  (per aerei)
// inline static dba_var dba_msg_get_latitude_var(dba_msg msg)
// inline static dba_var dba_msg_get_longitude_var(dba_msg msg)
// if (dba_msg_get_ident_var(m) != NULL)
// 
	uint16_t rep_cod;
	int ival;
	dba_var var;

	// rep_cod
	rep_cod = htons(extract_rep_cod(m));

	char *buf = (char*)calloc(1, 11);
	if (buf == NULL) throw wibble::exception::Consistency("creating buffer with metadata info", "allocation failed");
	memcpy(buf + 0, &rep_cod, 2);

	// Ident
	var = dba_msg_get_ident_var(m);
	if (var != NULL)
		strncpy(buf + 2, dba_var_value(var), 9);

	newmsg->opt.bufr.optional_section_length = 11;
	newmsg->opt.bufr.optional_section = buf;
}

static void add_info_generic(bufrex_msg newmsg, dba_msg m)
{
	// Check if there is "ident" and dispatch to fixed or mobile
	if (dba_msg_get_ident_var(m) != NULL)
		add_info_mobile(newmsg, m);
	else
		add_info_fixed(newmsg, m);
}

static void process(const std::string& filename, dba_file outfile)
{
	dba_rawmsg rmsg;
	bufrex_msg msg;
	dba_file file;
	dballe::checked(dba_rawmsg_create(&rmsg));
	dballe::checked(bufrex_msg_create(BUFREX_BUFR, &msg));
	dballe::checked(dba_file_create(BUFR, filename.c_str(), "r", &file));

	while (true)
	{
		bufrex_msg newmsg = NULL;

		int found;
		dballe::checked(dba_file_read(file, rmsg, &found));
		if (!found) break;

		// Decode message
		dballe::checked(bufrex_msg_decode(msg, rmsg));

		// Create new message with the same info as the old one
		dballe::checked(copy_base_msg(msg, &newmsg));

		// Loop over subsets
		for (size_t i = 0; i < msg->subsets_count; ++i)
		{
			// Create a bufrex_msg with the subset contents
			dballe::checked(subset_to_msg(newmsg, msg, i));

			// Parse into dba_msg
			dba_msgs msgs;
			dballe::checked(bufrex_msg_to_dba_msgs(newmsg, &msgs));
			dba_msg m = msgs->msgs[0];

			// Update reference time
			if (dba_var var = dba_msg_get_year_var(m)) dballe::checked(dba_var_enqi(var, &newmsg->rep_year));
			if (dba_var var = dba_msg_get_month_var(m)) dballe::checked(dba_var_enqi(var, &newmsg->rep_month));
			if (dba_var var = dba_msg_get_day_var(m)) dballe::checked(dba_var_enqi(var, &newmsg->rep_day));
			if (dba_var var = dba_msg_get_hour_var(m)) dballe::checked(dba_var_enqi(var, &newmsg->rep_hour));
			if (dba_var var = dba_msg_get_minute_var(m)) dballe::checked(dba_var_enqi(var, &newmsg->rep_minute));
			if (dba_var var = dba_msg_get_second_var(m)) dballe::checked(dba_var_enqi(var, &newmsg->rep_second));

			if (do_optional_section)
			{
				// Extract info and add optional section
				switch (m->type)
				{
					case MSG_GENERIC:
						add_info_generic(newmsg, m);
						break;
					case MSG_SYNOP:
					case MSG_PILOT:
					case MSG_TEMP:
					case MSG_BUOY:
					case MSG_METAR:
					case MSG_POLLUTION:
						add_info_fixed(newmsg, m);
						break;
					case MSG_TEMP_SHIP:
					case MSG_AIREP:
					case MSG_AMDAR:
					case MSG_ACARS:
					case MSG_SHIP:
					case MSG_SAT:
						add_info_mobile(newmsg, m);
						break;
				}
			}

			// Write out the message
			dba_rawmsg newrmsg;
			dballe::checked(bufrex_msg_encode(newmsg, &newrmsg));
			dballe::checked(dba_file_write(outfile, newrmsg));
			dba_rawmsg_delete(newrmsg);
		}
		bufrex_msg_delete(newmsg);
	}

	dba_file_delete(file);
}

int main(int argc, const char* argv[])
{
	wibble::commandline::Options opts;
	try {
		if (opts.parse(argc, argv))
			return 0;

		nag::init(opts.verbose->isSet(), opts.debug->isSet());

		do_optional_section = opts.annotate->boolValue();

		runtime::init();

		to_rep_cod = scan::Bufr::read_map_to_rep_cod();

		dba_file outfile;
		if (opts.outfile->isSet())
			dballe::checked(dba_file_create(BUFR, opts.outfile->stringValue().c_str(), "w", &outfile));
		else
			dballe::checked(dba_file_create(BUFR, "(stdout)", "w", &outfile));

		if (!opts.hasNext())
		{
			process("(stdin)", outfile);
		} else {
			while (opts.hasNext())
			{
				process(opts.next().c_str(), outfile);
			}
		}

		dba_file_delete(outfile);

		return 0;
	} catch (wibble::exception::BadOption& e) {
		cerr << e.desc() << endl;
		opts.outputHelp(cerr);
		return 1;
	} catch (std::exception& e) {
		cerr << e.what() << endl;
		return 1;
	}
}

// vim:set ts=4 sw=4:
