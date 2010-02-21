/*
 * arki-bufr-scan - Scan BUFR messages and encode summary information in their
 *                  optional section
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
#if 0
#include <arki/metadata.h>
#include <arki/types/reftime.h>
#include <arki/scan/any.h>

#include <wibble/exception.h>
#include <wibble/commandline/parser.h>
#include <wibble/string.h>
#include <wibble/sys/fs.h>
#include <wibble/sys/exec.h>
#endif

#include <dballe/core/rawmsg.h>
#include <dballe/core/file.h>
#include <dballe/bufrex/msg.h>
#include <dballe++/error.h>

#if 0
#include <cstdlib>
#include <cstring>
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

	Options() : StandardParserWithManpage("arki-bufr-scan", PACKAGE_VERSION, 1, PACKAGE_BUGREPORT)
	{
		usage = "[options] file1 file2...";
		description =
			"Read BUFR messages, encode each subsection in a "
			"separate message and add an optional section with "
			"information useful for arki-scan";

		debug = add<BoolOption>("debug", 0, "debug", "", "debug output");
		verbose = add<BoolOption>("verbose", 0, "verbose", "", "verbose output");
		outfile = add<StringOption>("output", 'o', "output", "file",
				"write the output to the given file instead of standard output");
	}
};

}
}

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

			// TODO: parse into bufrex_msg
			// TODO: extract info
			// TODO: add optional section

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

		runtime::init();

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
