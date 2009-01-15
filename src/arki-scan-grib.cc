/*
 * arki-scan-grib - Scan a GRIB (edition 1 or 2) file for metadata.
 *
 * Copyright (C) 2007,2008  ARPA-SIM <urpsim@smr.arpa.emr.it>
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

#include <wibble/exception.h>
#include <memory>
#include <iostream>

#include <arki/scan/grib.h>
#include <arki/metadata.h>
#include <arki/runtime.h>

#include <config.h>

using namespace std;
using namespace arki;

namespace wibble {
namespace commandline {

struct Options : public runtime::ScanOptions
{
	StringOption* tmpfile;

	Options() : ScanOptions("arki-scan-grib")
	{
		usage = "[options] [input...]";
		description =
			"Read one or more files containing messages in GRIB (edition 1 or 2) format,"
			" optionally write them to a given temporary file to recompose"
			" the data contained in a multi-grib file, and output the metadata"
			" that describe them.";

		tmpfile = add<StringOption>("tmpfile", 't', "tmpfile", "file",
			"name of the temporary file to use to resassemble multigrib data");
	}
};

}
}

int main(int argc, const char* argv[])
{
	wibble::commandline::Options opts;
	try {
		if (opts.parse(argc, argv))
			return 0;

		bool scanInline = opts.dataInline->boolValue() || opts.dispatch->isSet();

		// Create the right scanner
		auto_ptr<scan::Grib> scanner;
		if (opts.tmpfile->isSet())
		{
			string tmpfilename = opts.tmpfile->stringValue();
			ofstream tmpfile;
			tmpfile.open(tmpfilename.c_str(), ios::out | ios::app | ios::binary);
			if (!tmpfile.is_open() || !tmpfile.good())
				throw wibble::exception::File(tmpfilename, "opening file for appending data");
			scanner.reset(new scan::MultiGrib(tmpfilename, tmpfile));
		} else
			scanner.reset(new scan::Grib(scanInline));

		// Scan all the files in input
		Metadata md;
		while (opts.hasNext())
		{
			string filename = opts.next();
			scanner->open(filename);
			while (scanner->next(md))
				opts.consumer()(md);

			if (opts.mdispatch && opts.verbose->boolValue())
			{
				cerr << filename << ": " << opts.mdispatch->summarySoFar() << endl;

				opts.mdispatch->countSuccessful = 0;
				opts.mdispatch->countNotImported = 0;
				opts.mdispatch->countDuplicates = 0;
				opts.mdispatch->countInErrorDataset = 0;
			}
		}

		opts.flush();

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
