/*
 * arki-gzip - Compress data files
 *
 * Copyright (C) 2010--2011  ARPA-SIM <urpsim@smr.arpa.emr.it>
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

#include "config.h"

#include <wibble/exception.h>
#include <wibble/commandline/parser.h>
#include <wibble/sys/fs.h>
#include <arki/scan/any.h>
#include <arki/runtime.h>

#include <fstream>
#include <iostream>
#include <cstring>
#include <cerrno>

#include <sys/types.h>
#include <sys/stat.h>
#include <fcntl.h>
#include <arpa/inet.h>

using namespace std;
using namespace arki;
using namespace wibble;

namespace wibble {
namespace commandline {

struct Options : public StandardParserWithManpage
{
	BoolOption* keep;

	Options() : StandardParserWithManpage("arki-gzip", PACKAGE_VERSION, 1, PACKAGE_BUGREPORT)
	{
		usage = "[options] [inputfile [inputfile...]]";
		description = "Compress the given data files";

		keep = add<BoolOption>("keep", 'k', "keep", "", "do not delete uncompressed file");
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

		runtime::init();

		while (opts.hasNext())
		{
			string fname = opts.next();
			scan::compress(fname);
			if (!opts.keep->boolValue())
				sys::fs::deleteIfExists(fname);
		}

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
