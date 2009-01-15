/*
 * arki-dispatch - Dispatch data into datasets.
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
#include <wibble/commandline/parser.h>
#include <arki/configfile.h>
#include <arki/metadata.h>
#include <arki/matcher.h>
#include <arki/dataset.h>
#include <arki/utils.h>
#include <arki/runtime.h>

#include <fstream>
#include <iostream>

#include "config.h"

using namespace std;
using namespace arki;

namespace wibble {
namespace commandline {

struct Options : public runtime::OutputOptions
{
	VectorOption<String>* cfgfiles;
	StringOption* inputdir;

	Options() : runtime::OutputOptions("arki-dispatch")
	{
		usage = "[options] --config=configfile [inputmd]";
		description =
		    "Read metadata from standard input or from inputmd, dispatch it according"
			" to the contents of the config file and output the dispatched metadata"
			" to standard output.";

		inputdir = add<StringOption>("inputdir", 'i', "inputdir", "dir",
			"directory with the input data files referenced by the metadata"
			" (defaults to the current directory)");
		cfgfiles = add< VectorOption<String> >("config", 'C', "config", "file",
			"read the configuration from the given file (can be given more than once)");
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

		string inputdir(".");
		if (opts.inputdir->isSet())
			inputdir = opts.inputdir->stringValue();

		runtime::readMatcherAliasDatabase();

		// Read the config file
		ConfigFile cfg;
		if (!runtime::parseConfigFiles(cfg, *opts.cfgfiles))
			throw wibble::exception::BadOption("you need to specify the config file");

		// Create the dispatcher
		runtime::MetadataDispatch dispatcher(cfg, opts.consumer());

		// Open the input file(s)
		runtime::Input in(opts);

		Metadata md;
		while (md.read(in.stream(), in.name()))
			dispatcher(md);

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
