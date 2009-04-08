/*
 * arki-grep - Filter arkimet metadata
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
 * You should have received a copy of the GNU General Public License
 * along with this program; if not, write to the Free Software
 * Foundation, Inc., 51 Franklin St, Fifth Floor, Boston, MA  02110-1301 USA
 *
 * Author: Enrico Zini <enrico@enricozini.com>
 */

#include <wibble/exception.h>
#include <wibble/commandline/parser.h>
#include <arki/metadata.h>
#include <arki/matcher.h>
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
	StringOption* exprfile;
	BoolOption* invert;
	BoolOption* count;
	BoolOption* quiet;
	BoolOption* verbose;

	Options() : runtime::OutputOptions("arki-grep")
	{
		usage = "[options] [expression] [input]";
		description =
			"Read metadata from the given input file (or stdin), filter it"
			" according to a arkimet metadata expression and write it to the"
			" given output file (or stdout).";

		exprfile = add<StringOption>("file", 'f', "file", "file",
			"read the expression from the given file");
		invert = add<BoolOption>("invert", 'v', "invert", "",
			"only output the metadata that do not match the filter");
		count = add<BoolOption>("count", 'c', "count", "",
			"only output the number of metadata items that match the filter");
		quiet = add<BoolOption>("quiet", 'q', "quiet", "",
			"do not write anything to standard output, but just exit with error if no"
			" matching metadata was found");
		verbose = add<BoolOption>("verbose", 0, "verbose", "",
			"verbose output, useful to see how Arkimet has parsed a complex "
			"reftime query");
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

		runtime::readMatcherAliasDatabase();

		// Instantiate the filter
		Matcher expr;
		runtime::readQuery(expr, opts, opts.exprfile);

		if (opts.verbose->boolValue())
		{
			cerr << "Normalised expression:" << endl;
			string val = expr.toString();
			for (string::iterator i = val.begin(); i != val.end(); ++i)
				if (*i == ';') *i = '\n';
			cerr << " " << val << endl;
		}

		// Read all messages from stdin, filter them and output them
		unsigned count = 0;
		bool invert = opts.invert->boolValue();
		bool count_only = opts.count->boolValue();
		bool res_only = opts.quiet->boolValue();

		while (opts.hasNext())
		{
			// Open the input file
			runtime::Input in(opts.next());

			Metadata md;
			while (md.read(in.stream(), in.name()))
			{
				// Write it to mdoutput
				if (invert ? !expr(md) : expr(md))
				{
					if (!count_only && !res_only)
						opts.consumer()(md);
					++count;
					if (res_only)
						break;
				}
			}
		}

		opts.consumer().flush();

		if (count_only && !res_only)
			opts.output().stream() << count << endl;

		return count > 0 ? 0 : 1;
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
