/*
 * arki-dump - Dump a metadata file
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
 * You should have received a copy of the GNU General Public License
 * along with this program; if not, write to the Free Software
 * Foundation, Inc., 51 Franklin St, Fifth Floor, Boston, MA  02110-1301 USA
 *
 * Author: Enrico Zini <enrico@enricozini.com>
 */

#include <wibble/exception.h>
#include <wibble/commandline/parser.h>
#include <arki/metadata.h>
#include <arki/summary.h>
#include <arki/formatter.h>
#include <arki/runtime.h>

#include <fstream>
#include <iostream>

#include "config.h"

using namespace std;
using namespace arki;

namespace wibble {
namespace commandline {

struct Options : public StandardParserWithManpage
{
	BoolOption* reverse_data;
	BoolOption* reverse_summary;
	BoolOption* annotate;
	BoolOption* query;
	StringOption* outfile;

	Options() : StandardParserWithManpage("arki-dump", PACKAGE_VERSION, 1, PACKAGE_BUGREPORT)
	{
		usage = "[options] [input]";
		description =
			"Read data from the given input file (or stdin), and dump them"
			" in human readable format on stdout.";

		outfile = add<StringOption>("output", 'o', "output", "file",
				"write the output to the given file instead of standard output");
		annotate = add<BoolOption>("annotate", 0, "annotate", "",
				"annotate the human-readable Yaml output with field descriptions");

		reverse_data = add<BoolOption>("from-yaml-data", 0, "from-yaml-data", "",
			"read a Yaml data dump and write binary metadata");

		reverse_summary = add<BoolOption>("from-yaml-summary", 0, "from-yaml-summary", "",
			"read a Yaml summary dump and write a binary summary");

		query = add<BoolOption>("quuery", 0, "query", "",
			"print a query (specified on the command line) with the aliases expanded");
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

		// Validate command line options
		if (opts.query->boolValue() && opts.reverse_data->boolValue())
			throw wibble::exception::BadOption("--query conflicts with --from-yaml-data");
		if (opts.query->boolValue() && opts.reverse_summary->boolValue())
			throw wibble::exception::BadOption("--query conflicts with --from-yaml-summary");
		if (opts.query->boolValue() && opts.annotate->boolValue())
			throw wibble::exception::BadOption("--query conflicts with --annotate");
		if (opts.reverse_data->boolValue() && opts.reverse_summary->boolValue())
			throw wibble::exception::BadOption("--from-yaml-data conflicts with --from-yaml-summary");
		if (opts.annotate->boolValue() && opts.reverse_data->boolValue())
			throw wibble::exception::BadOption("--annotate conflicts with --from-yaml-data");
		if (opts.annotate->boolValue() && opts.reverse_summary->boolValue())
			throw wibble::exception::BadOption("--annotate conflicts with --from-yaml-summary");
		
		if (opts.query->boolValue())
		{
			if (!opts.hasNext())
				throw wibble::exception::BadOption("--query wants the query on the command line");
			Matcher m = Matcher::parse(opts.next());
			cout << m.toStringExpanded() << endl;
			return 0;
		}

		// Open the input file
		runtime::Input in(opts);

		// Open the output channel
		runtime::Output out(*opts.outfile);

		if (opts.reverse_data->boolValue())
		{
			Metadata md;
			while (md.readYaml(in.stream(), in.name()))
				md.write(out.stream(), out.name());
		}
		else if (opts.reverse_summary->boolValue())
		{
			Summary summary;
			while (summary.readYaml(in.stream(), in.name()))
				summary.write(out.stream(), out.name());
		}
		else
		{
			Formatter* formatter = 0;
			if (opts.annotate->boolValue())
				formatter = Formatter::create();

			Metadata md;
			Summary summary;

			wibble::sys::Buffer buf;
			string signature;
			unsigned version;

			while (types::readBundle(in.stream(), in.name(), buf, signature, version))
			{
				if (signature == "MD" || signature == "!D")
				{
					md.read(buf, version, in.name());
					if (md.source->style() == types::Source::INLINE)
						md.readInlineData(in.stream(), in.name());
					md.writeYaml(out.stream(), formatter);
				}
				else if (signature == "SU")
				{
					summary.read(buf, version, in.name());
					summary.writeYaml(out.stream(), formatter);
				}
			}
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
