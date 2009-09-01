/*
 * arki-dump - Dump a metadata file
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

struct Options : public runtime::OutputOptions
{
	BoolOption* reverse;

	Options() : runtime::OutputOptions("arki-dump")
	{
		usage = "[options] [input]";
		description =
			"Read metadata from the given input file (or stdin), and dump their content"
			" in human readable format on stdout.";

		reverse = add<BoolOption>("reverse", 'r', "reverse", "",
			"read a Yaml dump and write binary metadata");
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

		// Open the input file
		runtime::Input in(opts);

		if (!opts.reverse->boolValue())
		{
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
					opts.consumer()(md);
				}
				else if (signature == "SU")
				{
					summary.read(buf, version, in.name());
					opts.consumer().outputSummary(summary);
				}
			}
		} else {
			if (opts.summary->boolValue())
			{
				Summary summary;
				while (summary.readYaml(in.stream(), in.name()))
					opts.consumer().outputSummary(summary);
			} else {
				Metadata md;
				while (md.readYaml(in.stream(), in.name()))
					opts.consumer()(md);
			}
		}
		opts.consumer().flush();

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
