/*
 * arki-dump - Dump a metadata file
 *
 * Copyright (C) 2007--2010  ARPA-SIM <urpsim@smr.arpa.emr.it>
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
#include <arki/dataset/http.h>
#include <arki/dataset/gridspace.h>
#include <arki/summary.h>
#include <arki/formatter.h>
#include <arki/utils/geosdef.h>
#include <arki/runtime.h>

#include <fstream>
#include <iostream>

#include "config.h"

using namespace std;
using namespace arki;
using namespace wibble;

namespace wibble {
namespace commandline {

struct Options : public StandardParserWithManpage
{
	BoolOption* reverse_data;
	BoolOption* reverse_summary;
	BoolOption* annotate;
	BoolOption* query;
	BoolOption* config;
	BoolOption* aliases;
	BoolOption* bbox;
	StringOption* outfile;
	StringOption* gridspace;

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

		query = add<BoolOption>("query", 0, "query", "",
			"print a query (specified on the command line) with the aliases expanded");

		config = add<BoolOption>("config", 0, "config", "",
			"print the arkimet configuration used to access the given file or dataset or URL");

		aliases = add<BoolOption>("aliases", 0, "aliases", "", "dump the alias database (to dump the aliases of a remote server, put the server URL on the command line)");

		gridspace = add<StringOption>("gridspace", 0, "gridspace", "file",
				"access the given input through a gridspace "
				"described by `file', printing information "
				"about the process.");

		bbox = add<BoolOption>("bbox", 0, "bbox", "", "dump the bounding box");

	}
};


}
}

// Add to \a s the info from all data read from \a in
static void addToSummary(runtime::Input& in, Summary& s)
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
			s.add(md);
		}
		else if (signature == "SU")
		{
			summary.read(buf, version, in.name());
			s.add(summary);
		}
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
		if (opts.query->boolValue() && opts.aliases->boolValue())
			throw wibble::exception::BadOption("--query conflicts with --aliases");
		if (opts.query->boolValue() && opts.config->boolValue())
			throw wibble::exception::BadOption("--query conflicts with --config");
		if (opts.query->boolValue() && opts.reverse_data->boolValue())
			throw wibble::exception::BadOption("--query conflicts with --from-yaml-data");
		if (opts.query->boolValue() && opts.reverse_summary->boolValue())
			throw wibble::exception::BadOption("--query conflicts with --from-yaml-summary");
		if (opts.query->boolValue() && opts.annotate->boolValue())
			throw wibble::exception::BadOption("--query conflicts with --annotate");
		if (opts.query->boolValue() && opts.gridspace->isSet())
			throw wibble::exception::BadOption("--query conflicts with --gridspace");
		if (opts.query->boolValue() && opts.bbox->isSet())
			throw wibble::exception::BadOption("--query conflicts with --bbox");

		if (opts.config->boolValue() && opts.aliases->boolValue())
			throw wibble::exception::BadOption("--config conflicts with --aliases");
		if (opts.config->boolValue() && opts.reverse_data->boolValue())
			throw wibble::exception::BadOption("--config conflicts with --from-yaml-data");
		if (opts.config->boolValue() && opts.reverse_summary->boolValue())
			throw wibble::exception::BadOption("--config conflicts with --from-yaml-summary");
		if (opts.config->boolValue() && opts.annotate->boolValue())
			throw wibble::exception::BadOption("--config conflicts with --annotate");
		if (opts.config->boolValue() && opts.gridspace->isSet())
			throw wibble::exception::BadOption("--config conflicts with --gridspace");
		if (opts.config->boolValue() && opts.bbox->isSet())
			throw wibble::exception::BadOption("--config conflicts with --bbox");

		if (opts.aliases->boolValue() && opts.reverse_data->boolValue())
			throw wibble::exception::BadOption("--aliases conflicts with --from-yaml-data");
		if (opts.aliases->boolValue() && opts.reverse_summary->boolValue())
			throw wibble::exception::BadOption("--aliases conflicts with --from-yaml-summary");
		if (opts.aliases->boolValue() && opts.annotate->boolValue())
			throw wibble::exception::BadOption("--aliases conflicts with --annotate");
		if (opts.aliases->boolValue() && opts.gridspace->isSet())
			throw wibble::exception::BadOption("--aliases conflicts with --gridspace");
		if (opts.aliases->boolValue() && opts.bbox->isSet())
			throw wibble::exception::BadOption("--aliases conflicts with --bbox");

		if (opts.reverse_data->boolValue() && opts.reverse_summary->boolValue())
			throw wibble::exception::BadOption("--from-yaml-data conflicts with --from-yaml-summary");
		if (opts.annotate->boolValue() && opts.reverse_data->boolValue())
			throw wibble::exception::BadOption("--annotate conflicts with --from-yaml-data");
		if (opts.annotate->boolValue() && opts.reverse_summary->boolValue())
			throw wibble::exception::BadOption("--annotate conflicts with --from-yaml-summary");
		if (opts.annotate->boolValue() && opts.gridspace->isSet())
			throw wibble::exception::BadOption("--annotate conflicts with --gridspace");
		if (opts.annotate->boolValue() && opts.bbox->isSet())
			throw wibble::exception::BadOption("--annotate conflicts with --bbox");

		if (opts.gridspace->boolValue() && opts.bbox->isSet())
			throw wibble::exception::BadOption("--gridspace conflicts with --bbox");
		
		if (opts.query->boolValue())
		{
			if (!opts.hasNext())
				throw wibble::exception::BadOption("--query wants the query on the command line");
			Matcher m = Matcher::parse(opts.next());
			cout << m.toStringExpanded() << endl;
			return 0;
		}
		
		if (opts.aliases->boolValue())
		{
			ConfigFile cfg;
			if (opts.hasNext())
			{
				dataset::HTTP::getAliasDatabase(opts.next(), cfg);
			} else {
				MatcherAliasDatabase::serialise(cfg);
			}
			
			// Open the output file
			runtime::Output out(*opts.outfile);

			// Output the merged configuration
			cfg.output(out.stream(), out.name());

			return 0;
		}

		if (opts.config->boolValue())
		{
			ConfigFile cfg;
			while (opts.hasNext())
			{
				ReadonlyDataset::readConfig(opts.next(), cfg);
			}
			
			// Open the output file
			runtime::Output out(*opts.outfile);

			// Output the merged configuration
			cfg.output(out.stream(), out.name());

			return 0;
		}

		if (opts.gridspace->isSet())
		{
			// Access the data source
			ConfigFile cfg;
			ReadonlyDataset::readConfig(opts.next(), cfg);
			auto_ptr<ReadonlyDataset> ds(ReadonlyDataset::create(*cfg.sectionBegin()->second));

			// Wrap it with the gridspace
			runtime::Input in(opts.gridspace->stringValue());
			dataset::Gridspace gs(*ds);
			gs.read(in.stream(), in.name());

			// Open the output file
			runtime::Output out(*opts.outfile);

			ostream& o = out.stream();

			// Dump the state before validation
			o << "Before validation:" << endl;
			gs.dump(o, " ");

			// Dump how matchers are expanded
			o << "Matcher expansions:" << endl;
			gs.dumpExpands(o, " ");

			// Expand matchers into the soup
			gs.validateMatchers();

			// Dump how many values per item in the soup
			o << "Data count per item:" << endl;
			gs.dumpCountPerItem(o, " ");

			// Finally perform validation
			gs.validate();

			// Dump the state after validation
			o << "After validation:" << endl;
			gs.dump(o, " ");

			return 0;
		}

		if (opts.bbox->boolValue())
		{
			// Open the input file
			runtime::Input in(opts);

			// Read everything into a single summary
			Summary summary;
			addToSummary(in, summary);

			// Get the bounding box
			ARKI_GEOS_GEOMETRYFACTORY gf;
			std::auto_ptr<ARKI_GEOS_GEOMETRY> hull = summary.getConvexHull(gf);

			// Open the output file
			runtime::Output out(*opts.outfile);

			// Print it out
			if (hull.get())
				out.stream() << hull->toString() << endl;
			else
				out.stream() << "no bounding box could be computed." << endl;

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
					out.stream() << endl;
				}
				else if (signature == "SU")
				{
					summary.read(buf, version, in.name());
					summary.writeYaml(out.stream(), formatter);
					out.stream() << endl;
				}
			}
// Uncomment as a quick hack to check memory usage at this point:
//system(str::fmtf("ps u %d >&2", getpid()).c_str());
//types::debug_intern_stats();

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
