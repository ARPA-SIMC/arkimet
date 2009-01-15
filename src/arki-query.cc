/*
 * arki-query - Query datasets using a matcher expression
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
#include <arki/summary.h>
#include <arki/matcher.h>
#include <arki/dataset.h>
#include <arki/dataset/merged.h>
#include <arki/utils.h>
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
	StringOption* exprfile;
	BoolOption* merged;
	Matcher expr;

	Options() : runtime::OutputOptions("arki-query", 1, runtime::OutputOptions::PARMS)
	{
		usage = "[options] [expression] [configfile or directory...]";
		description =
		    "Query the datasets in the given config file for data matching the"
			" given expression, and output the matching metadata.";

		exprfile = add<StringOption>("file", 'f', "file", "file",
			"read the expression from the given file");
		merged = add<BoolOption>("merged", 0, "merged", "",
			"if multiple datasets are given, merge their data and output it in"
			" reference time order.  Note: sorting does not work when using"
			" --postprocess, --data or --report");
	}

	void parseLeadingParameters()
	{
		runtime::readMatcherAliasDatabase();

		// Instantiate the filter
		runtime::readQuery(expr, *this, exprfile);
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

		if (opts.postprocess->boolValue() && opts.cfg.sectionSize() > 1)
			throw wibble::exception::BadOption("postprocessing is not possible when querying more than one dataset at the same time");
		if (opts.report->boolValue() && opts.cfg.sectionSize() > 1)
			throw wibble::exception::BadOption("reports are not possible when querying more than one dataset at the same time");
		while (opts.hasNext())
			ReadonlyDataset::readConfig(opts.next(), opts.cfg);

		if (opts.merged->boolValue())
		{
			dataset::Merged merger;
			size_t dscount = 0;

			// Count all the datasets
			for (ConfigFile::const_section_iterator i = opts.cfg.sectionBegin();
					i != opts.cfg.sectionEnd(); ++i)
				++dscount;
			
			// Create an auto_ptr array to take care of memory management
			auto_ptr<ReadonlyDataset> datasets[dscount];

			// Instantiate the datasets and add them to the merger
			int idx = 0;
			for (ConfigFile::const_section_iterator i = opts.cfg.sectionBegin();
					i != opts.cfg.sectionEnd(); ++i, ++idx)
			{
				datasets[idx].reset(ReadonlyDataset::create(*i->second));
				merger.addDataset(*datasets[idx]);
			}

			// Perform the query
			opts.processDataset(merger, opts.expr);
		} else {
			// Query all the datasets in sequence
			for (ConfigFile::const_section_iterator i = opts.cfg.sectionBegin();
					i != opts.cfg.sectionEnd(); ++i)
			{
				auto_ptr<ReadonlyDataset> ds(ReadonlyDataset::create(*i->second));
				opts.processDataset(*ds, opts.expr);
			}
		}

		return 0;
		//return summary.count() > 0 ? 0 : 1;
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
