/*
 * arki-query - Query datasets using a matcher expression
 *
 * Copyright (C) 2007,2008,2009  ARPA-SIM <urpsim@smr.arpa.emr.it>
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
#include <arki/dataset.h>
#include <arki/dataset/merged.h>
#include <arki/utils.h>
#include <arki/nag.h>
#include <arki/runtime.h>

#include <fstream>
#include <iostream>

using namespace std;
using namespace arki;

namespace wibble {
namespace commandline {

struct Options : public arki::runtime::CommandLine
{
	Options() : runtime::CommandLine("arki-query", 1)
	{
		usage = "[options] [expression] [configfile or directory...]";
		description =
		    "Query the datasets in the given config file for data matching the"
			" given expression, and output the matching metadata.";

		addQueryOptions();
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

		opts.setupProcessing();

		if (opts.merged->boolValue())
		{
			dataset::Merged merger;
			size_t dscount = opts.inputInfo.sectionSize();
			
			// Create an auto_ptr array to take care of memory management
			auto_ptr<ReadonlyDataset> datasets[dscount];

			// Instantiate the datasets and add them to the merger
			int idx = 0;
			string names;
			for (ConfigFile::const_section_iterator i = opts.inputInfo.sectionBegin();
					i != opts.inputInfo.sectionEnd(); ++i, ++idx)
			{
				datasets[idx] = opts.openSource(*i->second);
				merger.addDataset(*datasets[idx]);
				if (names.empty())
					names = i->first;
				else
					names += ","+i->first;
			}

			// Perform the query
			opts.processSource(merger, names);

			for (size_t i = 0; i < dscount; ++i)
				opts.closeSource(datasets[i], true);
		} else {
			// Query all the datasets in sequence
			for (ConfigFile::const_section_iterator i = opts.inputInfo.sectionBegin();
					i != opts.inputInfo.sectionEnd(); ++i)
			{
				auto_ptr<ReadonlyDataset> ds = opts.openSource(*i->second);
				nag::verbose("Processing %s...", i->second->value("path").c_str());
				opts.processSource(*ds, i->second->value("path"));
				opts.closeSource(ds, true);
			}
		}

		opts.doneProcessing();

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
