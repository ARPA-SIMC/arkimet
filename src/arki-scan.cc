/*
 * arki-scan - Scan files for metadata and import them into datasets.
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
 * You should have received a copy of the GNU General Public License along
 * with this program; if not, write to the Free Software Foundation, Inc.,
 * 51 Franklin Street, Fifth Floor, Boston, MA 02110-1301 USA.
 *
 * Author: Enrico Zini <enrico@enricozini.com>
 */

#include <wibble/exception.h>
#include <wibble/string.h>

#include <arki/metadata.h>
#include <arki/matcher.h>
#include <arki/dataset.h>
#include <arki/dataset/file.h>
#include <arki/runtime.h>

#include <memory>
#include <iostream>
#include <cstdio>

using namespace std;
using namespace arki;
using namespace wibble;

namespace wibble {
namespace commandline {

struct Options : public runtime::CommandLine
{
	Options() : runtime::CommandLine("arki-scan")
	{
		usage = "[options] [input...]";
		description =
			"Read one or more files or datasets and process their data "
			"or import them in a dataset.";
		addScanOptions();
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

		bool all_successful = true;
		for (ConfigFile::const_section_iterator i = opts.inputInfo.sectionBegin();
				i != opts.inputInfo.sectionEnd(); ++i)
		{
			auto_ptr<ReadonlyDataset> ds = opts.openSource(*i->second);

			bool success = true;
			try {
				success = opts.processSource(*ds, i->second->value("path"));
			} catch (std::exception& e) {
				// FIXME: this is a quick experiment: a better message can
				// print some of the stats to document partial imports
				cerr << i->second->value("path") << " failed: " << e.what() << endl;
				success = false;
			}

			opts.closeSource(ds, success);

			// Take note if something went wrong
			if (!success) all_successful = false;
		}

		opts.doneProcessing();

		if (all_successful)
			return 0;
		else
			return 2;
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
