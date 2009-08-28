/*
 * arki-scan - Scan files for metadata and import them into datasets.
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
#include <wibble/string.h>

#include <arki/metadata.h>
#include <arki/matcher.h>
#include <arki/dataset.h>
#include <arki/dataset/file.h>
#include <arki/runtime.h>

#include <memory>
#include <iostream>
#include <cstdio>

#include "config.h"

#if HAVE_DBALLE
#include <dballe++/init.h>
#endif

using namespace std;
using namespace arki;
using namespace wibble;

namespace wibble {
namespace commandline {

struct Options : public runtime::ScanOptions
{
	commandline::StringOption* moveok;
	commandline::StringOption* moveko;
	commandline::StringOption* movework;

	Options() : runtime::ScanOptions("arki-scan")
	{
		usage = "[options] [input...]";
		description =
			"Read one or more files or datasets and output the metadata that "
			"describe them, or import them in a dataset.";

		moveok = add<StringOption>("moveok", 0, "moveok", "directory",
				"move input files imported successfully to the given directory");
		moveko = add<StringOption>("moveko", 0, "moveko", "directory",
				"move input files with problems to the given directory");
		movework = add<StringOption>("movework", 0, "movework", "directory",
				"move input files here before opening them. This is useful to "
				"catch the cases where arki-scan crashes without having a "
				"chance to handle errors.");
	}
};

}
}

std::string moveFile(const std::string& source, const std::string& targetdir)
{
	string targetFile = str::joinpath(targetdir, str::basename(source));
	if (rename(source.c_str(), targetFile.c_str()) == -1)
		throw wibble::exception::System("Moving " + source + " to " + targetFile);
	return targetFile;
}

std::string moveFile(const ReadonlyDataset& ds, const std::string& targetdir)
{
	if (const dataset::File* d = dynamic_cast<const dataset::File*>(&ds))
		return moveFile(d->pathname(), targetdir);
	else
		return string();
}

int main(int argc, const char* argv[])
{
	wibble::commandline::Options opts;
	try {
		if (opts.parse(argc, argv))
			return 0;

		bool scanInline = opts.dataInline->boolValue() || opts.dispatch->isSet();

#if HAVE_DBALLE
		dballe::DballeInit dballeInit;
#endif

		// Get the list of files to process
		ConfigFile cfg;
		while (opts.hasNext())
			ReadonlyDataset::readConfig(opts.next(), cfg);

		// Query/scan all the "datasets"
		for (ConfigFile::const_section_iterator i = cfg.sectionBegin();
				i != cfg.sectionEnd(); ++i)
		{
			if (opts.movework->isSet() && i->second->value("type") == "file")
				i->second->setValue("path", moveFile(i->second->value("path"), opts.movework->stringValue()));
			auto_ptr<ReadonlyDataset> ds(ReadonlyDataset::create(*i->second));
			try {
				if (opts.mdispatch)
					opts.mdispatch->setStartTime();

				dataset::DataQuery dq(Matcher(), scanInline);
				ds->queryData(dq, opts.consumer());

				if (opts.mdispatch)
				{
				   	if (opts.verbose->boolValue())
						cerr << i->second->value("path") << ": " << opts.mdispatch->summarySoFar() << endl;

					if (opts.moveok->isSet())
					{
						if (!opts.mdispatch->countNotImported
						 && !opts.mdispatch->countDuplicates
						 && !opts.mdispatch->countInErrorDataset)
							moveFile(*ds, opts.moveok->stringValue());
					}
					if (opts.moveko->isSet())
					{
						if (opts.mdispatch->countNotImported
						 || opts.mdispatch->countDuplicates
						 || opts.mdispatch->countInErrorDataset)
							moveFile(*ds, opts.moveko->stringValue());
					}

					opts.mdispatch->flush();

					opts.mdispatch->countSuccessful = 0;
					opts.mdispatch->countNotImported = 0;
					opts.mdispatch->countDuplicates = 0;
					opts.mdispatch->countInErrorDataset = 0;
				}
			} catch (std::exception& e) {
				// FIXME: this is a quick experiment: a better message can
				// print some of the stats to document partial imports
				cerr << i->second->value("path") << ": import FAILED: " << e.what() << endl;
				//if (opts.moveko->isSet())
					//moveFile(*ds, opts.moveko->stringValue());
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
