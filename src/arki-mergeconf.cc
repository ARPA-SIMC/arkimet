/*
 * arki-mergeconf - Merge arkimet dataset configurations
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
#include <arki/configfile.h>
#include <arki/dataset.h>
#include <arki/summary.h>
#include <arki/matcher.h>
#include <arki/runtime.h>
#include <wibble/commandline/parser.h>
#include "config.h"

#ifdef HAVE_GEOS
#include <memory>
#if GEOS_VERSION < 3
#include <geos/geom.h>

using namespace geos;

typedef DefaultCoordinateSequence CoordinateArraySequence;
#else
#include <geos/geom/Geometry.h>
#include <geos/geom/GeometryFactory.h>

using namespace geos::geom;
#endif
#endif


using namespace std;
using namespace arki;

namespace wibble {
namespace commandline {

struct Options : public StandardParserWithManpage
{
	StringOption* outfile;
	BoolOption* extra;

	Options() : StandardParserWithManpage("arki-mergeconf", PACKAGE_VERSION, 1, PACKAGE_BUGREPORT)
	{
		usage = "[options] [configfile(s) or directories]";
		description =
		    "Read dataset configuration from the given directories or config files, "
			" merge them and output the merged config file to standard output";

		outfile = add<StringOption>("output", 'o', "output", "file",
			"write the output to the given file instead of standard output");
		extra = add<BoolOption>("extra", 0, "extra", "",
			"extract extra information from the datasets (such as bounding box) "
			"and include it in the configuration");
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

		// Read the config files from the remaining commandline arguments
		ConfigFile cfg;
		bool foundConfig = false;
		while (opts.hasNext())
		{
			string file = opts.next();
			try {
				ReadonlyDataset::readConfig(file, cfg);
				foundConfig = true;
			} catch (std::exception& e) {
				cerr << file << " skipped: " << e.what() << endl;
			}
		}
		if (!foundConfig)
			throw wibble::exception::BadOption("you need to specify at least one valid config file or dataset directory");

		// Validate the configuration
		bool hasErrors = false;
		for (ConfigFile::const_section_iterator i = cfg.sectionBegin();
				i != cfg.sectionEnd(); ++i)
		{
			// Validate filters
			try {
				Matcher::parse(i->second->value("filter"));
			} catch (wibble::exception::Generic& e) {
				const ConfigFile::FilePos* fp = i->second->valueInfo("filter");
				if (fp)
					cerr << fp->pathname << ":" << fp->lineno << ":";
				cerr << e.what();
				cerr << endl;
				hasErrors = true;
			}
		}
		if (hasErrors)
		{
			cerr << "Some input files did not validate." << endl;
			return 1;
		}

		// If requested, compute extra information
		if (opts.extra->boolValue())
		{
			for (ConfigFile::section_iterator i = cfg.sectionBegin();
					i != cfg.sectionEnd(); ++i)
			{
				// Instantiate the dataset
				auto_ptr<ReadonlyDataset> d(ReadonlyDataset::create(*i->second));
				// Get the summary
				Summary sum;
				d->querySummary(Matcher(), sum);

#ifdef HAVE_GEOS
				// Compute bounding box, and store the WKT in bounding
				GeometryFactory gf;
				Item<types::BBox> bbox = sum.getConvexHull();
				auto_ptr<Geometry> geom(bbox->geometry(gf));
				if (geom.get())
					i->second->setValue("bounding", geom->toString());
#endif
			}
		}

		// Open the output file
		runtime::Output out(*opts.outfile);

		// Output the merged configuration
		cfg.output(out.stream(), out.name());

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
