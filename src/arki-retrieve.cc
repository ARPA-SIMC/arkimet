/*
 * arki-retrieve - Retrieve a dataset given a cartesian product of metadata,
 *                 in a way similar to mars and xgrib
 *
 * Copyright (C) 2010  ARPA-SIM <urpsim@smr.arpa.emr.it>
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
#include <wibble/string.h>
#include <arki/metadata.h>
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
	StringOption* outfile;

	Options() : StandardParserWithManpage("arki-retrieve", PACKAGE_VERSION, 1, PACKAGE_BUGREPORT)
	{
		usage = "[options] dataset|datafile";
		description =
 			"Retrieve a dataset given a cartesian product of metadata,"
 			"in a way similar to mars and xgrib";
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

		if (!opts.hasNext())
			throw wibble::exception::BadOption("you need to provide a dataset or data file name");

		// Read the metadata soup
		string file = "-";
		runtime::Input in(file);
		std::istream& input = in.stream();

		std::map<types::Code, std::set< Item<> > > soup;

		string line;
		while (!input.eof())
		{
			getline(input, line);
			if (input.fail() && !input && !input.eof())
				throw wibble::exception::File(file, "reading one line");
			line = str::trim(line);
			if (line.empty())
				continue;
			size_t pos = line.find(':');
			if (pos == string::npos)
			{
				throw wibble::exception::Consistency("parsing file " + in.name(),
						"cannot parse line \"" + line + "\"");
			}
			types::Code code = types::parseCodeName(line.substr(0, pos));
			Item<> item = decodeString(code, str::trim(line.substr(pos+1)));
			soup[code].insert(item);
		}

                for (std::map<types::Code, std::set< Item<> > >::const_iterator i = soup.begin();
				i != soup.end(); ++i)
		{
			cerr << types::tag(i->first) << ":" << endl;
			for (std::set< Item<> >::const_iterator j = i->second.begin();
					j != i->second.end(); ++j)
			{
				cerr << "  " << *j << endl;
			}
		}

		// Open the dataset
		ConfigFile cfg;
		ReadonlyDataset::readConfig(opts.next(), cfg);
		auto_ptr<ReadonlyDataset> ds(ReadonlyDataset::create(cfg));

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
