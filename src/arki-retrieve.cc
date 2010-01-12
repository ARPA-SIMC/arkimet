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
#include <algorithm>

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

/**
 * Define a linear metadata space.
 *
 * For every metadata type in the soup, we have a number of elements we want.
 *
 * Every metadata code in the soup defines a dimension of a matrix. The number
 * of items requested for that metadata code defines the number of elements of
 * the matrix across that dimension.
 *
 * Since the metadata codes are sorted, and the items are sorted, and the soup
 * (that is, the topology of the matrix) will not change, we can define an
 * unchanging bijective function between the matrix elements and a segment in
 * N. This means that we can associate to each matrix element an integer number
 * between 0 and the number of items in the matrix.
 *
 * What is required at the end of the game is to have one and only one metadata
 * item per matrix element.
 */
struct MDGrid
{
	std::map<types::Code, std::vector< Item<> > > soup;
	std::vector<size_t> dim_sizes;
	size_t maxidx;

	// Find the linearised matrix index for md. Returns -1 if md does not
	// match a point in the matrix
	int index(const Metadata& md) const
	{
		int res = 0;
		// Consolidate the soup space: remove duplicates, sort the vectors
		size_t dim = 0;
                for (std::map<types::Code, std::vector< Item<> > >::const_iterator i = soup.begin();
				i != soup.end(); ++i, ++dim)
		{
			UItem<> mdi = md.get(i->first);
			if (!mdi.defined())
				return -1;
			vector< Item<> >::const_iterator lb =
				lower_bound(i->second.begin(), i->second.end(), mdi);
			if (*lb != mdi)
				return -1;
			int idx = lb - i->second.begin();
			res += idx * dim_sizes[dim];
		}
		return res;
	}

	std::vector< Item<> > expand(size_t index) const
	{
		vector< Item<> > res;
		size_t dim = 0;
                for (std::map<types::Code, std::vector< Item<> > >::const_iterator i = soup.begin();
				i != soup.end(); ++i, ++dim)
		{
			size_t idx = index / dim_sizes[dim];
			res.push_back(i->second[idx]);
			index = index % dim_sizes[dim];
		}
		return res;
	}

	std::string make_query() const
	{
		vector<string> ands;
                for (std::map<types::Code, std::vector< Item<> > >::const_iterator i = soup.begin();
				i != soup.end(); ++i)
		{
			vector<string> ors;
			for (std::vector< Item<> >::const_iterator j = i->second.begin();
					j != i->second.end(); ++j)
				ors.push_back((*j)->exactQuery());
			ands.push_back(str::tolower(types::formatCode(i->first)) + ":" +
					str::join(ors.begin(), ors.end(), " or "));
		}
		return str::join(ands.begin(), ands.end(), "; ");
	}

	void read(std::istream& input, const std::string& fname)
	{
		string line;
		while (!input.eof())
		{
			getline(input, line);
			if (input.fail() && !input && !input.eof())
				throw wibble::exception::File(fname, "reading one line");
			line = str::trim(line);
			if (line.empty())
				continue;
			size_t pos = line.find(':');
			if (pos == string::npos)
			{
				throw wibble::exception::Consistency("parsing file " + fname,
						"cannot parse line \"" + line + "\"");
			}
			types::Code code = types::parseCodeName(line.substr(0, pos));
			Item<> item = decodeString(code, str::trim(line.substr(pos+1)));
			soup[code].push_back(item);
		}

		// Consolidate the soup space: remove duplicates, sort the vectors
		maxidx = soup.empty() ? 0 : 1;
                for (std::map<types::Code, std::vector< Item<> > >::iterator i = soup.begin();
				i != soup.end(); ++i)
		{
			// Copy to a set then back to the vector
			set< Item<> > s;
			std::copy(i->second.begin(), i->second.end(), inserter(s, s.begin()));
			i->second.clear();
			std::copy(s.begin(), s.end(), back_inserter(i->second));

			// Update the number of matrix elements below every dimension
			for (vector<size_t>::iterator j = dim_sizes.begin();
					j != dim_sizes.end(); ++j)
				*j *= i->second.size();
			dim_sizes.push_back(1);
			maxidx *= i->second.size();
		}
	}
};

int main(int argc, const char* argv[])
{
	wibble::commandline::Options opts;
	try {
		if (opts.parse(argc, argv))
			return 0;

		runtime::init();

		if (!opts.hasNext())
			throw wibble::exception::BadOption("you need to provide a dataset or data file name");

		MDGrid mdgrid;

		// Read the metadata soup
		string file = "-";
		runtime::Input in(file);
		mdgrid.read(in.stream(), in.name());

		cerr << "Metadata soup:" << endl;
                for (std::map<types::Code, std::vector< Item<> > >::const_iterator i = mdgrid.soup.begin();
				i != mdgrid.soup.end(); ++i)
		{
			cerr << " " << types::tag(i->first) << ":" << endl;
			for (std::vector< Item<> >::const_iterator j = i->second.begin();
					j != i->second.end(); ++j)
			{
				cerr << "  " << *j << endl;
			}
		}

		cerr << "Number of combinations: " << mdgrid.maxidx << endl;

		for (size_t i = 0; i < mdgrid.maxidx; ++i)
		{
			cerr << " Combination " << i << ":" << endl;
			vector< Item<> > c = mdgrid.expand(i);
			for (vector< Item<> >::const_iterator j = c.begin();
					j != c.end(); ++j)
			{
				cerr << "  " << *j << endl;
			}
		}

		cerr << "Arkimet query: " << mdgrid.make_query() << endl;

		// Open the dataset
		ConfigFile cfg;
		ReadonlyDataset::readConfig(opts.next(), cfg);
		auto_ptr<ReadonlyDataset> ds(ReadonlyDataset::create(cfg));

		// Read the metadata and fit in a vector sized with maxidx
		
		// Check that elements of the vector are there

		// Output from the vector

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
