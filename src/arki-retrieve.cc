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
#include <arki/summary.h>
#include <arki/dataset.h>
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

struct UnresolvedMatcher : public std::string
{
	std::set< Item<> > candidates;
	Matcher matcher;

	UnresolvedMatcher(types::Code code, const std::string& expr)
		: std::string(expr), matcher(Matcher::parse(types::tag(code) + ":" + expr)) {}
};

struct MatcherResolver : public summary::Visitor
{
	std::map<types::Code, std::vector<UnresolvedMatcher> >& matchers;

	MatcherResolver(std::map<types::Code, std::vector<UnresolvedMatcher> >& matchers)
		: matchers(matchers) {}

	virtual bool operator()(const std::vector< UItem<> >& md, const Item<summary::Stats>& stats)
	{
		for (map<types::Code, vector<UnresolvedMatcher> >::iterator i = matchers.begin();
				i != matchers.end(); ++i)
		{
			int pos = posForCode(i->first);
			if (pos == -1)
				throw wibble::exception::Consistency(
						"resolving matcher for " + types::tag(i->first),
						"metadata item not tracked by summaries");

			if (!md[pos].defined()) continue;

			for (vector<UnresolvedMatcher>::iterator j = i->second.begin();
					j != i->second.end(); ++j)
			{
				if (j->candidates.find(md[pos]) != j->candidates.end()) continue;
				if (j->matcher(md[pos]))
				{
					j->candidates.insert(md[pos]);
				}
			}
		}
		return true;
	}
};

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
	std::map<types::Code, std::vector<UnresolvedMatcher> > matchers;
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
			map< types::Code, vector<UnresolvedMatcher> >::const_iterator mi = matchers.find(i->first);
			if (mi != matchers.end())
				std::copy(mi->second.begin(), mi->second.end(), back_inserter(ors));
			for (std::vector< Item<> >::const_iterator j = i->second.begin();
					j != i->second.end(); ++j)
				ors.push_back((*j)->exactQuery());
			ands.push_back(types::tag(i->first) + ":" +
					str::join(ors.begin(), ors.end(), " or "));
		}
		for (map< types::Code, vector<UnresolvedMatcher> >::const_iterator i = matchers.begin();
				i != matchers.end(); ++i)
		{
			// Codes that are in soup have already been handled
			if (soup.find(i->first) != soup.end()) continue;
			ands.push_back(types::tag(i->first) + ":" +
					str::join(i->second.begin(), i->second.end(), " or "));
		}
		return str::join(ands.begin(), ands.end(), "; ");
	}

	/**
	 * Resolve matchers into metadata.
	 *
	 * For every unresolved matcher, check it against the dataset summary
	 * to find a single matching metadata item.
	 *
	 * Returns true if all matchers have been resolved to only one item,
	 * false if there are some ambiguous matchers.
	 *
	 * This method calls consolidate(), therefore altering the matrix
	 * space and invalidating indices.
	 */
	bool resolveMatchers(ReadonlyDataset& rd)
	{
		if (matchers.empty()) return true;

		Matcher matcher = Matcher::parse(make_query());
		Summary summary;
		rd.querySummary(matcher, summary);

		MatcherResolver mr(matchers);
		summary.visit(mr);

		vector<types::Code> solved;
		for (map< types::Code, vector<UnresolvedMatcher> >::iterator i = matchers.begin();
				i != matchers.end(); ++i)
		{
			vector<UnresolvedMatcher> unsolved;
			for (vector<UnresolvedMatcher>::const_iterator j = i->second.begin();
					j != i->second.end(); ++j)
			{
				if (j->candidates.size() == 1)
				{
					soup[i->first].push_back(*j->candidates.begin());
				} else {
					unsolved.push_back(*j);
				}
			}
			if (unsolved.empty())
				solved.push_back(i->first);
			else
				i->second = unsolved;
		}
		for (vector<types::Code>::const_iterator i = solved.begin();
				i != solved.end(); ++i)
			matchers.erase(*i);

		consolidate();		

		return matchers.empty();
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
			string type = line.substr(0, pos);
			string rest = str::trim(line.substr(pos+1));
			if (str::startsWith(type, "match "))
			{
				type = str::trim(type.substr(6));
				types::Code code = types::parseCodeName(type);
				matcher::MatcherType* mt = matcher::MatcherType::find(type);
				if (!mt)
					throw wibble::exception::Consistency("parsing file " + fname,
							"cannot find a matcher for \"" + type + "\"");

				// Parse in order to validate
				auto_ptr<matcher::OR> m(matcher::OR::parse(*mt, rest));

				// Add to the list of matchers to resolve
				matchers[code].push_back(UnresolvedMatcher(code, rest));
			} else {
				types::Code code = types::parseCodeName(type);
				Item<> item = decodeString(code, rest);
				soup[code].push_back(item);
			}
		}

		consolidate();
	}

	/**
	 * Consolidate the soup space: remove duplicates, sort the vectors
	 *
	 * This can be called more than once if the space changes, but it
	 * invalidates the previous linearisation of the matrix space, which
	 * means that all previously generated indices are to be considered
	 * invalid.
	 */
	void consolidate()
	{
		dim_sizes.clear();
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

		cerr << "Matchers to resolve:" << endl;
                for (std::map< types::Code, vector<UnresolvedMatcher> >::const_iterator i = mdgrid.matchers.begin();
				i != mdgrid.matchers.end(); ++i)
		{
			cerr << " " << types::tag(i->first) << ":" << endl;
			for (std::vector<UnresolvedMatcher>::const_iterator j = i->second.begin();
					j != i->second.end(); ++j)
			{
				cerr << "  " << *j << endl;
			}
		}

		// Detect the dataset
		ConfigFile cfg;
		ReadonlyDataset::readConfig(opts.next(), cfg);
		cerr << "Dataset config:" << endl;
		cfg.output(cerr, "(stderr)");

		// Open the dataset
		auto_ptr<ReadonlyDataset> ds(ReadonlyDataset::create(*cfg.sectionBegin()->second));

		// Resolve matchers
		if (!mdgrid.resolveMatchers(*ds))
		{
			cerr << "These matchers did not match one item unambigously:" << endl;
			for (std::map< types::Code, vector<UnresolvedMatcher> >::const_iterator i = mdgrid.matchers.begin();
					i != mdgrid.matchers.end(); ++i)
			{
				for (std::vector<UnresolvedMatcher>::const_iterator j = i->second.begin();
						j != i->second.end(); ++j)
				{
					cerr << " " << j->matcher << ": ";
					if (j->candidates.empty())
						cerr << "no valid candidates found." << endl;
					else
					{
						cerr << "found more than one candidate:" << endl;
						for (std::set< Item<> >::const_iterator k = j->candidates.begin();
								k != j->candidates.end(); ++k)
							cerr << "  " << *k << endl;
					}
				}
			}
			return 1;
		} else
			cerr << "All matchers resolved." << endl;

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
