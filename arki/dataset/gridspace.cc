/*
 * dataset/gridspace - Filter another dataset over a dense data grid
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
 * You should have received a copy of the GNU General Public License along
 * with this program; if not, write to the Free Software Foundation, Inc.,
 * 51 Franklin Street, Fifth Floor, Boston, MA 02110-1301 USA.
 *
 * Author: Enrico Zini <enrico@enricozini.com>
 */

#include <arki/dataset/gridspace.h>
#include <arki/utils/dataset.h>
#include <arki/metadata.h>
#include <arki/matcher.h>
#include <arki/summary.h>
#include <arki/nag.h>
#include <arki/postprocess.h>

#include "config.h"

#ifdef HAVE_LUA
#include <arki/report.h>
#endif

// #include <iostream>

using namespace std;
using namespace wibble;
using namespace arki::utils;

namespace arki {
namespace dataset {

namespace gridspace {

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

int MDGrid::index(const Metadata& md) const
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

std::vector< Item<> > MDGrid::expand(size_t index) const
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

std::string MDGrid::make_query() const
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

bool MDGrid::resolveMatchers(ReadonlyDataset& rd)
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

	return matchers.empty();
}

void MDGrid::clear()
{
	soup.clear();
	matchers.clear();
	dim_sizes.clear();
	maxidx = 0;
}

void MDGrid::add(const Item<>& item)
{
	soup[item->serialisationCode()].push_back(item);
}

void MDGrid::add(types::Code code, const std::string& expr)
{
	matchers[code].push_back(UnresolvedMatcher(code, expr));
}

void MDGrid::read(std::istream& input, const std::string& fname)
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
			add(code, rest);
		} else {
			types::Code code = types::parseCodeName(type);
			Item<> item = decodeString(code, rest);
			add(item);
		}
	}
}

void MDGrid::consolidate()
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

/**
 * Given a metadata stream, check that there is one and only one item for every
 * point in the grid space
 */
struct SpaceChecker : public std::vector<bool>, public MetadataConsumer
{
	const MDGrid& mdg;
	size_t received;
	size_t duplicates;
	size_t notfit;

	SpaceChecker(const MDGrid& mdg) : mdg(mdg), received(0), duplicates(0), notfit(0)
	{
		resize(mdg.maxidx, false);
	}

        virtual bool operator()(Metadata& md)
	{
		int idx = mdg.index(md);
		if (idx == -1)
			++notfit;
		else if (at(idx))
			++duplicates;
		else
		{
			(*this)[idx] = true;
			++received;
		}
		return true;
	}
};

struct FilterValid : public MetadataConsumer
{
	const MDGrid& mdg;
	MetadataConsumer& next;

	FilterValid(const MDGrid& mdg, MetadataConsumer& next) : mdg(mdg), next(next)
	{
	}

        virtual bool operator()(Metadata& md)
	{
		if (mdg.index(md) != -1)
			return next(md);
		return true;
	}
};


}

Gridspace::Gridspace(ReadonlyDataset& nextds) : nextds(nextds)
{
}

Gridspace::~Gridspace()
{
}

void Gridspace::validate()
{
	mdgrid.consolidate();

	// Resolve matchers
	if (!mdgrid.resolveMatchers(nextds))
	{
		stringstream err;
		err << "Some matchers did not resolve unambiguously to one metadata item:" << endl;
		for (std::map< types::Code, vector<gridspace::UnresolvedMatcher> >::const_iterator i = mdgrid.matchers.begin();
				i != mdgrid.matchers.end(); ++i)
		{
			for (std::vector<gridspace::UnresolvedMatcher>::const_iterator j = i->second.begin();
					j != i->second.end(); ++j)
			{
				err << " " << j->matcher;
				if (j->candidates.empty())
					err << "(no match)" << endl;
				else
				{
					err << "(matches "
				            << str::join(j->candidates.begin(), j->candidates.end(), "; ")
					    << ")" << endl;
				}
			}
		}
		throw wibble::exception::Consistency("resolving gridspace matchers", err.str());
	} else
		nag::verbose("All matchers resolved unambiguously.");

	nag::verbose("Number of combinations: %d.", mdgrid.maxidx);

	/*
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
	*/

	// Read the metadata and fit in a vector sized with maxidx
	gridspace::SpaceChecker sc(mdgrid);
	dataset::DataQuery dq(Matcher::parse(mdgrid.make_query()), false);
	nextds.queryData(dq, sc);

	nag::verbose("Found: %d/%d.", sc.received, mdgrid.maxidx);
	nag::verbose("Duplicates: %d.", sc.duplicates);
	nag::verbose("Unfit: %d.", sc.notfit);

	if (sc.received < mdgrid.maxidx)
		throw wibble::exception::Consistency("validating gridspace",
				str::fmtf("There are %d/%d missing items in the result set",
					mdgrid.maxidx-sc.received, mdgrid.maxidx));

	if (sc.duplicates)
		throw wibble::exception::Consistency("validating gridspace",
				"The dataset contains more than one element per data grid point");
}

void Gridspace::queryData(const dataset::DataQuery& q, MetadataConsumer& consumer)
{
	gridspace::FilterValid fv(mdgrid, consumer);
	nextds.queryData(q, fv);
}

void Gridspace::querySummary(const Matcher& matcher, Summary& summary)
{
	using namespace wibble::str;

	Summary s;
	nextds.querySummary(Matcher::parse(mdgrid.make_query()), s);
	s.filter(matcher, summary);
}

void Gridspace::queryBytes(const dataset::ByteQuery& q, std::ostream& out)
{
	switch (q.type)
	{
		case dataset::ByteQuery::BQ_DATA: {
			ds::DataOnly dataonly(out);
			gridspace::FilterValid fv(mdgrid, dataonly);
			nextds.queryData(q, fv);
			break;
		}
		case dataset::ByteQuery::BQ_POSTPROCESS: {
			Postprocess postproc(q.param, out);
			gridspace::FilterValid fv(mdgrid, postproc);
			nextds.queryData(q, fv);
			postproc.flush();
			break;
		}
		case dataset::ByteQuery::BQ_REP_METADATA: {
#ifdef HAVE_LUA
			Report rep;
			rep.captureOutput(out);
			rep.load(q.param);
			gridspace::FilterValid fv(mdgrid, rep);
			nextds.queryData(q, fv);
			rep.report();
#endif
			break;
		}
		case dataset::ByteQuery::BQ_REP_SUMMARY: {
#ifdef HAVE_LUA
			Report rep;
			rep.captureOutput(out);
			rep.load(q.param);
			Summary s;
			querySummary(q.matcher, s);
			rep(s);
			rep.report();
#endif
			break;
		}
		default:
			throw wibble::exception::Consistency("querying dataset", "unsupported query type: " + str::fmt((int)q.type));
	}
}

}
}
// vim:set ts=4 sw=4:
