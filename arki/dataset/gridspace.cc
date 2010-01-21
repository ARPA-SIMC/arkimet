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

#include <wibble/regexp.h>

// #include <iostream>

using namespace std;
using namespace wibble;
using namespace arki::utils;

namespace arki {
namespace dataset {

namespace gridspace {

struct MatcherResolver : public MetadataConsumer
{
	std::map<types::Code, std::vector<UnresolvedMatcher> >& oneMatchers;
	std::map<types::Code, std::vector<UnresolvedMatcher> >& allMatchers;

	MatcherResolver(std::map<types::Code, std::vector<UnresolvedMatcher> >& oneMatchers,
			std::map<types::Code, std::vector<UnresolvedMatcher> >& allMatchers)
		: oneMatchers(oneMatchers), allMatchers(allMatchers) {}

	void resolve(std::map<types::Code, std::vector<UnresolvedMatcher> >& matchers, const Metadata& md)
	{
		for (map<types::Code, vector<UnresolvedMatcher> >::iterator i = matchers.begin();
				i != matchers.end(); ++i)
		{
			UItem<> item = md.get(i->first);

			if (!item.defined()) continue;

			for (vector<UnresolvedMatcher>::iterator j = i->second.begin();
					j != i->second.end(); ++j)
			{
				if (j->candidates.find(item) != j->candidates.end()) continue;
				if (j->matcher(item))
				{
					j->candidates.insert(item);
				}
			}
		}
	}

	virtual bool operator()(Metadata& md)
        {
		resolve(oneMatchers, md);
		resolve(allMatchers, md);
		return true;
	}
};

MDGrid::MDGrid()
	: maxidx(0), all_local(false) {}

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
		if (lb == i->second.end())
			return -1;
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
	set<types::Code> dimensions;

	// Collect the codes as dimensions of relevance
	for (std::map<types::Code, std::vector< Item<> > >::const_iterator i = soup.begin();
			i != soup.end(); ++i)
		dimensions.insert(i->first);
	for (map< types::Code, vector<UnresolvedMatcher> >::const_iterator i = oneMatchers.begin();
			i != oneMatchers.end(); ++i)
		dimensions.insert(i->first);
	for (map< types::Code, vector<UnresolvedMatcher> >::const_iterator i = allMatchers.begin();
			i != allMatchers.end(); ++i)
		dimensions.insert(i->first);
	for (map< types::Code, vector<string> >::const_iterator i = extraMatchers.begin();
			i != extraMatchers.end(); ++i)
		dimensions.insert(i->first);

	vector<string> ands;
	for (set<types::Code>::const_iterator i = dimensions.begin(); i != dimensions.end(); ++i)
	{
		vector<string> ors;

		std::map<types::Code, std::vector< Item<> > >::const_iterator si = soup.find(*i);
		map< types::Code, vector<UnresolvedMatcher> >::const_iterator oi = oneMatchers.find(*i);
		map< types::Code, vector<UnresolvedMatcher> >::const_iterator ai = allMatchers.find(*i);
		map< types::Code, vector<string> >::const_iterator ei = extraMatchers.find(*i);

		// OR all the matchers first
		if (oi != oneMatchers.end())
			std::copy(oi->second.begin(), oi->second.end(), back_inserter(ors));
		if (ai != allMatchers.end())
			std::copy(ai->second.begin(), ai->second.end(), back_inserter(ors));
		if (ei != extraMatchers.end())
			std::copy(ei->second.begin(), ei->second.end(), back_inserter(ors));

		Matcher m;
		if (!ors.empty())
			m = Matcher::parse(types::tag(*i) + ":" +
				str::join(ors.begin(), ors.end(), " or "));

		// then OR all the soup items that are not matched by the ORed matchers
		if (si != soup.end())
			for (std::vector< Item<> >::const_iterator j = si->second.begin();
					j != si->second.end(); ++j)
				if (m.empty() || !m(*j))
					ors.push_back((*j)->exactQuery());
		ands.push_back(types::tag(*i) + ":" + str::join(ors.begin(), ors.end(), " or "));
	}
	return str::join(ands.begin(), ands.end(), "; ");
}

struct AreAllLocal : public MetadataConsumer
{
	bool allLocal;
	MetadataConsumer& next;

	AreAllLocal(MetadataConsumer& next) : allLocal(true), next(next) {}

	bool operator()(Metadata& md)
        {
		if (allLocal && md.source->style() == types::Source::INLINE)
			allLocal = false;
		return next(md);
        }

};

void MDGrid::want_mds(ReadonlyDataset& rd)
{
	if (!mds.empty()) return;

	// Query the metadata only and keep them around
	Matcher matcher = Matcher::parse(make_query());
	mds.clear();
	AreAllLocal aal(mds);
	rd.queryData(DataQuery(matcher, false), aal);
	all_local = aal.allLocal;
}

void MDGrid::find_matcher_candidates()
{
	MatcherResolver mr(oneMatchers, allMatchers);
	mds.queryData(DataQuery(Matcher(), false), mr);
}

bool MDGrid::resolveMatchers(ReadonlyDataset& rd)
{
	want_mds(rd);
	if (mds.empty())
		throw wibble::exception::Consistency("validating gridspace information", "the metadata and matcher given do not match any item in the dataset");

	if (oneMatchers.empty() && allMatchers.empty()) return true;

	find_matcher_candidates();

	// Copy items from solved oneMatchers to the soup
	for (map< types::Code, vector<UnresolvedMatcher> >::iterator i = oneMatchers.begin();
			i != oneMatchers.end(); )
	{
		vector<UnresolvedMatcher> unsolved;
		for (vector<UnresolvedMatcher>::const_iterator j = i->second.begin();
				j != i->second.end(); ++j)
			if (j->candidates.size() == 1)
				soup[i->first].push_back(*j->candidates.begin());
			else
				unsolved.push_back(*j);
		if (unsolved.empty())
		{
			map< types::Code, vector<UnresolvedMatcher> >::iterator next = i;
			++next;
			oneMatchers.erase(i);
			i = next;
		}
		else
		{
			i->second = unsolved;
			++i;
		}
	}

	// Copy items from solved allMatchers to the soup
	for (map< types::Code, vector<UnresolvedMatcher> >::iterator i = allMatchers.begin();
			i != allMatchers.end(); )
	{
		vector<UnresolvedMatcher> unsolved;
		for (vector<UnresolvedMatcher>::const_iterator j = i->second.begin();
				j != i->second.end(); ++j)
			if (j->candidates.empty())
				unsolved.push_back(*j);
			else
				std::copy(j->candidates.begin(), j->candidates.end(), back_inserter(soup[i->first]));
		if (unsolved.empty())
		{
			map< types::Code, vector<UnresolvedMatcher> >::iterator next = i;
			++next;
			allMatchers.erase(i);
			i = next;
		}
		else
		{
			i->second = unsolved;
			++i;
		}
	}

	return oneMatchers.empty() && allMatchers.empty();
}

void MDGrid::clear()
{
	soup.clear();
	oneMatchers.clear();
	allMatchers.clear();
	mds.clear();
	dim_sizes.clear();
	maxidx = 0;
	all_local = false;
}

void MDGrid::add(const Item<>& item)
{
	soup[item->serialisationCode()].push_back(item);
}

void MDGrid::addOne(types::Code code, const std::string& expr)
{
	oneMatchers[code].push_back(UnresolvedMatcher(code, expr));
}

void MDGrid::addAll(types::Code code, const std::string& expr)
{
	allMatchers[code].push_back(UnresolvedMatcher(code, expr));
}

void MDGrid::addTimeInterval(const Item<types::Time>& begin, const Item<types::Time>& end, int step)
{
	// Condense the interval in a query, to avoid make_query generating
	// huge queries when there are long dense intervals
	extraMatchers[types::TYPE_REFTIME].push_back(">= " + begin->toSQL() + ",<" + end->toSQL());

	vector< Item<types::reftime::Position> > items = types::reftime::Position::generate(*begin, *end, step);
	for (vector< Item<types::reftime::Position> >::const_iterator i = items.begin();
			i != items.end(); ++i)
		soup[types::TYPE_REFTIME].push_back(*i);
}

void MDGrid::read(std::istream& input, const std::string& fname)
{
#define SEP "[[:blank:]]+"
	ERegexp re_rtinterval("^[[:blank:]]*from" SEP "(.+)" SEP "to" SEP "(.+)" SEP "step" SEP "([0-9]+)[[:space:]]*$", 4, REG_ICASE);
#undef SEP

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
		if (str::startsWith(type, "match one "))
		{
			type = str::trim(type.substr(10));
			types::Code code = types::parseCodeName(type);
			addOne(code, rest);
		} else if (str::startsWith(type, "match all ")) {
			type = str::trim(type.substr(10));
			types::Code code = types::parseCodeName(type);
			addAll(code, rest);
		} else if (str::startsWith(type, "reftime sequence")) {
			if (re_rtinterval.match(rest))
			{
				Item<types::Time> begin = types::Time::createFromSQL(re_rtinterval[1]);
				Item<types::Time> end = types::Time::createFromSQL(re_rtinterval[2]);
				int step = strtoul(re_rtinterval[3].c_str(), 0, 10);
				addTimeInterval(begin, end, step);
			} else
				throw wibble::exception::Consistency("parsing file " + fname,
						"cannot parse reftime sequence \"" + rest + "\"");
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

void Gridspace::validateMatchers()
{
	mdgrid.consolidate();

	// Resolve matchers
	if (!mdgrid.resolveMatchers(nextds))
	{
		stringstream err;
		err << "Some matchers did not resolve unambiguously to one metadata item:" << endl;
		for (std::map< types::Code, vector<gridspace::UnresolvedMatcher> >::const_iterator i = mdgrid.oneMatchers.begin();
				i != mdgrid.oneMatchers.end(); ++i)
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
		for (std::map< types::Code, vector<gridspace::UnresolvedMatcher> >::const_iterator i = mdgrid.allMatchers.begin();
				i != mdgrid.allMatchers.end(); ++i)
			for (std::vector<gridspace::UnresolvedMatcher>::const_iterator j = i->second.begin();
					j != i->second.end(); ++j)
				err << " " << j->matcher << "(no match)" << endl;
		throw wibble::exception::Consistency("resolving gridspace matchers", err.str());
	} else
		nag::verbose("All matchers resolved unambiguously.");

	mdgrid.consolidate();
}

void Gridspace::validate()
{
	validateMatchers();

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
	mdgrid.mds.queryData(dq, sc);

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

/// Dump details about this gridspace to the given output stream
void Gridspace::dump(ostream& o, const std::string& prefix) const
{
	using namespace gridspace;

	// Dump soup
	o << prefix << "Resolved metadata:" << endl;
	for (std::map<types::Code, std::vector< Item<> > >::const_iterator i = mdgrid.soup.begin();
			i != mdgrid.soup.end(); ++i)
	{
		o << prefix << " " << types::tag(i->first) << ":" << endl;
		for (std::vector< Item<> >::const_iterator j = i->second.begin();
				j != i->second.end(); ++j)
			o << prefix << "  " << *j << endl;
	}

	// Dump one matchers
	o << prefix << "Unresolved \"match one\" matchers:" << endl;
	for (std::map<types::Code, std::vector<UnresolvedMatcher> >::const_iterator i = mdgrid.oneMatchers.begin();
			i != mdgrid.oneMatchers.end(); ++i)
	{
		o << prefix << " " << types::tag(i->first) << ":" << endl;
		for (std::vector<UnresolvedMatcher>::const_iterator j = i->second.begin();
				j != i->second.end(); ++j)
			o << prefix << "  " << *j << endl;
	}

	// Dump all matchers
	o << prefix << "Unresolved \"match all\" matchers:" << endl;
	for (std::map<types::Code, std::vector<UnresolvedMatcher> >::const_iterator i = mdgrid.allMatchers.begin();
			i != mdgrid.allMatchers.end(); ++i)
	{
		o << prefix << " " << types::tag(i->first) << ":" << endl;
		for (std::vector<UnresolvedMatcher>::const_iterator j = i->second.begin();
				j != i->second.end(); ++j)
			o << prefix << "  " << *j << endl;
	}

	// Dump extra matchers
	o << prefix << "Extra match expressions used to make the dataset query:" << endl;
	for (std::map<types::Code, std::vector<string> >::const_iterator i = mdgrid.extraMatchers.begin();
			i != mdgrid.extraMatchers.end(); ++i)
	{
		o << prefix << " " << types::tag(i->first) << ":" << endl;
		for (std::vector<string>::const_iterator j = i->second.begin();
				j != i->second.end(); ++j)
			o << prefix << "  " << *j << endl;
	}

	// Dump misc gridspace info
	o << prefix << "Elements in metadata cache: " << mdgrid.mds.size() << endl;
	o << prefix << "Number of grid dimensions: " << mdgrid.dim_sizes.size() << endl;
	o << prefix << "Number of items in grid: " << mdgrid.maxidx << endl;
	o << prefix << "Item data is " << (mdgrid.all_local ? "local" : "remote") << endl;
	o << prefix << "Arkimet query: " << mdgrid.make_query() << endl;
}

void Gridspace::dumpExpands(std::ostream& o, const std::string& prefix)
{
	using namespace gridspace;

	mdgrid.consolidate();

	mdgrid.want_mds(nextds);
	if (mdgrid.mds.empty())
	{
		o << prefix << "The metadata and matchers given do not match any item in the dataset." << endl;
		return;
	}

	if (mdgrid.oneMatchers.empty() && mdgrid.allMatchers.empty())
	{
		o << prefix << "There are no matchers to resolve." << endl;
		return;
	}

	mdgrid.find_matcher_candidates();

	// Dump matchers and candidates

	// Dump one matchers
	if (!mdgrid.oneMatchers.empty())
	{
		o << prefix << "\"match one\" matchers:" << endl;
		for (std::map<types::Code, std::vector<UnresolvedMatcher> >::const_iterator i = mdgrid.oneMatchers.begin();
				i != mdgrid.oneMatchers.end(); ++i)
		{
			o << prefix << " " << types::tag(i->first) << ":" << endl;
			for (std::vector<UnresolvedMatcher>::const_iterator j = i->second.begin();
					j != i->second.end(); ++j)
			{
				o << prefix << "  " << *j << " expands to: " << endl;
				for (set< Item<> >::const_iterator k = j->candidates.begin();
						k != j->candidates.end(); ++k)
				{
					o << prefix << "   " << *k << endl;
				}
			}
		}
	}

	// Dump all matchers
	if (!mdgrid.allMatchers.empty())
	{
		o << prefix << "\"match all\" matchers:" << endl;
		for (std::map<types::Code, std::vector<UnresolvedMatcher> >::const_iterator i = mdgrid.allMatchers.begin();
				i != mdgrid.allMatchers.end(); ++i)
		{
			o << prefix << " " << types::tag(i->first) << ":" << endl;
			for (std::vector<UnresolvedMatcher>::const_iterator j = i->second.begin();
					j != i->second.end(); ++j)
			{
				o << prefix << "  " << *j << " expands to: " << endl;
				for (set< Item<> >::const_iterator k = j->candidates.begin();
						k != j->candidates.end(); ++k)
				{
					o << prefix << "   " << *k << endl;
				}
			}
		}
	}
}

namespace {

struct ItemCountDumper : public MetadataConsumer
{
	const gridspace::MDGrid& mdg;
	std::map< Item<>, int > counts;

	ItemCountDumper(const gridspace::MDGrid& mdg) : mdg(mdg)
	{
		// Initialise all counts at 0
		for (std::map<types::Code, std::vector< Item<> > >::const_iterator i = mdg.soup.begin();
				i != mdg.soup.end(); ++i)
			for (std::vector< Item<> >::const_iterator j = i->second.begin();
					j != i->second.end(); ++j)
				counts[*j] = 0;
	}

	virtual bool operator()(Metadata& md)
	{
		for (std::map<types::Code, std::vector< Item<> > >::const_iterator i = mdg.soup.begin();
				i != mdg.soup.end(); ++i)
		{
			UItem<> item = md.get(i->first);
			if (!item.defined()) continue;
			vector< Item<> >::const_iterator lb =
				lower_bound(i->second.begin(), i->second.end(), item);
			if (lb == i->second.end()) continue;
			if (*lb != item) continue;
			++counts[item];
		}
		return true;
	}
};

}

void Gridspace::dumpCountPerItem(std::ostream& out, const std::string& prefix)
{
	ItemCountDumper icd(mdgrid);
	mdgrid.mds.sendTo(icd);
	for (std::map< Item<>, int >::const_iterator i = icd.counts.begin();
			i != icd.counts.end(); ++i)
	{
		out << prefix << i->first->tag() << ":" << i->first << ": "
	            << i->second << "/" << mdgrid.maxidx / mdgrid.soup[i->first->serialisationCode()].size() << endl;
	}
}

void Gridspace::queryData(const dataset::DataQuery& q, MetadataConsumer& consumer)
{
	if (mdgrid.all_local || !q.withData)
	{
		gridspace::FilterValid fv(mdgrid, consumer);
		mdgrid.mds.queryData(q, fv);
	} else {
		gridspace::FilterValid fv(mdgrid, consumer);
		nextds.queryData(q, fv);
	}
}

void Gridspace::querySummary(const Matcher& matcher, Summary& summary)
{
	using namespace wibble::str;

	utils::metadata::Summarise cons(summary);
	queryData(dataset::DataQuery(matcher, false), cons);
}

void Gridspace::queryBytes(const dataset::ByteQuery& q, std::ostream& out)
{
	switch (q.type)
	{
		case dataset::ByteQuery::BQ_DATA: {
			ds::DataOnly dataonly(out);
			queryData(q, dataonly);
			break;
		}
		case dataset::ByteQuery::BQ_POSTPROCESS: {
			Postprocess postproc(q.param, out);
			nextds.queryData(q, postproc);
			postproc.flush();
			break;
		}
		case dataset::ByteQuery::BQ_REP_METADATA: {
#ifdef HAVE_LUA
			Report rep;
			rep.captureOutput(out);
			rep.load(q.param);
			nextds.queryData(q, rep);
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
