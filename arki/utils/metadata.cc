/*
 * utils/metadata - Useful functions for working with metadata
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

#include <arki/utils/metadata.h>
#include <arki/utils/dataset.h>
#include <arki/summary.h>
#include <arki/sort.h>
#include <arki/postprocess.h>
#include <fstream>
#include <memory>

#include "config.h"

#ifdef HAVE_LUA
#include <arki/report.h>
#endif

using namespace std;
using namespace wibble;

namespace arki {
namespace utils {
namespace metadata {

void Collector::writeAtomically(const std::string& fname) const
{
	AtomicWriter writer(fname);
	for (const_iterator i = begin(); i != end(); ++i)
		writer(*i);
	writer.close();
}

void Collector::queryData(const dataset::DataQuery& q, MetadataConsumer& consumer)
{
	// First ask the index.  If it can do something useful, iterate with it
	//
	// If the index would just do a linear scan of everything, then instead
	// scan the directories in sorted order.
	//
	// For each directory try to match its summary first, and if it matches
	// then produce all the contents.
	MetadataConsumer* c = &consumer;
	auto_ptr<sort::Stream> sorter;
	auto_ptr<ds::TemporaryDataInliner> inliner;

	if (q.withData)
	{
		inliner.reset(new ds::TemporaryDataInliner(*c));
		c = inliner.get();
	}

	if (q.sorter)
	{
		sorter.reset(new sort::Stream(*q.sorter, *c));
		c = sorter.get();
	}

	for (std::vector<Metadata>::iterator i = begin();
			i != end(); ++i)
		if (q.matcher(*i))
			if (!(*c)(*i))
				break;
}

void Collector::querySummary(const Matcher& matcher, Summary& summary)
{
	using namespace wibble::str;

	for (std::vector<Metadata>::iterator i = begin();
			i != end(); ++i)
		if (matcher(*i))
			summary.add(*i);
}

void Collector::queryBytes(const dataset::ByteQuery& q, std::ostream& out)
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
			queryData(q, postproc);
			postproc.flush();
			break;
		}
		case dataset::ByteQuery::BQ_REP_METADATA: {
#ifdef HAVE_LUA
			Report rep;
			rep.captureOutput(out);
			rep.load(q.param);
			queryData(q, rep);
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

AtomicWriter::AtomicWriter(const std::string& fname)
	: fname(fname), tmpfname(fname + ".tmp"), outmd(0)
{
	outmd = new std::ofstream;
	outmd->open(tmpfname.c_str(), ios::out | ios::trunc);
	if (!outmd->is_open() || outmd->fail())
		throw wibble::exception::File(tmpfname, "opening file for writing");
}

AtomicWriter::~AtomicWriter()
{
	close();
}

bool AtomicWriter::operator()(Metadata& md)
{
	md.write(*outmd, tmpfname);
	return true;
}

bool AtomicWriter::operator()(const Metadata& md)
{
	md.write(*outmd, tmpfname);
	return true;
}

void AtomicWriter::close()
{
	if (outmd)
	{
		outmd->close();
		delete outmd;
		outmd = 0;
		if (rename(tmpfname.c_str(), fname.c_str()) < 0)
			throw wibble::exception::System("Renaming " + tmpfname + " to " + fname);
	}
}

bool Summarise::operator()(Metadata& md)
{
	s.add(md);
	return true;
}

}
}
}
// vim:set ts=4 sw=4:
