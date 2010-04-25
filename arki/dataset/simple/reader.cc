/*
 * dataset/simple/reader - Reader for simple datasets with no duplicate checks
 *
 * Copyright (C) 2009--2010  ARPA-SIM <urpsim@smr.arpa.emr.it>
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

#include <arki/dataset/simple/reader.h>
#include <arki/dataset/simple/index.h>
#include <arki/configfile.h>
#include <arki/summary.h>
#include <arki/types/reftime.h>
#include <arki/matcher.h>
#include <arki/utils/metadata.h>
#include <arki/utils/files.h>
#include <arki/utils/dataset.h>
#include <arki/utils/compress.h>
#include <arki/scan/any.h>
#include <arki/postprocess.h>
#include <arki/sort.h>
#include <arki/nag.h>

#include <wibble/exception.h>
#include <wibble/sys/fs.h>
#include <wibble/string.h>

#include <fstream>
#include <ctime>
#include <cstdio>

#include "config.h"

#ifdef HAVE_LUA
#include <arki/report.h>
#endif

using namespace std;
using namespace wibble;
using namespace arki;
using namespace arki::types;
using namespace arki::utils;

namespace arki {
namespace dataset {
namespace simple {

Reader::Reader(const ConfigFile& cfg)
	: m_dir(cfg.value("path")), m_mft(0)
{
	// Create the directory if it does not exist
	wibble::sys::fs::mkpath(m_dir);
	
	if (Manifest::exists(m_dir))
	{
		auto_ptr<Manifest> mft = Manifest::create(m_dir);

		m_mft = mft.release();
		m_mft->openRO();
	}
}

Reader::Reader(const std::string& dir)
	: m_dir(dir), m_mft(0)
{
	// Create the directory if it does not exist
	wibble::sys::fs::mkpath(m_dir);
	
	if (Manifest::exists(m_dir))
	{
		auto_ptr<Manifest> mft = Manifest::create(m_dir);

		m_mft = mft.release();
		m_mft->openRO();
	}
}

Reader::~Reader()
{
	if (m_mft) delete m_mft;
}

bool Reader::is_dataset(const std::string& dir)
{
	return Manifest::exists(dir);
}

void Reader::queryData(const dataset::DataQuery& q, MetadataConsumer& consumer)
{
	if (!m_mft) return;

	vector<string> files;
	m_mft->fileList(q.matcher, files);

	// TODO: does it make sense to check with the summary first?

	MetadataConsumer* c = &consumer;
	auto_ptr<sort::Stream> sorter;
	auto_ptr<ds::DataInliner> inliner;

	if (q.withData)
	{
		inliner.reset(new ds::DataInliner(*c));
		c = inliner.get();
	}
		
	if (q.sorter)
	{
		sorter.reset(new sort::Stream(*q.sorter, *c));
		c = sorter.get();
	}

	ds::PathPrepender prepender(sys::fs::abspath(m_dir), *c);
	ds::MatcherFilter filter(q.matcher, prepender);
	for (vector<string>::const_iterator i = files.begin(); i != files.end(); ++i)
		scan::scan(str::joinpath(m_dir, *i), filter);
}

void Reader::queryBytes(const dataset::ByteQuery& q, std::ostream& out)
{
	if (!m_mft) return;

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

			// TODO: if we flush here, do we close the postprocessor for a further query?
			// TODO: POSTPROCESS isn't it just a query for the metadata?
			// TODO: in that case, the reader should implement it
			// TODO: as such: simpler code, and handle the case of
			// TODO: multiple archives nicely
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

void Reader::querySummaries(const Matcher& matcher, Summary& summary)
{
	vector<string> files;
	m_mft->fileList(matcher, files);

	for (vector<string>::const_iterator i = files.begin(); i != files.end(); ++i)
	{
		string pathname = str::joinpath(m_dir, *i);

		// Silently skip files that have been deleted
		if (!sys::fs::access(pathname + ".summary", R_OK))
			continue;

		Summary s;
		s.readFile(pathname + ".summary");
		s.filter(matcher, summary);
	}
}

void Reader::querySummary(const Matcher& matcher, Summary& summary)
{
	if (!m_mft) return;

	// Check if the matcher discriminates on reference times
	const matcher::Implementation* rtmatch = 0;
	if (matcher.m_impl)
		rtmatch = matcher.m_impl->get(types::TYPE_REFTIME);

	if (!rtmatch)
	{
		// The matcher does not contain reftime, we can work with a
		// global summary
		string cache_pathname = str::joinpath(m_dir, "summary");

		if (sys::fs::access(cache_pathname, R_OK))
		{
			Summary s;
			s.readFile(cache_pathname);
			s.filter(matcher, summary);
		} else if (sys::fs::access(m_dir, W_OK)) {
			// Rebuild the cache
			Summary s;
			querySummaries(Matcher(), s);

			// Save the summary
			s.writeAtomically(cache_pathname);

			// Query the newly generated summary that we still have
			// in memory
			s.filter(matcher, summary);
		} else
			querySummaries(matcher, summary);
	} else {
		querySummaries(matcher, summary);
	}
}

void Reader::maintenance(maintenance::MaintFileVisitor& v)
{
	if (!m_mft) return;
	m_mft->check(v);
}

}
}
}
