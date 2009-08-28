/*
 * dataset/ondisk2/reader - Local on disk dataset reader
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

#include <arki/dataset/ondisk2/reader.h>
#include <arki/dataset/ondisk2/index.h>
#include <arki/dataset/ondisk2/archive.h>
#include <arki/dataset/targetfile.h>
#include <arki/types/assigneddataset.h>
#include <arki/configfile.h>
#include <arki/metadata.h>
#include <arki/matcher.h>
#include <arki/utils.h>
#include <arki/utils/files.h>
#include <arki/utils/dataset.h>
#include <arki/summary.h>
#include <arki/postprocess.h>
#include <arki/nag.h>

#include <wibble/exception.h>
#include <wibble/string.h>
#include <wibble/sys/fs.h>

#include <fstream>
#include <sstream>

#include <sys/types.h>
#include <sys/stat.h>
#include <fcntl.h>
#include <unistd.h>
#include <cerrno>

#include "config.h"

#ifdef HAVE_LUA
#include <arki/report.h>
#endif


using namespace std;
using namespace wibble;
using namespace arki::utils;

namespace arki {
namespace dataset {
namespace ondisk2 {

Reader::Reader(const ConfigFile& cfg)
       	: m_name(cfg.value("name")), m_root(cfg.value("path")),
	  m_idx(0), m_tf(0), m_archive(0)
{
	m_tf = TargetFile::create(cfg);
	m_idx = new RIndex(cfg);
	m_idx->open();
}

Reader::~Reader()
{
	if (m_idx) delete m_idx;
	if (m_tf) delete m_tf;
	if (m_archive) delete m_archive;
}

bool Reader::hasArchive() const
{
	string arcdir = str::joinpath(m_root, ".archive");
	return sys::fs::access(arcdir, F_OK);
}

Archives& Reader::archive()
{
	if (!m_archive)
		m_archive = new Archives(str::joinpath(m_root, ".archive"));
	return *m_archive;
}

const Archives& Reader::archive() const
{
	if (!m_archive)
		m_archive = new Archives(str::joinpath(m_root, ".archive"));
	return *m_archive;
}

void Reader::queryLocalData(const dataset::DataQuery& q, MetadataConsumer& consumer)
{
	MetadataConsumer* c = &consumer;
	auto_ptr<ds::DataInliner> inliner;

	if (q.withData)
	{
		inliner.reset(new ds::DataInliner(*c));
		c = inliner.get();
	}

	ds::PathPrepender prepender(sys::fs::abspath(m_root), *c);
	if (!m_idx || !m_idx->query(q, prepender))
		throw wibble::exception::Consistency("querying " + m_root, "index could not be used");
}

void Reader::queryData(const dataset::DataQuery& q, MetadataConsumer& consumer)
{
	// First ask the index.  If it can do something useful, iterate with it
	//
	// If the index would just do a linear scan of everything, then instead
	// scan the directories in sorted order.
	//
	// For each directory try to match its summary first, and if it matches
	// then produce all the contents.

	// Query the archives first
	if (hasArchive())
		archive().queryData(q, consumer);
	queryLocalData(q, consumer);
}

void Reader::queryBytes(const dataset::ByteQuery& q, std::ostream& out)
{
	// Query the archives first
	if (hasArchive())
		archive().queryBytes(q, out);

	switch (q.type)
	{
		case dataset::ByteQuery::BQ_DATA: {
			ds::DataOnly dataonly(out);
			queryLocalData(q, dataonly);
			break;
		}
		case dataset::ByteQuery::BQ_POSTPROCESS: {
			Postprocess postproc(q.param, out);
			queryLocalData(q, postproc);
			postproc.flush();
			break;
		}
		case dataset::ByteQuery::BQ_REP_METADATA: {
#ifdef HAVE_LUA
			Report rep;
			rep.captureOutput(out);
			rep.load(q.param);
			queryLocalData(q, rep);
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

#if 0
void Reader::scanSummaries(const std::string& top, const Matcher& matcher, Summary& result)
{
	using namespace wibble::str;

	const matcher::Implementation* rtmatch = 0;

	//fprintf(stderr, "TF %p IML %p TOP %s\n", m_tf, matcher.m_impl, top.c_str());

	// Check the dir name against a reftime matcher
	if (m_tf && matcher.m_impl && !top.empty())
	{
		rtmatch = matcher.m_impl->get(types::TYPE_REFTIME);
		//fprintf(stderr, "tf-check-dir %s\n", top.c_str());
		if (rtmatch && ! m_tf->pathMatches(top, *rtmatch))
		{
			//fprintf(stderr, "Skipped %s because of name\n", top.c_str());
			return;
		}
	}

	string root = joinpath(m_root, top);

	// Read the summary of the directory: if it does not match, return right
	// away
	Summary summary;
	if (readSummary(summary, joinpath(root, "summary")) && !matcher(summary))
		return;

	// If there is no summary or it does match, query the subdirectories and
	// scan the .metadata files
	wibble::sys::fs::Directory dir(root);
	for (wibble::sys::fs::Directory::const_iterator i = dir.begin();
			i != dir.end(); ++i)
	{
		// Skip '.', '..' and hidden files
		if ((*i)[0] == '.') continue;

		if (utils::isdir(root, i))
		{
			// Query the subdirectory
			scanSummaries(str::joinpath(top, *i), matcher, result);
		} else if (str::endsWith(*i, ".summary")) {
			string basename = (*i).substr(0, (*i).size() - 8);
			string relname = joinpath(top, basename);
			string fullname = joinpath(m_root, relname);

			// If there is the rebuild flagfile, skip this data file
			// because the metadata may be inconsistent
			if (hasRebuildFlagfile(fullname))
				continue;

			// Check the file name against a reftime matcher
			//if (rtmatch) fprintf(stderr, "tf-check %s\n", relname.c_str());
			if (rtmatch && ! m_tf->pathMatches(relname, *rtmatch))
			{
				//fprintf(stderr, "Skipped %s because of name\n", relname.c_str());
				continue;
			}

			// Check in the summary if we should scan this metadata
			Summary summary;
			if (!readSummary(summary, fullname + ".summary"))
				continue;
			// Filter the summary file
			summary.filter(matcher, result);
		}
	}
}
#endif

void Reader::querySummary(const Matcher& matcher, Summary& summary)
{
	using namespace wibble::str;

	// Query the archives first
	if (hasArchive())
		archive().querySummary(matcher, summary);

	if (!m_idx || !m_idx->querySummary(matcher, summary))
		throw wibble::exception::Consistency("querying " + m_root, "index could not be used");
}

}
}
}
// vim:set ts=4 sw=4:
