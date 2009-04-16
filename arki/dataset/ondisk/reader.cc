/*
 * dataset/ondisk/reader - Local on disk dataset reader
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

#include <arki/dataset/ondisk.h>
#include <arki/dataset/targetfile.h>
#include <arki/dataset/ondisk/index.h>
#include <arki/configfile.h>
#include <arki/metadata.h>
#include <arki/matcher.h>
#include <arki/utils.h>
#include <arki/utils/files.h>
#include <arki/summary.h>
#include <arki/postprocess.h>

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
using namespace arki::utils::files;

namespace arki {
namespace dataset {
namespace ondisk {

Reader::Reader(const ConfigFile& cfg) : m_idx(0), m_tf(0)
{
	m_root = cfg.value("path");
	m_tf = TargetFile::create(cfg);

	// If there is no 'index' in the config file, don't index anything
	if (cfg.value("index") != string()
	 && !hasIndexFlagfile(m_root)
	 && utils::hasFlagfile(str::joinpath(m_root, "index.sqlite")))
	{
		m_idx = new RIndex(cfg);
		m_idx->open();
	}
}

Reader::~Reader()
{
	if (m_idx) delete m_idx;
	if (m_tf) delete m_tf;
}

static bool readSummary(Summary& s, const std::string& fname)
{
	std::auto_ptr<struct stat> st = wibble::sys::fs::stat(fname);
	if (st.get() == NULL)
		return false;

	ifstream in;
	in.open(fname.c_str(), ios::in);
	if (!in.is_open() || in.fail())
		throw wibble::exception::File(fname, "opening file for reading");

	return s.read(in, fname);
}

/**
 * Prepend a path to all metadata
 */
struct PathPrepender : public MetadataConsumer
{
	string path;
	MetadataConsumer& next;
	PathPrepender(const std::string& path, MetadataConsumer& next) : path(path), next(next) {}
	bool operator()(Metadata& md)
	{
		md.prependPath(path);
		return next(md);
	}
};

/**
 * Inline the data into all metadata
 */
struct DataInliner : public MetadataConsumer
{
	MetadataConsumer& next;
	DataInliner(MetadataConsumer& next) : next(next) {}
	bool operator()(Metadata& md)
	{
		// Read the data
		wibble::sys::Buffer buf = md.getData();
		// Change the source as inline
		md.setInlineData(md.source->format, buf);
		return next(md);
	}
};

/**
 * Output the data from a metadata stream into an ostream
 */
struct DataOnly : public MetadataConsumer
{
	std::ostream& out;
	DataOnly(std::ostream& out) : out(out) {}
	bool operator()(Metadata& md)
	{
		wibble::sys::Buffer buf = md.getData();
		out.write((const char*)buf.data(), buf.size());
		return true;
	}
};

static void scan(const std::string& fname, const Matcher& matcher, MetadataConsumer& consumer)
{
	Metadata md;
	ifstream in;
	in.open(fname.c_str(), ios::in);
	if (!in.is_open() || in.fail())
		throw wibble::exception::File(fname, "opening file for reading");
	while (md.read(in, fname))
	{
		if (md.deleted) continue;
		if (matcher(md))
			consumer(md);
	}
}

void Reader::queryWithoutIndex(const std::string& top, const Matcher& matcher, MetadataConsumer& consumer) const
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
	PathPrepender prepender(top, consumer);
	wibble::sys::fs::Directory dir(root);
	vector<string> dirs;
	vector<string> files;
	for (wibble::sys::fs::Directory::const_iterator i = dir.begin();
			i != dir.end(); ++i)
	{
		// Skip '.', '..' and hidden files
		if ((*i)[0] == '.') continue;
		if (utils::isdir(root, i))
			dirs.push_back(*i);
		else if (str::endsWith(*i, ".metadata"))
			files.push_back(*i);
	}
	std::sort(dirs.begin(), dirs.end());
	std::sort(files.begin(), files.end());
	for (vector<string>::const_iterator i = dirs.begin();
			i != dirs.end(); ++i)
	{
		// Query the subdirectory
		queryWithoutIndex(joinpath(top, *i), matcher, consumer);
	}
	for (vector<string>::const_iterator i = files.begin();
			i != files.end(); ++i)
	{
		string basename = (*i).substr(0, (*i).size() - 9);
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
		if (readSummary(summary, fullname + ".summary") && !matcher(summary))
			continue;
		// Scan the metadata file
		scan(joinpath(root, *i), matcher, prepender);
	}
}

void Reader::queryMetadata(const Matcher& matcher, bool withData, MetadataConsumer& consumer)
{
	// First ask the index.  If it can do something useful, iterate with it
	//
	// If the index would just do a linear scan of everything, then instead
	// scan the directories in sorted order.
	//
	// For each directory try to match its summary first, and if it matches
	// then produce all the contents.

	if (withData)
	{
		DataInliner inliner(consumer);
		PathPrepender prepender(sys::fs::abspath(m_root), inliner);
		if (!m_idx || !m_idx->query(matcher, prepender))
			queryWithoutIndex("", matcher, prepender);
	} else {
		PathPrepender prepender(sys::fs::abspath(m_root), consumer);
		if (!m_idx || !m_idx->query(matcher, prepender))
			queryWithoutIndex("", matcher, prepender);
	}
}

void Reader::queryBytes(const Matcher& matcher, std::ostream& out, ByteQuery qtype, const std::string& param)
{
	switch (qtype)
	{
		case BQ_DATA: {
			DataOnly dataonly(out);
			PathPrepender prepender(sys::fs::abspath(m_root), dataonly);
			if (!m_idx || !m_idx->query(matcher, prepender))
				queryWithoutIndex("", matcher, prepender);
			break;
		}
		case BQ_POSTPROCESS: {
			Postprocess postproc(param, out);
			PathPrepender prepender(sys::fs::abspath(m_root), postproc);
			if (!m_idx || !m_idx->query(matcher, prepender))
				queryWithoutIndex("", matcher, prepender);
			postproc.flush();
			break;
		}
		case BQ_REP_METADATA: {
#ifdef HAVE_LUA
			Report rep;
			rep.captureOutput(out);
			rep.load(param);
			queryMetadata(matcher, false, rep);
			rep.report();
#endif
			break;
		}
		case BQ_REP_SUMMARY: {
#ifdef HAVE_LUA
			Report rep;
			rep.captureOutput(out);
			rep.load(param);
			Summary s;
			querySummary(matcher, s);
			rep(s);
			rep.report();
#endif
			break;
		}
		default:
			throw wibble::exception::Consistency("querying dataset", "unsupported query type: " + str::fmt((int)qtype));
	}
}

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

void Reader::querySummary(const Matcher& matcher, Summary& summary)
{
	using namespace wibble::str;

	if (matcher.empty() || matcher->get(types::TYPE_REFTIME) == 0)
	{
		// If the matcher does not query by reference time, we can use the
		// toplevel summary and get an exact result directly from it
		Summary mine;
		if (!readSummary(mine, joinpath(m_root, "summary")))
			return;
		mine.filter(matcher, summary);
	} else {
		// Else, we match on the datafiles' summaries only
		scanSummaries("", matcher, summary);
	}
}

}
}
}
// vim:set ts=4 sw=4:
