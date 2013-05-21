/*
 * dataset/file - Dataset on a single file
 *
 * Copyright (C) 2008--2013  ARPA-SIM <urpsim@smr.arpa.emr.it>
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

#include "config.h"

#include <arki/dataset/file.h>
#include <arki/metadata/consumer.h>
#include <arki/configfile.h>
#include <arki/matcher.h>
#include <arki/summary.h>
#include <arki/postprocess.h>
#include <arki/sort.h>
#include <arki/utils/dataset.h>
#include <arki/utils/files.h>
#include <arki/scan/any.h>
#include <wibble/exception.h>
#include <wibble/string.h>
#include <wibble/sys/fs.h>
#include <sys/stat.h>

#ifdef HAVE_LUA
#include <arki/report.h>
#endif

using namespace std;
using namespace wibble;
using namespace arki::utils;

namespace arki {
namespace dataset {

void File::readConfig(const std::string& fname, ConfigFile& cfg)
{
	using namespace wibble;

	ConfigFile section;

	if (fname == "-")
	{
		// If the input is stdin, make hardcoded assumptions
		section.setValue("name", "stdin");
		section.setValue("path", "-");
		section.setValue("type", "file");
		section.setValue("format", "arkimet");
	} else if (str::endsWith(fname, ":-")) {
		// If the input is stdin, make hardcoded assumptions
		section.setValue("name", "stdin");
		section.setValue("path", "-");
		section.setValue("type", "file");
		section.setValue("format", fname.substr(0, fname.size()-2));
	} else {
		section.setValue("type", "file");
		if (sys::fs::exists(fname))
		{
                    section.setValue("path", sys::fs::abspath(fname));
                    section.setValue("format", files::format_from_ext(fname, "arkimet"));
                    string name = str::basename(fname);
                    section.setValue("name", name);
		} else {
			size_t fpos = fname.find(':');
			if (fpos == string::npos)
				throw wibble::exception::Consistency("examining file " + fname, "file does not exist");
			section.setValue("format", files::normaliseFormat(fname.substr(0, fpos)));

			string fname1 = fname.substr(fpos+1);
			if (!sys::fs::exists(fname1))
				throw wibble::exception::Consistency("examining file " + fname1, "file does not exist");
			section.setValue("path", sys::fs::abspath(fname1));
			section.setValue("name", str::basename(fname1));
		}
	}

	// Merge into cfg
	cfg.mergeInto(section.value("name"), section);
}

File* File::create(const ConfigFile& cfg)
{
	string format = str::tolower(cfg.value("format"));
	
	if (format == "arkimet")
		return new ArkimetFile(cfg);
	if (format == "yaml")
		return new YamlFile(cfg);
#ifdef HAVE_GRIBAPI
	if (format == "grib")
		return new RawFile(cfg);
#endif
#ifdef HAVE_DBALLE
	if (format == "bufr")
		return new RawFile(cfg);
#endif
#ifdef HAVE_ODIMH5
	if (format == "odimh5")
		return new RawFile(cfg);
#endif
#ifdef HAVE_VM2
    if (format == "vm2")
        return new RawFile(cfg);
#endif
	throw wibble::exception::Consistency("creating a file dataset", "unknown file format \""+format+"\"");
}

File::File(const ConfigFile& cfg)
{
	m_pathname = cfg.value("path");
	m_format = cfg.value("format");
}

IfstreamFile::IfstreamFile(const ConfigFile& cfg) : File(cfg), m_file(0), m_close(false)
{
	if (m_pathname == "-")
	{
		m_file = &std::cin;
	} else {
		m_file = new std::ifstream(m_pathname.c_str(), ios::in);
		if (/*!m_file->is_open() ||*/ m_file->fail())
			throw wibble::exception::File(m_pathname, "opening file for reading");
		m_close = true;
	}
}

IfstreamFile::~IfstreamFile()
{
	if (m_file && m_close)
	{
		delete m_file;
	}
}

void File::queryData(const dataset::DataQuery& q, metadata::Consumer& consumer)
{
	scan(q, consumer);
}

void File::querySummary(const Matcher& matcher, Summary& summary)
{
	metadata::Summarise summariser(summary);
	scan(DataQuery(matcher), summariser);
}

ArkimetFile::ArkimetFile(const ConfigFile& cfg) : IfstreamFile(cfg) {}
ArkimetFile::~ArkimetFile() {}
void ArkimetFile::scan(const dataset::DataQuery& q, metadata::Consumer& consumer)
{
	metadata::Consumer* c = &consumer;
	// Order matters here, as delete will happen in reverse order
	auto_ptr<ds::DataInliner> inliner;
	auto_ptr<sort::Stream> sorter;

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


	ds::MatcherFilter mf(q.matcher, *c);
	Metadata::readFile(*m_file, m_pathname, mf);

	if (sorter.get()) sorter->flush();
}

YamlFile::YamlFile(const ConfigFile& cfg) : IfstreamFile(cfg) {}
YamlFile::~YamlFile() {}
void YamlFile::scan(const dataset::DataQuery& q, metadata::Consumer& consumer)
{
	metadata::Consumer* c = &consumer;
	// Order matters here, as delete will happen in reverse order
	auto_ptr<ds::DataInliner> inliner;
	auto_ptr<sort::Stream> sorter;

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

	Metadata md;
	while (md.readYaml(*m_file, m_pathname))
	{
		if (!q.matcher(md))
			continue;
		(*c)(md);
	}

	if (sorter.get()) sorter->flush();
}

RawFile::RawFile(const ConfigFile& cfg) : File(cfg)
{
}

RawFile::~RawFile() {}

void RawFile::scan(const dataset::DataQuery& q, metadata::Consumer& consumer)
{
	metadata::Consumer* c = &consumer;
	auto_ptr<ds::DataInliner> inliner;
	auto_ptr<sort::Stream> sorter;

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

	ds::MatcherFilter mf(q.matcher, *c);
	scan::scan(m_pathname, mf, m_format);
}

}
}
// vim:set ts=4 sw=4:
