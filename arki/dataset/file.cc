/*
 * dataset/file - Dataset on a single file
 *
 * Copyright (C) 2008,2009  ARPA-SIM <urpsim@smr.arpa.emr.it>
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

#include <arki/dataset/file.h>
#include <arki/configfile.h>
#include <arki/matcher.h>
#include <arki/summary.h>
#include <arki/postprocess.h>
#include <arki/sort.h>
#include <arki/utils/dataset.h>
#include <arki/scan/any.h>
#include <wibble/exception.h>
#include <wibble/string.h>
#include <wibble/sys/fs.h>
#include <sys/stat.h>

#include "config.h"

#ifdef HAVE_DBALLE
#include <arki/scan/bufr.h>
#endif

#ifdef HAVE_GRIBAPI
#include <arki/scan/grib.h>
#endif

#ifdef HAVE_LUA
#include <arki/report.h>
#endif

using namespace std;
using namespace wibble;
using namespace arki::utils;

namespace arki {
namespace dataset {

string normaliseFormat(const std::string& format)
{
	string f = str::tolower(format);
	if (f == "metadata") return "arkimet";
	if (f == "grib1") return "grib";
	if (f == "grib2") return "grib";
	return f;
}

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
		if (sys::fs::access(fname, F_OK))
		{
			section.setValue("path", sys::fs::abspath(fname));

			string name = str::basename(fname);

			// Extract the extension if present
			size_t epos = name.rfind('.');
			if (epos != string::npos)
				section.setValue("format", normaliseFormat(name.substr(epos+1)));
			else
				section.setValue("format", "arkimet");

			section.setValue("name", name);
		} else {
			size_t fpos = fname.find(':');
			if (fpos == string::npos)
				throw wibble::exception::Consistency("examining file " + fname, "file does not exist");
			section.setValue("format", normaliseFormat(fname.substr(0, fpos)));

			string fname1 = fname.substr(fpos+1);
			if (!sys::fs::access(fname1, F_OK))
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

	throw wibble::exception::Consistency("creating a file dataset", "unknown file format \""+format+"\"");
}

File::File(const ConfigFile& cfg)
{
	m_pathname = cfg.value("path");
}

IfstreamFile::IfstreamFile(const ConfigFile& cfg) : File(cfg)
{
	m_file.open(m_pathname.c_str(), ios::in);
	if (!m_file.is_open() || m_file.fail())
		throw wibble::exception::File(m_pathname, "opening file for reading");
}

IfstreamFile::~IfstreamFile()
{
	m_file.close();
}

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
 * Summarise all metadata
 */
struct Summariser : public MetadataConsumer
{
	Summary& summary;

	Summariser(Summary& s) : summary(s) {}

	bool operator()(Metadata& md)
	{
		summary.add(md);
		return true;
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

void File::queryData(const dataset::DataQuery& q, MetadataConsumer& consumer)
{
	scan(q, consumer);
}

void File::querySummary(const Matcher& matcher, Summary& summary)
{
	Summariser summariser(summary);
	scan(DataQuery(matcher), summariser);
}

void File::queryBytes(const dataset::ByteQuery& q, std::ostream& out)
{
	switch (q.type())
	{
		case dataset::ByteQuery::BQ_DATA: {
			DataOnly dataonly(out);
			scan(q, dataonly);
			break;
		}
		case dataset::ByteQuery::BQ_POSTPROCESS: {
			Postprocess postproc(q.param, out);
			scan(q, postproc);
			postproc.flush();
			break;
		}
		case dataset::ByteQuery::BQ_REP_METADATA: {
#ifdef HAVE_LUA
			Report rep;
			rep.captureOutput(out);
			rep.load(q.param);
			scan(q, rep);
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
			throw wibble::exception::Consistency("querying dataset", "unsupported query type: " + str::fmt((int)q.type()));
	}
}


ArkimetFile::ArkimetFile(const ConfigFile& cfg) : IfstreamFile(cfg) {}
ArkimetFile::~ArkimetFile() {}
void ArkimetFile::scan(const dataset::DataQuery& q, MetadataConsumer& consumer)
{
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

	sys::Buffer buf;
	string signature;
	unsigned version;

	Metadata md;
	while (types::readBundle(m_file, m_pathname, buf, signature, version))
	{
		if (signature == "MD" || signature == "!D")
		{
			md.read(buf, version, m_pathname);
			if (md.source->style() == types::Source::INLINE)
				md.readInlineData(m_file, m_pathname);

			// Don't consume the deleted ones
			if (signature == "!D")
				continue;

			if (!q.matcher(md))
				continue;
			
			(*c)(md);
		}
	}
}

YamlFile::YamlFile(const ConfigFile& cfg) : IfstreamFile(cfg) {}
YamlFile::~YamlFile() {}
void YamlFile::scan(const dataset::DataQuery& q, MetadataConsumer& consumer)
{
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

	Metadata md;
	while (md.readYaml(m_file, m_pathname))
	{
		if (!q.matcher(md))
			continue;
		(*c)(md);
	}
}

RawFile::RawFile(const ConfigFile& cfg) : File(cfg)
{
}

RawFile::~RawFile() {}

void RawFile::scan(const dataset::DataQuery& q, MetadataConsumer& consumer)
{
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

	scan::scan(m_pathname, *c);
}

}
}
// vim:set ts=4 sw=4:
