#include "arki/libconfig.h"
#include "arki/dataset/file.h"
#include "arki/metadata/consumer.h"
#include "arki/configfile.h"
#include "arki/matcher.h"
#include "arki/summary.h"
#include "arki/postprocess.h"
#include "arki/sort.h"
#include "arki/utils/dataset.h"
#include "arki/utils/files.h"
#include "arki/scan/any.h"
#include "arki/utils/string.h"
#include "arki/utils/sys.h"
#include "arki/wibble/exception.h"
#include <sys/stat.h>
#include <iostream>

#ifdef HAVE_LUA
#include <arki/report.h>
#endif

using namespace std;
using namespace arki::utils;

namespace arki {
namespace dataset {

void File::readConfig(const std::string& fname, ConfigFile& cfg)
{
    ConfigFile section;

    if (fname == "-")
    {
        // If the input is stdin, make hardcoded assumptions
        section.setValue("name", "stdin");
        section.setValue("path", "-");
        section.setValue("type", "file");
        section.setValue("format", "arkimet");
    } else if (str::endswith(fname, ":-")) {
        // If the input is stdin, make hardcoded assumptions
        section.setValue("name", "stdin");
        section.setValue("path", "-");
        section.setValue("type", "file");
        section.setValue("format", fname.substr(0, fname.size()-2));
    } else {
        section.setValue("type", "file");
        if (sys::exists(fname))
        {
            section.setValue("path", sys::abspath(fname));
            section.setValue("format", files::format_from_ext(fname, "arkimet"));
            string name = str::basename(fname);
            section.setValue("name", name);
        } else {
            size_t fpos = fname.find(':');
            if (fpos == string::npos)
            {
                stringstream ss;
                ss << "dataset file " << fname << " does not exist";
                throw runtime_error(ss.str());
            }
            section.setValue("format", files::normaliseFormat(fname.substr(0, fpos)));

            string fname1 = fname.substr(fpos+1);
            if (!sys::exists(fname1))
            {
                stringstream ss;
                ss << "dataset file " << fname1 << " does not exist";
                throw runtime_error(ss.str());
            }
            section.setValue("path", sys::abspath(fname1));
            section.setValue("name", str::basename(fname1));
        }
    }

    // Merge into cfg
    cfg.mergeInto(section.value("name"), section);
}

File* File::create(const ConfigFile& cfg)
{
    string format = str::lower(cfg.value("format"));

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
#ifdef HAVE_HDF5
    if (format == "odimh5")
        return new RawFile(cfg);
#endif
#ifdef HAVE_VM2
    if (format == "vm2")
        return new RawFile(cfg);
#endif

    stringstream ss;
    ss << "cannot create a dataset for the unknown file format \"" << format << "\"";
    throw runtime_error(ss.str());
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

void File::queryData(const dataset::DataQuery& q, metadata::Eater& consumer)
{
	scan(q, consumer);
}

void File::querySummary(const Matcher& matcher, Summary& summary)
{
    metadata::SummarisingEater summariser(summary);
    scan(DataQuery(matcher), summariser);
}

ArkimetFile::ArkimetFile(const ConfigFile& cfg) : IfstreamFile(cfg) {}
ArkimetFile::~ArkimetFile() {}
void ArkimetFile::scan(const dataset::DataQuery& q, metadata::Eater& consumer)
{
	metadata::Eater* c = &consumer;
	// Order matters here, as delete will happen in reverse order
	unique_ptr<sort::Stream> sorter;

	if (q.sorter)
	{
		sorter.reset(new sort::Stream(*q.sorter, *c));
		c = sorter.get();
	}


    metadata::FilteredEater mf(q.matcher, *c);
    Metadata::readFile(*m_file, m_pathname, mf);

	if (sorter.get()) sorter->flush();
}

YamlFile::YamlFile(const ConfigFile& cfg) : IfstreamFile(cfg) {}
YamlFile::~YamlFile() {}
void YamlFile::scan(const dataset::DataQuery& q, metadata::Eater& consumer)
{
	metadata::Eater* c = &consumer;
	// Order matters here, as delete will happen in reverse order
	unique_ptr<sort::Stream> sorter;

	if (q.sorter)
	{
		sorter.reset(new sort::Stream(*q.sorter, *c));
		c = sorter.get();
	}

    while (true)
    {
        unique_ptr<Metadata> md(new Metadata);
        if (!md->readYaml(*m_file, m_pathname))
            break;
        if (!q.matcher(*md))
            continue;
        c->eat(move(md));
    }

	if (sorter.get()) sorter->flush();
}

RawFile::RawFile(const ConfigFile& cfg) : File(cfg)
{
}

RawFile::~RawFile() {}

void RawFile::scan(const dataset::DataQuery& q, metadata::Eater& consumer)
{
	metadata::Eater* c = &consumer;
	unique_ptr<sort::Stream> sorter;

	if (q.sorter)
	{
		sorter.reset(new sort::Stream(*q.sorter, *c));
		c = sorter.get();
	}

    metadata::FilteredEater mf(q.matcher, *c);
	scan::scan(m_pathname, mf, m_format);
}

}
}
