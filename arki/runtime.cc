/*
 * runtime - Common code used in most xgribarch executables
 *
 * Copyright (C) 2007,2008  ARPA-SIM <urpsim@smr.arpa.emr.it>
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

#include <arki/runtime.h>

#include <wibble/exception.h>
#include <wibble/sys/fs.h>
#include <wibble/string.h>
#include <arki/configfile.h>
#include <arki/summary.h>
#include <arki/matcher.h>
#include <arki/utils.h>
#include <arki/dataset.h>
#include <arki/dispatcher.h>
#include <arki/formatter.h>
#include <arki/postprocess.h>
#include <arki/sort.h>
#include <sys/types.h>
#include <sys/stat.h>
#include <fcntl.h>
#include <iostream>
#include <cstdlib>
#include "config.h"


#ifdef HAVE_LUA
#include <arki/report.h>
#endif

using namespace std;
using namespace wibble;
using namespace wibble::commandline;

namespace arki {
namespace runtime {

FlushableMetadataConsumer::FlushableMetadataConsumer()
	: countMetadata(0), countSummary(0)
{
}
FlushableMetadataConsumer::~FlushableMetadataConsumer()
{
}

void FlushableMetadataConsumer::flush()
{
	// By default, do nothing
}

/**
 * Raw output of metadata
 */
struct MetadataOutput : public FlushableMetadataConsumer
{
	Output& output;

	MetadataOutput(Output& output) : output(output) {}

	virtual bool outputSummary(Summary& s)
	{
		++countSummary;
		s.write(output.stream(), output.name());
		return true;
	}
	bool operator()(Metadata& md)
	{
		++countMetadata;
		md.write(output.stream(), output.name());
		return true;
	}
};

/**
 * Raw output of metadata
 */
struct MetadataInlineOutput : public FlushableMetadataConsumer
{
	Output& output;

	MetadataInlineOutput(Output& output) : output(output) {}

	virtual bool outputSummary(Summary& s)
	{
		// Can't print summaries here
		++countSummary;
		return true;
	}
	bool operator()(Metadata& md)
	{
		// Read the data
		wibble::sys::Buffer buf = md.getData();
		// Change the source as inline
		md.setInlineData(md.source->format, buf);
		md.write(output.stream(), output.name());
		++countMetadata;
		return true;
	}
};

/**
 * Raw output of metadata
 */
struct DataOutput : public FlushableMetadataConsumer
{
	Output& output;

	DataOutput(Output& output) : output(output) {}

	virtual bool outputSummary(Summary& s)
	{
		// Can't print summaries here
		++countSummary;
		return true;
	}
	bool operator()(Metadata& md)
	{
		// Read the data
		wibble::sys::Buffer buf = md.getData();
		// Write the data only
		output.stream().write((const char*)buf.data(), buf.size());
		++countMetadata;
		return true;
	}
};

/**
 * Raw output of metadata
 */
struct PostprocessedDataOutput : public FlushableMetadataConsumer
{
	Output& output;
	Postprocess postproc;

	// FIXME: hardcoded to stdout until we find a way to get a proper fd out of 'output'
	PostprocessedDataOutput(Output& output, const std::string& command)
		: output(output), postproc(command, 1) {}

	PostprocessedDataOutput(Output& output, const std::string& command, const ConfigFile& cfg)
		: output(output), postproc(command, 1, cfg) {}

	virtual bool outputSummary(Summary& s)
	{
		// Can't print summaries here
		++countSummary;
		return true;
	}
	bool operator()(Metadata& md)
	{
		if (!postproc(md))
			return false;
		++countMetadata;
		return true;
	}
	virtual void flush()
	{
		postproc.flush();
	}
};

/**
 * Yaml output of metadata
 */
class MetadataYamlOutput : public FlushableMetadataConsumer
{
protected:
	Output& output;
	const Formatter* formatter;

public:
	MetadataYamlOutput(Output& output, const Formatter* formatter = 0)
		: output(output), formatter(formatter) {}

	virtual bool outputSummary(Summary& s)
	{
		s.writeYaml(output.stream(), formatter);
		output.stream() << endl;
		++countSummary;
		return true;
	}
	virtual bool operator()(Metadata& md)
	{
		md.writeYaml(output.stream(), formatter);
		output.stream() << endl;
		++countMetadata;
		return true;
	}
};

struct MetadataSummarise : public FlushableMetadataConsumer
{
	FlushableMetadataConsumer* chained;
	Summary summary;

	MetadataSummarise(FlushableMetadataConsumer* chained) : chained(chained) {}
	virtual ~MetadataSummarise()
	{
		if (chained) delete chained;
	}

	virtual bool operator()(Metadata& md)
	{
		summary.add(md);
		++countMetadata;
		return true;
	}
	virtual bool outputSummary(Summary& s)
	{
		summary.add(s);
		++countSummary;
		return true;
	}
	virtual void flush()
	{
		chained->outputSummary(summary);
		chained->flush();
	}
};

#ifdef HAVE_LUA
/**
 * Use Lua to produce a report of a metadata/summary stream
 */
struct MetadataReport : public FlushableMetadataConsumer
{
	Output& output;
	Report rep;

	MetadataReport(Output& output, const std::string name)
		: output(output)
	{
		string filename;

		// Otherwise the file given in the environment variable ARKI_ALIASES is tried.
		char* dir = getenv("ARKI_REPORT");
		if (dir)
			filename = str::joinpath(dir, name);
		else
			filename = str::joinpath(str::joinpath(CONF_DIR, "report"), name);

		rep.captureOutput(output.stream());
		rep.loadFile(filename);
	}

	virtual bool operator()(Metadata& md)
	{
		if (!rep(md))
			return false;
		++countMetadata;
		return true;
	}
	virtual bool outputSummary(Summary& s)
	{
		if (!rep(s))
			return false;
		++countSummary;
		return true;
	}
	virtual void flush()
	{
		rep.report();
	}
};
#endif


OutputOptions::OutputOptions(const std::string& name, int mansection, ConfigType cfgtype)
	: StandardParserWithManpage(name, PACKAGE_VERSION, mansection, PACKAGE_BUGREPORT),
	  cfgfiles(0), cfgtype(cfgtype),
	  m_formatter(0), m_output(0), m_consumer(0)
{
	OptionGroup* outputOpts = createGroup("Options controlling output style");
	
	yaml = outputOpts->add<BoolOption>("yaml", 0, "yaml", "",
			"dump the metadata as human-readable Yaml records");
	yaml->longNames.push_back("dump");
	annotate = outputOpts->add<BoolOption>("annotate", 0, "annotate", "",
			"annotate the human-readable Yaml output with field descriptions");
	dataInline = outputOpts->add<BoolOption>("inline", 0, "inline", "",
			"output the binary metadata together with the data (pipe through "
			" arki-dump or arki-grep to estract the data afterwards)");
	dataOnly = outputOpts->add<BoolOption>("data", 0, "data", "",
			"output only the data");
	postprocess = outputOpts->add<StringOption>("postproc", 'p', "postproc", "command",
			"output only the data, postprocessed with the given filter");
#ifdef HAVE_LUA
	report = outputOpts->add<StringOption>("report", 0, "report", "name",
			"produce the given report with the command output");
#endif
	outfile = outputOpts->add<StringOption>("output", 'o', "output", "file",
			"write the output to the given file instead of standard output");
	summary = outputOpts->add<BoolOption>("summary", 0, "summary", "",
			"output only the summary of the data");
	sort = outputOpts->add<StringOption>("sort", 0, "sort", "period:order",
			"sort order.  Period can be year, month, day, hour or minute."
			" Order can be a list of one or more metadata"
			" names (as seen in --yaml field names), with a '-'"
			" prefix to indicate reverse order.  For example,"
			" 'day:origin, -timerange, reftime' orders one day at a time"
			" by origin first, then by reverse timerange, then by reftime."
			" Default: do not sort");
	add(outputOpts);

	if (cfgtype != NONE)
		cfgfiles = add< VectorOption<String> >("config", 'C', "config", "file",
			"read the configuration from the given file (can be given more than once)");
}

OutputOptions::~OutputOptions()
{
	if (m_consumer)
		delete m_consumer;
	if (m_formatter)
		delete m_formatter;
	if (m_output)
		delete m_output;
}

bool OutputOptions::isBinaryMetadataOutput() const
{
	return dynamic_cast<MetadataOutput*>(m_consumer) != 0;
}

bool OutputOptions::parse(int argc, const char* argv[])
{
	if (StandardParserWithManpage::parse(argc, argv))
		return true;

	// Parse config files if requested
	if (cfgfiles)
	{
		bool found = runtime::parseConfigFiles(cfg, *cfgfiles);
		if (cfgtype == PARMS)
		{
			parseLeadingParameters();
			found = runtime::parseConfigFiles(cfg, *this) || found;
		}
		if (!found)
			throw wibble::exception::BadOption("you need to specify at least one dataset or configuration file");
	}

	// Option conflict checks
#ifdef HAVE_LUA
	if (report->isSet())
	{
		if (yaml->boolValue())
			throw wibble::exception::BadOption("--dump/--yaml conflicts with --report");
		if (annotate->boolValue())
			throw wibble::exception::BadOption("--annotate conflicts with --report");
		//if (summary->boolValue())
		//	throw wibble::exception::BadOption("--summary conflicts with --report");
		if (dataInline->boolValue())
			throw wibble::exception::BadOption("--inline conflicts with --report");
		if (postprocess->isSet())
			throw wibble::exception::BadOption("--postprocess conflicts with --report");
		if (sort->isSet())
			throw wibble::exception::BadOption("--sort conflicts with --report");
	}
#endif

	if (yaml->boolValue())
	{
		if (dataInline->boolValue())
			throw wibble::exception::BadOption("--dump/--yaml conflicts with --inline");
		if (dataOnly->boolValue() || postprocess->isSet())
			throw wibble::exception::BadOption("--dump/--yaml conflicts with --data or --postprocess");
		if (postprocess->isSet())
			throw wibble::exception::BadOption("--dump/--yaml conflicts with --postprocess");
	}
	if (annotate->boolValue())
	{
		if (dataInline->boolValue())
			throw wibble::exception::BadOption("--annotate conflicts with --inline");
		if (dataOnly->boolValue() || postprocess->isSet())
			throw wibble::exception::BadOption("--annotate conflicts with --data or --postprocess");
		if (postprocess->isSet())
			throw wibble::exception::BadOption("--annotate conflicts with --postprocess");
	}
	if (summary->boolValue())
	{
		if (dataInline->boolValue())
			throw wibble::exception::BadOption("--summary conflicts with --inline");
		if (dataOnly->boolValue() || postprocess->isSet())
			throw wibble::exception::BadOption("--summary conflicts with --data or --postprocess");
		if (postprocess->isSet())
			throw wibble::exception::BadOption("--summary conflicts with --postprocess");
		if (sort->isSet())
			throw wibble::exception::BadOption("--summary conflicts with --sort");
	}
	if (dataInline->boolValue())
	{
		if (dataOnly->boolValue() || postprocess->isSet())
			throw wibble::exception::BadOption("--inline conflicts with --data or --postprocess");
		if (postprocess->isSet())
			throw wibble::exception::BadOption("--inline conflicts with --postprocess");
	}
	if (postprocess->isSet())
	{
		if (sort->isSet())
			throw wibble::exception::BadOption("--sort conflicts with --postprocess");
	}

	if (annotate->boolValue())
		m_formatter = Formatter::create();

	return false;
}

void OutputOptions::createConsumer()
{
	if (!m_output)
		m_output = new Output(*outfile);
	
	// Create the consumer that we want
	if (yaml->boolValue())
		// Output the Yaml metadata
		m_consumer = new MetadataYamlOutput(*m_output, m_formatter);
	else if (dataInline->boolValue())
		// Output inline metadata
		m_consumer = new MetadataInlineOutput(*m_output);
	else if (dataOnly->boolValue() || postprocess->isSet())
		// Output data only
		if (postprocess->isSet())
			if (cfgtype != NONE)
				m_consumer = new PostprocessedDataOutput(*m_output, postprocess->stringValue(), cfg);
			else
				m_consumer = new PostprocessedDataOutput(*m_output, postprocess->stringValue());
		else
			m_consumer = new DataOutput(*m_output);
#ifdef HAVE_LUA
	else if (report->isSet())
		m_consumer = new MetadataReport(*m_output, report->stringValue());
#endif
	else
		// Output metadata or summaries only
		m_consumer = new MetadataOutput(*m_output);

	// If a summary is requested, prepend the summariser
	if (summary->boolValue())
		m_consumer = new MetadataSummarise(m_consumer);
}

void OutputOptions::processDataset(ReadonlyDataset& ds, const Matcher& m)
{
	if (!m_output)
		m_output = new Output(*outfile);

	// Perform the appropriate query to the dataset
	if (yaml->boolValue() || annotate->isSet())
	{
		// Output the Yaml metadata
		MetadataYamlOutput consumer(*m_output, m_formatter);

		// If a summary is requested, prepend the summariser
		if (summary->boolValue())
		{
			Summary s;
			ds.querySummary(m, s);
			consumer.outputSummary(s);
		}
		else if (sort->isSet())
		{
			sort::TimeIntervalSorter sorter(consumer, sort->stringValue());
			ds.queryMetadata(m, false, sorter);
			sorter.flush();
		}
		else
			ds.queryMetadata(m, false, consumer);
		consumer.flush();
	}
	else if (dataInline->boolValue())
	{
		MetadataOutput consumer(*m_output);
		if (sort->isSet())
		{
			sort::TimeIntervalSorter sorter(consumer, sort->stringValue());
			ds.queryMetadata(m, true, sorter);
			sorter.flush();
		} else
			ds.queryMetadata(m, true, consumer);
		consumer.flush();
	}
	else if (dataOnly->boolValue() || postprocess->isSet()
#ifdef HAVE_LUA
		|| report->isSet()
#endif
		)
	{
		ReadonlyDataset::ByteQuery qtype = ReadonlyDataset::BQ_DATA;
		string param;

		if (postprocess->isSet())
		{
			qtype = ReadonlyDataset::BQ_POSTPROCESS;
			param = postprocess->stringValue();
			/*
			if (cfgtype != NONE)
				m_consumer = new PostprocessedDataOutput(*m_output, postprocess->stringValue(), cfg);
			else
				m_consumer = new PostprocessedDataOutput(*m_output, postprocess->stringValue());
			*/
			ds.queryBytes(m, output().stream(), qtype, param);
#ifdef HAVE_LUA
		} else if (report->isSet()) {
			if (summary->boolValue())
				qtype = ReadonlyDataset::BQ_REP_SUMMARY;
			else
				qtype = ReadonlyDataset::BQ_REP_METADATA;
			param = report->stringValue();
			ds.queryBytes(m, output().stream(), qtype, param);
#endif
		} else if (sort->isSet()) {
			DataOutput consumer(output());
			sort::TimeIntervalSorter sorter(consumer, sort->stringValue());
			ds.queryMetadata(m, true, sorter);
			sorter.flush();
			consumer.flush();
		} else {
			ds.queryBytes(m, output().stream(), qtype, param);
		}
		
	}
	else if (summary->boolValue())
	{
		MetadataOutput consumer(*m_output);
		Summary s;
		ds.querySummary(m, s);
		consumer.outputSummary(s);
	}
	else if (sort->isSet())
	{
		MetadataOutput consumer(*m_output);
		sort::TimeIntervalSorter sorter(consumer, sort->stringValue());
		ds.queryMetadata(m, false, sorter);
		sorter.flush();
	}
	else
	{
		MetadataOutput consumer(*m_output);
		ds.queryMetadata(m, false, consumer);
	}
}

ScanOptions::ScanOptions(const std::string& name, int mansection)
	: OutputOptions(name, mansection), cfg(0), mdispatch(0)
{
	files = add<StringOption>("files", 0, "files", "file",
			"read the list of files to scan from the given file instead of the command line");
	dispatch = add< VectorOption<String> >("dispatch", 0, "dispatch", "conffile",
			"dispatch the files right after scanning, using the given configuration file "
			"or dataset directory (can be specified multiple times)");
	testdispatch = add< VectorOption<String> >("testdispatch", 0, "testdispatch", "conffile",
			"simulate dispatching the files right after scanning, using the given configuration file "
			"or dataset directory (can be specified multiple times)");
	verbose = add<BoolOption>("verbose", 0, "verbose", "",
			"print to standard error a line per every file with a summary of how it was handled");
}

ScanOptions::~ScanOptions()
{
	if (mdispatch) delete mdispatch;
	if (cfg) delete cfg;
}

MetadataConsumer& ScanOptions::consumer()
{
	if (mdispatch)
		return *mdispatch;
	else
		return OutputOptions::consumer();
}

void ScanOptions::flush()
{
	if (mdispatch)
		mdispatch->flush();
	else
		OutputOptions::consumer().flush();
}

bool ScanOptions::parse(int argc, const char* argv[])
{
	if (OutputOptions::parse(argc, argv))
		return true;
	
	if (dispatch->isSet() || testdispatch->isSet())
	{
		if (dispatch->isSet() && testdispatch->isSet())
			throw wibble::exception::BadOption("you cannot use --dispatch together with --testdispatch");
		runtime::readMatcherAliasDatabase();
		cfg = new ConfigFile;

		if (testdispatch->isSet()) {
			if (!runtime::parseConfigFiles(*cfg, *testdispatch))
				throw wibble::exception::BadOption("you need to specify the config file");
			mdispatch = new MetadataDispatch(*cfg, OutputOptions::consumer(), true);
		} else {
			if (!runtime::parseConfigFiles(*cfg, *dispatch))
				throw wibble::exception::BadOption("you need to specify the config file");
			mdispatch = new MetadataDispatch(*cfg, OutputOptions::consumer());
		}
	}
	
	// Handle --files, appending its contents to the arg list
	// and complaining if the arg list is not empty
	if (files->isSet())
	{
		if (!m_args.empty())
			throw wibble::exception::BadOption("input files in the command line are not allowed if you use --files");

		// Open the file
		string file = files->stringValue();
		std::istream* input;
		std::ifstream in;
		if (file != "-")
		{
			in.open(file.c_str(), ios::in);
			if (!in.is_open() || in.fail())
				throw wibble::exception::File(file, "opening file for reading");
			input = &in;
		} else {
			input = &cin;
		}

		// Read the content and append to m_args
		string line;
		while (!input->eof())
		{
			getline(*input, line);
			if (input->fail() && !input->eof())
				throw wibble::exception::File(file, "reading one line");
			line = str::trim(line);
			if (line.empty())
				continue;
			m_args.push_back(line);
		}
	}

	return false;
}


Input::Input(wibble::commandline::Parser& opts)
	: m_in(&cin), m_name("(stdin)")
{
	if (opts.hasNext())
	{
		string file = opts.next();
		if (file != "-")
		{
			m_file_in.open(file.c_str(), ios::in);
			if (!m_file_in.is_open() || m_file_in.fail())
				throw wibble::exception::File(file, "opening file for reading");
			m_in = &m_file_in;
			m_name = file;
		}
	}
}

Input::Input(const std::string& file)
	: m_in(&cin), m_name("(stdin)")
{
	if (file != "-")
	{
		m_file_in.open(file.c_str(), ios::in);
		if (!m_file_in.is_open() || m_file_in.fail())
			throw wibble::exception::File(file, "opening file for reading");
		m_in = &m_file_in;
		m_name = file;
	}
}

Output::Output(const std::string& fileName)
{
	openFile(fileName);
}

Output::Output(wibble::commandline::Parser& opts) : m_out(0)
{
	if (opts.hasNext())
	{
		string file = opts.next();
		if (file != "-")
			openFile(file);
	}
	if (!m_out)
		openStdout();
}

Output::Output(wibble::commandline::StringOption& opt) : m_out(0)
{
	if (opt.isSet())
	{
		string file = opt.value();
		if (file != "-")
			openFile(file);
	}
	if (!m_out)
		openStdout();
}

Output::~Output()
{
	if (m_out) delete m_out;
}

void Output::openStdout()
{
	m_name = "(stdout)";
	posixBuf.attach(1);
	m_out = new ostream(&posixBuf);
}

void Output::openFile(const std::string& fname)
{
	int fd = open(fname.c_str(), O_WRONLY | O_CREAT, 0666);
	if (fd == -1)
		throw wibble::exception::File(fname, "opening file for writing");
	m_name = fname;
	posixBuf.attach(fd);
	m_out = new ostream(&posixBuf);
}

void parseConfigFile(ConfigFile& cfg, const std::string& fileName)
{
	using namespace wibble;

	if (fileName == "-")
	{
		// Parse the config file from stdin
		cfg.parse(cin, fileName);
		return;
	}

	// Remove trailing slashes, if any
	string fname = fileName;
	while (!fname.empty() && fname[fname.size() - 1] == '/')
		fname.resize(fname.size() - 1);

	// Check if it's a file or a directory
	std::auto_ptr<struct stat> st = sys::fs::stat(fname);
	if (st.get() == 0)
		throw wibble::exception::Consistency("reading configuration from " + fname, fname + " does not exist");
	if (S_ISDIR(st->st_mode))
	{
		// If it's a directory, merge in its config file
		string name = str::basename(fname);
		string file = str::joinpath(fname, "config");

		ConfigFile section;
		ifstream in;
		in.open(file.c_str(), ios::in);
		if (!in.is_open() || in.fail())
			throw wibble::exception::File(file, "opening config file for reading");
		// Parse the config file into a new section
		section.parse(in, file);
		// Fill in missing bits
		section.setValue("name", name);
		section.setValue("path", sys::fs::abspath(fname));
		// Merge into cfg
		cfg.mergeInto(name, section);
	} else {
		// If it's a file, then it's a merged config file
		ifstream in;
		in.open(fname.c_str(), ios::in);
		if (!in.is_open() || in.fail())
			throw wibble::exception::File(fname, "opening config file for reading");
		// Parse the config file
		cfg.parse(in, fname);
	}
}

bool parseConfigFiles(ConfigFile& cfg, const wibble::commandline::VectorOption<wibble::commandline::String>& files)
{
	bool found = false;
	for (vector<string>::const_iterator i = files.values().begin();
			i != files.values().end(); ++i)
	{
		parseConfigFile(cfg, *i);
		//ReadonlyDataset::readConfig(*i, cfg);
		found = true;
	}
	return found;
}

bool parseConfigFiles(ConfigFile& cfg, wibble::commandline::Parser& opts)
{
	bool found = false;
	while (opts.hasNext())
	{
		ReadonlyDataset::readConfig(opts.next(), cfg);
		found = true;
	}
	return found;
}

void readMatcherAliasDatabase(wibble::commandline::StringOption* file)
{
	ConfigFile cfg;

	// The file named in the given StringOption (if any) is tried first.
	if (file && file->isSet())
	{
		parseConfigFile(cfg, file->stringValue());
		MatcherAliasDatabase::addGlobal(cfg);
		return;
	}

	// Otherwise the file given in the environment variable ARKI_ALIASES is tried.
	char* fromEnv = getenv("ARKI_ALIASES");
	if (fromEnv)
	{
		parseConfigFile(cfg, fromEnv);
		MatcherAliasDatabase::addGlobal(cfg);
		return;
	}

#ifdef CONF_DIR
	// Else, CONF_DIR is tried.
	string name = string(CONF_DIR) + "/match-alias.conf";
	auto_ptr<struct stat> st = wibble::sys::fs::stat(name);
	if (st.get())
	{
		parseConfigFile(cfg, name);
		MatcherAliasDatabase::addGlobal(cfg);
		return;
	}
#endif

	// Else, nothing is loaded.
}

std::vector<std::string> rcFiles(const std::string& nameInConfdir, const std::string& nameInEnv, wibble::commandline::StringOption* dirOption)
{
	std::string dirname;
	char* fromEnv = 0;

	if (dirOption && dirOption->isSet())
	{
		// The directory named in the given StringOption (if any) is tried first.
		dirname = dirOption->stringValue();
	}
	
	else if ((fromEnv = getenv(nameInEnv.c_str())))
	{
		// Otherwise the directory given in the environment variable nameInEnv is tried.
		dirname = fromEnv;
	}
#ifdef CONF_DIR
	// Else, CONF_DIR is tried.
	else
		dirname = string(CONF_DIR) + "/" + nameInConfdir;
#else
	// Or if there is no config, we fail to read anything
	else
		return string();
#endif

	vector<string> files;
	wibble::sys::fs::Directory dir(dirname);
	for (wibble::sys::fs::Directory::const_iterator i = dir.begin();
			i != dir.end(); ++i)
	{
		string file = *i;
		// Skip hidden files
		if (file[0] == '.') continue;
		// Skip backup files
		if (file[file.size() - 1] == '~') continue;
		// Skip non-files
		if (i->d_type == DT_UNKNOWN)
		{
			std::auto_ptr<struct stat> st = wibble::sys::fs::stat(dirname + "/" + file);
			if (!S_ISREG(st->st_mode)) continue;
		} else if (i->d_type != DT_REG)
			continue;
		files.push_back(wibble::str::joinpath(dirname, *i));
	}

	// Sort the file names
	std::sort(files.begin(), files.end());

	return files;
}

std::string readRcDir(const std::string& nameInConfdir, const std::string& nameInEnv, wibble::commandline::StringOption* dirOption)
{
	vector<string> files = rcFiles(nameInConfdir, nameInEnv, dirOption);

	// Read all the contents
	string res;
	for (vector<string>::const_iterator i = files.begin();
			i != files.end(); ++i)
		res += utils::readFile(*i);
	return res;
}

SourceCode readSourceFromRcDir(const std::string& nameInConfdir, const std::string& nameInEnv, wibble::commandline::StringOption* dirOption)
{
	vector<string> files = rcFiles(nameInConfdir, nameInEnv, dirOption);
	SourceCode res;

	// Read all the contents
	for (vector<string>::const_iterator i = files.begin();
			i != files.end(); ++i)
	{
		string tmp = utils::readFile(*i);
		res.push_back(FileInfo(*i, tmp.size()));
		res.code += tmp;
	}
	return res;
}

void readQuery(Matcher& expr, wibble::commandline::Parser& opts, wibble::commandline::StringOption* fromFile)
{
	if (fromFile && fromFile->isSet())
	{
		// Read the entire file into memory and parse it as an expression
		expr = Matcher::parse(utils::readFile(fromFile->stringValue()));
	} else {
		// Read from the first commandline argument
		if (!opts.hasNext())
		{
			if (fromFile)
				throw wibble::exception::BadOption("you need to specify a filter expression or use --"+fromFile->longNames[0]);
			else
				throw wibble::exception::BadOption("you need to specify a filter expression");
		}
		// And parse it as an expression
		expr = Matcher::parse(opts.next());
	}
}

MetadataDispatch::MetadataDispatch(const ConfigFile& cfg, MetadataConsumer& next, bool test)
	: cfg(cfg), dispatcher(0), next(next),
	  countSuccessful(0), countDuplicates(0), countInErrorDataset(0), countNotImported(0)
{
	if (test)
		dispatcher = new TestDispatcher(cfg, cerr);
	else
		dispatcher = new RealDispatcher(cfg);
}

MetadataDispatch::~MetadataDispatch()
{
	if (dispatcher)
		delete dispatcher;
}

bool MetadataDispatch::operator()(Metadata& md)
{
	bool problems = false;

	// Try dispatching
	switch (dispatcher->dispatch(md, next))
	{
		case Dispatcher::DISP_OK:
			++countSuccessful;
			break;
		case Dispatcher::DISP_DUPLICATE_ERROR:
			++countDuplicates;
			problems = true;
			break;
		case Dispatcher::DISP_ERROR:
			++countInErrorDataset;
			problems = true;
			break;
		case Dispatcher::DISP_NOTWRITTEN:
			// If dispatching failed, add a big note about it.
			// Analising the notes in the output should be enough to catch this
			// even happening.
			md.notes.push_back(types::Note::create("WARNING: The data has not been imported in ANY dataset"));
			++countNotImported;
			problems = true;
			break;
	}

	return dispatcher->canContinue();
}

void MetadataDispatch::flush()
{
	if (dispatcher) dispatcher->flush();
}

string MetadataDispatch::summarySoFar() const
{
	if (!countSuccessful && !countNotImported && !countDuplicates && !countInErrorDataset)
		return "no data processed";

	if (!countNotImported && !countDuplicates && !countInErrorDataset)
	{
		if (countSuccessful == 1)
			return "everything ok: " + str::fmt(countSuccessful) + " message imported";
		else
			return "everything ok: " + str::fmt(countSuccessful) + " messages imported";
	}

	stringstream res;

	if (countNotImported)
		res << "serious problems: ";
	else
		res << "some problems: ";

	res << countSuccessful << " ok, "
		<< countDuplicates << " duplicates, "
	    << countInErrorDataset << " in error dataset";

	if (countNotImported)
		res << ", " << countNotImported << " NOT imported";

	return res.str();
}


}
}
// vim:set ts=4 sw=4:
