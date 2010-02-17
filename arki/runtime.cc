/*
 * runtime - Common code used in most arkimet executables
 *
 * Copyright (C) 2007--2010  ARPA-SIM <urpsim@smr.arpa.emr.it>
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
#include <wibble/sys/signal.h>
#include <wibble/string.h>
#include <arki/configfile.h>
#include <arki/summary.h>
#include <arki/matcher.h>
#include <arki/utils.h>
#include <arki/dataset.h>
#include <arki/dataset/file.h>
#include <arki/dataset/http.h>
#include <arki/dispatcher.h>
#include <arki/targetfile.h>
#include <arki/formatter.h>
#include <arki/postprocess.h>
#include <arki/sort.h>
#include <arki/dataset/gridspace.h>
#include <arki/nag.h>
#include <sys/types.h>
#include <sys/stat.h>
#include <fcntl.h>
#include <iostream>
#include <cstdlib>
#include "config.h"

#if HAVE_DBALLE
#include <dballe++/init.h>
#endif

#ifdef HAVE_LUA
#include <arki/report.h>
#endif

using namespace std;
using namespace wibble;
using namespace wibble::commandline;

#if HAVE_DBALLE
dballe::DballeInit dballeInit;
#endif

namespace arki {
namespace runtime {

void init()
{
	runtime::readMatcherAliasDatabase();
}

CommandLine::CommandLine(const std::string& name, int mansection)
	: StandardParserWithManpage(name, PACKAGE_VERSION, mansection, PACKAGE_BUGREPORT),
	  output(0), processor(0), dispatcher(0)
{
	infoOpts = createGroup("Options controlling verbosity");
	debug = infoOpts->add<BoolOption>("debug", 0, "debug", "", "debug output");
	verbose = infoOpts->add<BoolOption>("verbose", 0, "verbose", "", "verbose output");
	status = infoOpts->add<BoolOption>("status", 0, "status", "",
			"print to standard error a line per every file with a summary of how it was handled");

	// Used only if requested
	inputOpts = createGroup("Options controlling input data");
	cfgfiles = 0; exprfile = 0;
	files = 0; moveok = moveko = movework = 0;
	restr = 0;

	outputOpts = createGroup("Options controlling output style");
	merged = 0;
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
	targetfile = outputOpts->add<StringOption>("targetfile", 0, "targetfile", "pattern",
			"append the output to file names computed from the data"
			" to be written. See /etc/arkimet/targetfile for details.");
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
	gridspace = outputOpts->add<StringOption>("gridspace", 0, "gridspace", "file",
			"output only those items that fit in a dense, discrete"
			" matrix of metadata. The input file is parsed one"
			" line at a time. A line in the form: 'type: value'"
			" adds a metadata item to the corresponding grid"
			" dimension. A line in the form: 'match type: expr'"
			" adds the a metadata item that matches the given"
			" arkimet expression.");

	dispatchOpts = createGroup("Options controlling dispatching data to datasets");
	dispatch = dispatchOpts->add< VectorOption<String> >("dispatch", 0, "dispatch", "conffile",
			"dispatch the data to the datasets described in the "
			"given configuration file (or a dataset path can also "
			"be given), then output the metadata of the data that "
			"has been dispatched (can be specified multiple times)");
	testdispatch = dispatchOpts->add< VectorOption<String> >("testdispatch", 0, "testdispatch", "conffile",
			"simulate dispatching the files right after scanning, using the given configuration file "
			"or dataset directory (can be specified multiple times)");
}

CommandLine::~CommandLine()
{
	if (dispatcher) delete dispatcher;
	if (processor) delete processor;
	if (output) delete output;
}

void CommandLine::addScanOptions()
{
	files = inputOpts->add<StringOption>("files", 0, "files", "file",
			"read the list of files to scan from the given file instead of the command line");
	moveok = inputOpts->add<StringOption>("moveok", 0, "moveok", "directory",
			"move input files imported successfully to the given directory");
	moveko = inputOpts->add<StringOption>("moveko", 0, "moveko", "directory",
			"move input files with problems to the given directory");
	movework = inputOpts->add<StringOption>("movework", 0, "movework", "directory",
			"move input files here before opening them. This is useful to "
			"catch the cases where arki-scan crashes without having a "
			"chance to handle errors.");
}

void CommandLine::addQueryOptions()
{
	cfgfiles = inputOpts->add< VectorOption<String> >("config", 'C', "config", "file",
		"read configuration about input sources from the given file (can be given more than once)");
	exprfile = inputOpts->add<StringOption>("file", 'f', "file", "file",
		"read the expression from the given file");
	merged = outputOpts->add<BoolOption>("merged", 0, "merged", "",
		"if multiple datasets are given, merge their data and output it in"
		" reference time order.  Note: sorting does not work when using"
		" --postprocess, --data or --report");
	restr = add<StringOption>("restrict", 0, "restrict", "names",
			"restrict operations to only those datasets that allow one of the given (comma separated) names");
}

bool CommandLine::parse(int argc, const char* argv[])
{
	add(infoOpts);
	add(inputOpts);
	add(outputOpts);
	add(dispatchOpts);

	if (StandardParserWithManpage::parse(argc, argv))
		return true;

	nag::init(verbose->isSet(), debug->isSet());

	// Check conflicts among options
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
		if (dataOnly->boolValue())
			throw wibble::exception::BadOption("--postprocess conflicts with --data");
	}
	if (gridspace->isSet())
	{
		Input in(gridspace->stringValue());
		char c;
		while (true)
		{
			in.stream().get(c);
			if (in.stream().eof()) break;
			if (in.stream().bad())
				throw wibble::exception::File(gridspace->stringValue(), "reading one character");
			gridspace_def << c;
		}
	}
	
	return false;
}

struct YamlProcessor : public DatasetProcessor, public MetadataConsumer
{
        Formatter* formatter;
	Output& output;
	Summary* summary;
	sort::Compare* sorter;
	dataset::DataQuery query;

	YamlProcessor(const CommandLine& opts)
		: formatter(0), output(*opts.output), summary(0), sorter(0),
		  query(opts.query, false)
	{
		if (opts.annotate->boolValue())
			formatter = Formatter::create();

		if (opts.summary->boolValue())
			summary = new Summary();
		else if (opts.sort->boolValue())
		{
			sorter = sort::Compare::parse(opts.sort->stringValue()).release();
			query.sorter = sorter;
		}
	}

	virtual ~YamlProcessor()
	{
		if (sorter) delete sorter;
		if (summary) delete summary;
		if (formatter) delete formatter;
	}

	virtual void process(ReadonlyDataset& ds, const std::string& name)
	{
		if (summary)
			ds.querySummary(query.matcher, *summary);
		else
			ds.queryData(query, *this);
	}

	virtual bool operator()(Metadata& md)
	{
		md.writeYaml(output.stream(), formatter);
		output.stream() << endl;
		return true;
	}

	virtual void end()
	{
		if (summary)
		{
			summary->writeYaml(output.stream(), formatter);
			output.stream() << endl;
		}
	}
};

struct BinaryProcessor : public DatasetProcessor
{
	Output& out;
	sort::Compare* sorter;
	dataset::ByteQuery query;

	BinaryProcessor(const CommandLine& opts)
		: out(*opts.output), sorter(0)
	{
		if (opts.postprocess->isSet())
		{
			query.setPostprocess(opts.query, opts.postprocess->stringValue());
#ifdef HAVE_LUA
		} else if (opts.report->isSet()) {
			if (opts.summary->boolValue())
				query.setRepSummary(opts.query, opts.report->stringValue());
			else
				query.setRepMetadata(opts.query, opts.report->stringValue());
#endif
		} else
			query.setData(opts.query);
		
		if (opts.sort->isSet())
		{
			sorter = sort::Compare::parse(opts.sort->stringValue()).release();
			query.sorter = sorter;
		}
	}

	virtual ~BinaryProcessor()
	{
		if (sorter) delete sorter;
	}

	virtual void process(ReadonlyDataset& ds, const std::string& name)
	{
		// TODO: validate query's postprocessor with ds' config
		ds.queryBytes(query, out.stream());
	}
};

struct DataProcessor : public DatasetProcessor, public MetadataConsumer
{
	Output& output;
	Summary* summary;
	sort::Compare* sorter;
	dataset::DataQuery query;

	DataProcessor(const CommandLine& opts)
		: output(*opts.output), summary(0), sorter(0),
		  query(opts.query, false)
	{
		if (opts.summary->boolValue())
			summary = new Summary();
		else
		{
			query.withData = opts.dataInline->boolValue();

			if (opts.sort->boolValue())
			{
				sorter = sort::Compare::parse(opts.sort->stringValue()).release();
				query.sorter = sorter;
			}
		}
	}

	virtual ~DataProcessor()
	{
		if (sorter) delete sorter;
		if (summary) delete summary;
	}

	virtual void process(ReadonlyDataset& ds, const std::string& name)
	{
		if (summary)
			ds.querySummary(query.matcher, *summary);
		else
			ds.queryData(query, *this);
	}

	virtual bool operator()(Metadata& md)
	{
		md.write(output.stream(), output.name());
		return true;
	}

	virtual void end()
	{
		if (summary)
		{
			summary->write(output.stream(), output.name());
		}
	}
};

void CommandLine::setupProcessing()
{
	// Parse the matcher query
	if (exprfile)
	{
		if (exprfile->isSet())
		{
			// Read the entire file into memory and parse it as an expression
			query = Matcher::parse(utils::readFile(exprfile->stringValue()));
		} else {
			// Read from the first commandline argument
			if (!hasNext())
			{
				if (exprfile)
					throw wibble::exception::BadOption("you need to specify a filter expression or use --"+exprfile->longNames[0]);
				else
					throw wibble::exception::BadOption("you need to specify a filter expression");
			}
			// And parse it as an expression
			query = Matcher::parse(next());
		}
	}


	// Initialise the dataset list

	if (cfgfiles)	// From -C options, looking for config files or datasets
		for (vector<string>::const_iterator i = cfgfiles->values().begin();
				i != cfgfiles->values().end(); ++i)
			parseConfigFile(inputInfo, *i);

	if (files && files->isSet())	// From --files option, looking for data files or datasets
	{
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

		// Read the content and scan the related files or dataset directories
		string line;
		while (!input->eof())
		{
			getline(*input, line);
			if (input->fail() && !input->eof())
				throw wibble::exception::File(file, "reading one line");
			line = str::trim(line);
			if (line.empty())
				continue;
			ReadonlyDataset::readConfig(line, inputInfo);
		}
	}

	while (hasNext())	// From command line arguments, looking for data files or datasets
		ReadonlyDataset::readConfig(next(), inputInfo);

	if (inputInfo.sectionSize() == 0)
		throw wibble::exception::BadOption("you need to specify at least one input file or dataset");

	// Filter the dataset list
	if (restr && restr->isSet())
	{
		Restrict rest(restr->stringValue());
		rest.remove_unallowed(inputInfo);
		if (inputInfo.sectionSize() == 0)
			throw wibble::exception::BadOption("no accessible datasets found for the given --restrict value");
	}

	// Some things cannot be done when querying multiple datasets at the same time
	if (inputInfo.sectionSize() > 1 && !dispatcher)
	{
		if (postprocess->boolValue())
			throw wibble::exception::BadOption("postprocessing is not possible when querying more than one dataset at the same time");
		if (report->boolValue())
			throw wibble::exception::BadOption("reports are not possible when querying more than one dataset at the same time");
	}


	// Validate the query with all the servers

	if (exprfile)
	{
		string orig = query.toString();
		string wanted = query.toStringExpanded();
		if (orig != wanted)
		{
			std::set<std::string> servers;
			for (ConfigFile::const_section_iterator i = inputInfo.sectionBegin();
					i != inputInfo.sectionEnd(); ++i)
			{
				string server = i->second->value("server");
				if (!server.empty()) servers.insert(server);
			}
			bool mismatch = false;
			for (set<string>::const_iterator i = servers.begin();
					i != servers.end(); ++i)
			{
				string got;
				try {
					got = dataset::HTTP::expandMatcher(orig, *i);
				} catch (wibble::exception::Generic& e) {
					// If the server cannot expand the query, we're
					// ok as we send it expanded. What we are
					// checking here is that the server does not
					// have a different idea of the same aliases
					// that we use
					continue;
				}
				if (got != wanted)
				{
					nag::warning("Server %s expands the query as %s", i->c_str(), got.c_str());
					mismatch = true;
				}
			}
			if (mismatch)
			{
				nag::warning("Locally, we expand the query as %s", wanted.c_str());
				throw wibble::exception::Consistency("checking alias consistency", "aliases locally and on the server differ");
			}
		}
	}


	// Open output stream

	if (!output)
		output = new Output(*outfile);

	// Create the appropriate processor

	if (yaml->boolValue() || annotate->isSet())
		processor = new YamlProcessor(*this);
	else if (dataOnly->boolValue() || postprocess->isSet()
#ifdef HAVE_LUA
		|| report->isSet()
#endif
		)
		processor = new BinaryProcessor(*this);
	else
		processor = new DataProcessor(*this);


	// Create the dispatcher if needed

	if (dispatch->isSet() || testdispatch->isSet())
	{
		if (dispatch->isSet() && testdispatch->isSet())
			throw wibble::exception::BadOption("you cannot use --dispatch together with --testdispatch");
		runtime::readMatcherAliasDatabase();

		if (testdispatch->isSet()) {
			for (vector<string>::const_iterator i = testdispatch->values().begin();
					i != testdispatch->values().end(); ++i)
				parseConfigFile(dispatchInfo, *i);
			dispatcher = new MetadataDispatch(dispatchInfo, *processor, true);
		} else {
			for (vector<string>::const_iterator i = dispatch->values().begin();
					i != dispatch->values().end(); ++i)
				parseConfigFile(dispatchInfo, *i);
			dispatcher = new MetadataDispatch(dispatchInfo, *processor);
		}
	}
	if (dispatcher)
		dispatcher->reportStatus = status->boolValue();
}

void CommandLine::doneProcessing()
{
	if (processor)
		processor->end();
}

static std::string moveFile(const std::string& source, const std::string& targetdir)
{
	string targetFile = str::joinpath(targetdir, str::basename(source));
	if (rename(source.c_str(), targetFile.c_str()) == -1)
		throw wibble::exception::System("Moving " + source + " to " + targetFile);
	return targetFile;
}

static std::string moveFile(const ReadonlyDataset& ds, const std::string& targetdir)
{
	if (const dataset::File* d = dynamic_cast<const dataset::File*>(&ds))
		return moveFile(d->pathname(), targetdir);
	else
		return string();
}

std::auto_ptr<ReadonlyDataset> CommandLine::openSource(ConfigFile& info)
{
	if (movework && movework->isSet() && info.value("type") == "file")
		info.setValue("path", moveFile(info.value("path"), movework->stringValue()));

	return auto_ptr<ReadonlyDataset>(ReadonlyDataset::create(info));
}

bool CommandLine::processSource(ReadonlyDataset& ds, const std::string& name)
{
	if (dispatcher)
		return dispatcher->process(ds, name);

	ReadonlyDataset* this_source = &ds;
	auto_ptr<dataset::Gridspace> gs;
	auto_ptr<TargetfileSpy> tf;

	if (gridspace->isSet())
	{
		gridspace_def.seekg(0);
		gs.reset(new dataset::Gridspace(*this_source));
		gs->read(gridspace_def, gridspace->stringValue());
		gs->validate();
		this_source = gs.get();
	}

	if (targetfile->isSet())
	{
		tf.reset(new TargetfileSpy(*this_source, *output, targetfile->stringValue()));
		this_source = tf.get();
	}

	processor->process(*this_source, name);
	return true;
}

void CommandLine::closeSource(std::auto_ptr<ReadonlyDataset> ds, bool successful)
{
	if (successful && moveok && moveok->isSet())
	{
		moveFile(*ds, moveok->stringValue());
	}
	else if (!successful && moveko && moveko->isSet())
	{
		moveFile(*ds, moveko->stringValue());
	}
	// TODO: print status

	// ds will be automatically deallocated here
}

MetadataDispatch::MetadataDispatch(const ConfigFile& cfg, DatasetProcessor& next, bool test)
	: cfg(cfg), dispatcher(0), next(next), reportStatus(false),
	  countSuccessful(0), countDuplicates(0), countInErrorDataset(0), countNotImported(0)
{
	timerclear(&startTime);

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

bool MetadataDispatch::process(ReadonlyDataset& ds, const std::string& name)
{
	setStartTime();
	results.clear();

	dataset::DataQuery dq(Matcher(), true);
	try {
		ds.queryData(dq, *this);
	} catch (std::exception& e) {
		// FIXME: this is a quick experiment: a better message can
		// print some of the stats to document partial imports
		//cerr << i->second->value("path") << ": import FAILED: " << e.what() << endl;
		nag::warning("import FAILED: %s", e.what());
		// Still process what we've got so far
		next.process(results, name);
		throw;
	}

	// Process the resulting annotated metadata as a dataset
	next.process(results, name);

	if (reportStatus)
	{
		cerr << name << ": " << summarySoFar() << endl;
		cerr.flush();
	}

	bool success = countSuccessful != 0 && !(
		    countNotImported
		 && countDuplicates
		 && countInErrorDataset);

	flush();

	countSuccessful = 0;
	countNotImported = 0;
	countDuplicates = 0;
	countInErrorDataset = 0;

	return success;
}

bool MetadataDispatch::operator()(Metadata& md)
{
	bool problems = false;

	// Try dispatching
	switch (dispatcher->dispatch(md, results))
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
	string timeinfo;
	if (timerisset(&startTime))
	{
		struct timeval now;
		struct timeval diff;
		gettimeofday(&now, NULL);
		timersub(&now, &startTime, &diff);
		timeinfo = str::fmtf(" in %d.%06d seconds", diff.tv_sec, diff.tv_usec);
	}
	if (!countSuccessful && !countNotImported && !countDuplicates && !countInErrorDataset)
		return "no data processed" + timeinfo;

	if (!countNotImported && !countDuplicates && !countInErrorDataset)
	{
		if (countSuccessful == 1)
			return "everything ok: " + str::fmt(countSuccessful) + " message imported" + timeinfo;
		else
			return "everything ok: " + str::fmt(countSuccessful) + " messages imported" + timeinfo;
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

	res << timeinfo;

	return res.str();
}

void MetadataDispatch::setStartTime()
{
	gettimeofday(&startTime, NULL);
}


}
}
// vim:set ts=4 sw=4:
