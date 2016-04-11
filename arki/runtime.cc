#include "config.h"
#include "arki/runtime.h"
#include "arki/exceptions.h"
#include "arki/utils/string.h"
#include "arki/configfile.h"
#include "arki/summary.h"
#include "arki/matcher.h"
#include "arki/utils.h"
#include "arki/utils/files.h"
#include "arki/utils/string.h"
#include "arki/dataset.h"
#include "arki/dataset/file.h"
#include "arki/dataset/http.h"
#include "arki/dataset/index/base.h"
#include "arki/dispatcher.h"
#include "arki/targetfile.h"
#include "arki/formatter.h"
#include "arki/postprocess.h"
#include "arki/querymacro.h"
#include "arki/sort.h"
#include "arki/nag.h"
#include "arki/iotrace.h"
#include "arki/types-init.h"
#include "arki/validator.h"
#include "arki/utils/sys.h"
#ifdef HAVE_LUA
#include "arki/report.h"
#endif
#include <sys/types.h>
#include <sys/stat.h>
#include <fcntl.h>
#include <iostream>
#include <cstdlib>
#include <cassert>

#if __xlC__
// From glibc
#define timersub(a, b, result)                                                \
  do {                                                                        \
    (result)->tv_sec = (a)->tv_sec - (b)->tv_sec;                             \
    (result)->tv_usec = (a)->tv_usec - (b)->tv_usec;                          \
    if ((result)->tv_usec < 0) {                                              \
      --(result)->tv_sec;                                                     \
      (result)->tv_usec += 1000000;                                           \
    }                                                                         \
  } while (0)
#endif

using namespace std;
using namespace arki::utils;
using namespace arki::types;

namespace arki {
namespace runtime {

void init()
{
    types::init_default_types();
    runtime::readMatcherAliasDatabase();
    iotrace::init();
}

HandledByCommandLineParser::HandledByCommandLineParser(int status) : status(status) {}
HandledByCommandLineParser::~HandledByCommandLineParser() {}

std::unique_ptr<dataset::Reader> make_qmacro_dataset(const ConfigFile& ds_cfg, const ConfigFile& dispatch_cfg, const std::string& qmacroname, const std::string& query, const std::string& url)
{
    unique_ptr<dataset::Reader> ds;
    string baseurl = dataset::HTTP::allSameRemoteServer(dispatch_cfg);
    if (baseurl.empty())
    {
        // Create the local query macro
        nag::verbose("Running query macro %s on local datasets", qmacroname.c_str());
        ds.reset(new Querymacro(ds_cfg, dispatch_cfg, qmacroname, query));
    } else {
        // Create the remote query macro
        nag::verbose("Running query macro %s on %s", qmacroname.c_str(), baseurl.c_str());
        ConfigFile cfg;
        cfg.setValue("name", qmacroname);
        cfg.setValue("type", "remote");
        cfg.setValue("path", baseurl);
        cfg.setValue("qmacro", query);
        if (!url.empty())
            cfg.setValue("url", url);
        ds.reset(dataset::Reader::create(cfg));
    }
    return ds;
}

CommandLine::CommandLine(const std::string& name, int mansection)
    : StandardParserWithManpage(name, PACKAGE_VERSION, mansection, PACKAGE_BUGREPORT),
      output(0), processor(0), dispatcher(0)
{
    using namespace arki::utils::commandline;

	infoOpts = createGroup("Options controlling verbosity");
	debug = infoOpts->add<BoolOption>("debug", 0, "debug", "", "debug output");
	verbose = infoOpts->add<BoolOption>("verbose", 0, "verbose", "", "verbose output");
	status = infoOpts->add<BoolOption>("status", 0, "status", "",
			"print to standard error a line per every file with a summary of how it was handled");

	// Used only if requested
	inputOpts = createGroup("Options controlling input data");
	cfgfiles = 0; exprfile = 0; qmacro = 0;
	files = 0; moveok = moveko = movework = 0;
	restr = 0;
	ignore_duplicates = 0;
    validate = 0;

	outputOpts = createGroup("Options controlling output style");
	merged = 0; postproc_data = 0;
	yaml = outputOpts->add<BoolOption>("yaml", 0, "yaml", "",
			"dump the metadata as human-readable Yaml records");
	yaml->longNames.push_back("dump");
	json = outputOpts->add<BoolOption>("json", 0, "json", "",
			"dump the metadata in JSON format");
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
	summary_short = outputOpts->add<BoolOption>("summary-short", 0, "summary-short", "",
			"output a list of all metadata values that exist in the summary of the data");
	summary_restrict = outputOpts->add<StringOption>("summary-restrict", 0, "summary-restrict", "types",
			"summarise using only the given metadata types (comma-separated list)");
	sort = outputOpts->add<StringOption>("sort", 0, "sort", "period:order",
			"sort order.  Period can be year, month, day, hour or minute."
			" Order can be a list of one or more metadata"
			" names (as seen in --yaml field names), with a '-'"
			" prefix to indicate reverse order.  For example,"
			" 'day:origin, -timerange, reftime' orders one day at a time"
			" by origin first, then by reverse timerange, then by reftime."
			" Default: do not sort");

	dispatchOpts = createGroup("Options controlling dispatching data to datasets");
	dispatch = dispatchOpts->add< VectorOption<String> >("dispatch", 0, "dispatch", "conffile",
			"dispatch the data to the datasets described in the "
			"given configuration file (or a dataset path can also "
			"be given), then output the metadata of the data that "
			"has been dispatched (can be specified multiple times)");
	testdispatch = dispatchOpts->add< VectorOption<String> >("testdispatch", 0, "testdispatch", "conffile",
			"simulate dispatching the files right after scanning, using the given configuration file "
			"or dataset directory (can be specified multiple times)");

	postproc_data = inputOpts->add< VectorOption<ExistingFile> >("postproc-data", 0, "postproc-data", "file",
		"when querying a remote server with postprocessing, upload a file"
		" to be used by the postprocessor (can be given more than once)");
}

CommandLine::~CommandLine()
{
	if (dispatcher) delete dispatcher;
	if (processor) delete processor;
	if (output) delete output;
}

void CommandLine::addScanOptions()
{
    using namespace arki::utils::commandline;

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
	ignore_duplicates = dispatchOpts->add<BoolOption>("ignore-duplicates", 0, "ignore-duplicates", "",
			"do not consider the run unsuccessful in case of duplicates");
    validate = dispatchOpts->add<StringOption>("validate", 0, "validate", "checks",
            "run the given checks on the input data before dispatching"
            " (comma-separated list; use 'list' to get a list)");
}

void CommandLine::addQueryOptions()
{
    using namespace arki::utils::commandline;

	cfgfiles = inputOpts->add< VectorOption<String> >("config", 'C', "config", "file",
		"read configuration about input sources from the given file (can be given more than once)");
	exprfile = inputOpts->add<StringOption>("file", 'f', "file", "file",
		"read the expression from the given file");
	merged = outputOpts->add<BoolOption>("merged", 0, "merged", "",
		"if multiple datasets are given, merge their data and output it in"
		" reference time order.  Note: sorting does not work when using"
		" --postprocess, --data or --report");
	qmacro = add<StringOption>("qmacro", 0, "qmacro", "name",
		"run the given query macro instead of a plain query");
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

    if (postprocess->isSet() && targetfile->isSet())
        throw commandline::BadOption("--postprocess conflicts with --targetfile");
    if (postproc_data && postproc_data->isSet() && !postprocess->isSet())
        throw commandline::BadOption("--upload only makes sense with --postprocess");

    // Initialize the processor maker
    pmaker.summary = summary->boolValue();
    pmaker.summary_short = summary_short->boolValue();
    pmaker.yaml = yaml->boolValue();
    pmaker.json = json->boolValue();
    pmaker.annotate = annotate->boolValue();
    pmaker.data_only = dataOnly->boolValue();
    pmaker.data_inline = dataInline->boolValue();
    pmaker.postprocess = postprocess->stringValue();
    pmaker.report = report->stringValue();
    pmaker.summary_restrict = summary_restrict->stringValue();
    pmaker.sort = sort->stringValue();

    // Run here a consistency check on the processor maker configuration
    std::string errors = pmaker.verify_option_consistency();
    if (!errors.empty())
        throw commandline::BadOption(errors);
	
	return false;
}

void CommandLine::setupProcessing()
{
    // Honour --validate=list
    if (validate)
    {
        if (validate->stringValue() == "list")
        {
            // Print validator list and exit
            const ValidatorRepository& vals = ValidatorRepository::get();
            for (ValidatorRepository::const_iterator i = vals.begin();
                    i != vals.end(); ++i)
            {
                cout << i->second->name << " - " << i->second->desc << endl;
            }
            throw HandledByCommandLineParser();
        }
    }

    // Parse the matcher query
    if (exprfile)
    {
        if (exprfile->isSet())
        {
            // Read the entire file into memory and parse it as an expression
            strquery = files::read_file(exprfile->stringValue());
        } else {
            // Read from the first commandline argument
            if (!hasNext())
            {
                if (exprfile)
                    throw commandline::BadOption("you need to specify a filter expression or use --"+exprfile->longNames[0]);
                else
                    throw commandline::BadOption("you need to specify a filter expression");
            }
            // And parse it as an expression
            strquery = next();
        }
    }


	// Initialise the dataset list

	if (cfgfiles)	// From -C options, looking for config files or datasets
		for (vector<string>::const_iterator i = cfgfiles->values().begin();
				i != cfgfiles->values().end(); ++i)
			parseConfigFile(inputInfo, *i);

    if (files && files->isSet())    // From --files option, looking for data files or datasets
    {
        // Open the file
        string file = files->stringValue();
        unique_ptr<NamedFileDescriptor> in;
        if (file != "-")
            in.reset(new InputFile(file));
        else
            in.reset(new Stdin);

        // Read the content and scan the related files or dataset directories
        auto reader = LineReader::from_fd(*in);
        string line;
        while (reader->getline(line))
        {
            line = str::strip(line);
            if (line.empty())
                continue;
            dataset::Reader::readConfig(line, inputInfo);
        }
    }

    while (hasNext())	// From command line arguments, looking for data files or datasets
        dataset::Reader::readConfig(next(), inputInfo);

    if (inputInfo.sectionSize() == 0)
        throw commandline::BadOption("you need to specify at least one input file or dataset");

    if (summary && summary->isSet() && summary_short && summary_short->isSet())
        throw commandline::BadOption("--summary and --summary-short cannot be used together");

    // Filter the dataset list
    if (restr && restr->isSet())
    {
        Restrict rest(restr->stringValue());
        rest.remove_unallowed(inputInfo);
        if (inputInfo.sectionSize() == 0)
            throw commandline::BadOption("no accessible datasets found for the given --restrict value");
    }

    // Some things cannot be done when querying multiple datasets at the same time
    if (inputInfo.sectionSize() > 1 && !dispatcher && !(qmacro && qmacro->isSet()))
    {
        if (postprocess->boolValue())
            throw commandline::BadOption("postprocessing is not possible when querying more than one dataset at the same time");
        if (report->boolValue())
            throw commandline::BadOption("reports are not possible when querying more than one dataset at the same time");
    }


    // Validate the query with all the servers
    if (exprfile)
    {
        if (qmacro && qmacro->isSet())
        {
            query = Matcher::parse("");
        } else {
            // Resolve the query on each server (including the local system, if
            // queried). If at least one server can expand it, send that
            // expanded query to all servers. If two servers expand the same
            // query in different ways, raise an error.
            set<string> servers_seen;
            string expanded;
            string resolved_by;
            bool first = true;
            for (ConfigFile::const_section_iterator i = inputInfo.sectionBegin();
                    i != inputInfo.sectionEnd(); ++i)
            {
                string server = i->second->value("server");
                if (servers_seen.find(server) != servers_seen.end()) continue;
                string got;
                try {
                    if (server.empty())
                    {
                        got = Matcher::parse(strquery).toStringExpanded();
                        resolved_by = "local system";
                    } else {
                        got = dataset::HTTP::expandMatcher(strquery, server);
                        resolved_by = server;
                    }
                } catch (std::exception& e) {
                    // If the server cannot expand the query, we're
                    // ok as we send it expanded. What we are
                    // checking here is that the server does not
                    // have a different idea of the same aliases
                    // that we use
                    continue;
                }
                if (!first && got != expanded)
                {
                    nag::warning("%s expands the query as %s", server.c_str(), got.c_str());
                    nag::warning("%s expands the query as %s", resolved_by.c_str(), expanded.c_str());
                    throw std::runtime_error("cannot check alias consistency: two systems queried disagree about the query alias expansion");
                } else if (first)
                    expanded = got;
                first = false;
            }

            // If no server could expand it, do it anyway locally: either we
            // can resolve it with local aliases, or we raise an appropriate
            // error message
            if (first)
                expanded = strquery;

            query = Matcher::parse(expanded);
        }
    }

    // Open output stream

    if (!output)
        output = make_output(*outfile).release();

    // Create the core processor
    unique_ptr<DatasetProcessor> p = pmaker.make(query, *output);
    processor = p.release();

    // If targetfile is requested, wrap with the targetfile processor
    if (targetfile->isSet())
    {
        SingleOutputProcessor* sop = dynamic_cast<SingleOutputProcessor*>(processor);
        assert(sop != nullptr);
        processor = new TargetFileProcessor(sop, targetfile->stringValue());
    }

    // Create the dispatcher if needed
    if (dispatch->isSet() || testdispatch->isSet())
    {
        if (dispatch->isSet() && testdispatch->isSet())
            throw commandline::BadOption("you cannot use --dispatch together with --testdispatch");
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
	{
		dispatcher->reportStatus = status->boolValue();
		if (ignore_duplicates && ignore_duplicates->boolValue())
			dispatcher->ignore_duplicates = true;

        if (validate)
        {
            const ValidatorRepository& vals = ValidatorRepository::get();

            // Add validators to dispatcher
            str::Split splitter(validate->stringValue(), ",");
            for (str::Split::const_iterator iname = splitter.begin();
                    iname != splitter.end(); ++iname)
            {
                ValidatorRepository::const_iterator i = vals.find(*iname);
                if (i == vals.end())
                    throw commandline::BadOption("unknown validator '" + *iname + "'. You can get a list using --validate=list.");
                dispatcher->dispatcher->add_validator(*(i->second));
            }
        }
    } else {
        if (validate && validate->isSet())
            throw commandline::BadOption("--validate only makes sense with --dispatch or --testdispatch");
    }

    if (postproc_data && postproc_data->isSet())
    {
        // Pass files for the postprocessor in the environment
        string val = str::join(":", postproc_data->values().begin(), postproc_data->values().end());
        setenv("ARKI_POSTPROC_FILES", val.c_str(), 1);
    } else
        unsetenv("ARKI_POSTPROC_FILES");
}

void CommandLine::doneProcessing()
{
	if (processor)
		processor->end();
}

static std::string moveFile(const std::string& source, const std::string& targetdir)
{
    string targetFile = str::joinpath(targetdir, str::basename(source));
    if (::rename(source.c_str(), targetFile.c_str()) == -1)
        throw_system_error ("cannot move " + source + " to " + targetFile);
    return targetFile;
}

static std::string moveFile(const dataset::Reader& ds, const std::string& targetdir)
{
    if (const dataset::File* d = dynamic_cast<const dataset::File*>(&ds))
        return moveFile(d->pathname(), targetdir);
    else
        return string();
}

std::unique_ptr<dataset::Reader> CommandLine::openSource(ConfigFile& info)
{
    if (movework && movework->isSet() && info.value("type") == "file")
        info.setValue("path", moveFile(info.value("path"), movework->stringValue()));

    return unique_ptr<dataset::Reader>(dataset::Reader::create(info));
}

bool CommandLine::processSource(dataset::Reader& ds, const std::string& name)
{
    if (dispatcher)
        return dispatcher->process(ds, name);
    processor->process(ds, name);
    return true;
}

void CommandLine::closeSource(std::unique_ptr<dataset::Reader> ds, bool successful)
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
	: cfg(cfg), dispatcher(0), next(next), ignore_duplicates(false), reportStatus(false),
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

bool MetadataDispatch::process(dataset::Reader& ds, const std::string& name)
{
    setStartTime();
    results.clear();

    try {
        ds.query_data(Matcher(), [&](unique_ptr<Metadata> md) { return this->dispatch(move(md)); });
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

	bool success = !(countNotImported || countInErrorDataset);
	if (ignore_duplicates)
		success = success && (countSuccessful || countDuplicates);
	else
		success = success && (countSuccessful && !countDuplicates);

	flush();

	countSuccessful = 0;
	countNotImported = 0;
	countDuplicates = 0;
	countInErrorDataset = 0;

	return success;
}

bool MetadataDispatch::dispatch(unique_ptr<Metadata>&& md)
{
    // Dispatch to matching dataset
    switch (dispatcher->dispatch(move(md), results.inserter_func()))
    {
        case Dispatcher::DISP_OK:
            ++countSuccessful;
            break;
        case Dispatcher::DISP_DUPLICATE_ERROR:
            ++countDuplicates;
            break;
        case Dispatcher::DISP_ERROR:
            ++countInErrorDataset;
            break;
        case Dispatcher::DISP_NOTWRITTEN:
            // If dispatching failed, add a big note about it.
            // Analising the notes in the output should be enough to catch this
            // even happening.
            // No point adding the note here: md has already been consumed by 'results'
            //md->add_note("WARNING: The data has not been imported in ANY dataset");
            ++countNotImported;
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
        char buf[32];
        snprintf(buf, 32, " in %d.%06d seconds", (int)diff.tv_sec, (int)diff.tv_usec);
        timeinfo = buf;
    }
    if (!countSuccessful && !countNotImported && !countDuplicates && !countInErrorDataset)
        return "no data processed" + timeinfo;

    if (!countNotImported && !countDuplicates && !countInErrorDataset)
    {
        stringstream ss;
        ss << "everything ok: " << countSuccessful << " message";
        if (countSuccessful != 1)
            ss << "s";
        ss << " imported" + timeinfo;
        return ss.str();
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
