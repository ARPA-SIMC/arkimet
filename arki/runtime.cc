#include "config.h"
#include "arki/runtime.h"
#include "arki/runtime/dispatch.h"
#include "arki/runtime/source.h"
#include "arki/runtime/processor.h"
#include "arki/exceptions.h"
#include "arki/dispatcher.h"
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
#include "arki/dataset/merged.h"
#include "arki/targetfile.h"
#include "arki/formatter.h"
#include "arki/postprocess.h"
#include "arki/querymacro.h"
#include "arki/sort.h"
#include "arki/nag.h"
#include "arki/iotrace.h"
#include "arki/types-init.h"
#include "arki/utils/sys.h"
#ifdef HAVE_LUA
#include "arki/report.h"
#endif
#include <sys/types.h>
#include <sys/stat.h>
#include <fcntl.h>
#include <iostream>
#include <cstdlib>

using namespace std;
using namespace arki::utils;
using namespace arki::types;

namespace arki {
namespace runtime {

void init()
{
    static bool initialized = false;

    if (initialized) return;
    types::init_default_types();
    runtime::readMatcherAliasDatabase();
    iotrace::init();
    initialized = true;
}

HandledByCommandLineParser::HandledByCommandLineParser(int status) : status(status) {}
HandledByCommandLineParser::~HandledByCommandLineParser() {}

std::unique_ptr<dataset::Reader> make_qmacro_dataset(const ConfigFile& ds_cfg, const ConfigFile& dispatch_cfg, const std::string& qmacroname, const std::string& query, const std::string& url)
{
    unique_ptr<dataset::Reader> ds;
    string baseurl = dataset::http::Reader::allSameRemoteServer(dispatch_cfg);
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
        ds = dataset::Reader::create(cfg);
    }
    return ds;
}


BaseCommandLine::BaseCommandLine(const std::string& name, int mansection)
    : StandardParserWithManpage(name, PACKAGE_VERSION, mansection, PACKAGE_BUGREPORT)
{
    using namespace arki::utils::commandline;

    infoOpts = createGroup("Options controlling verbosity");
    debug = infoOpts->add<BoolOption>("debug", 0, "debug", "", "debug output");
    verbose = infoOpts->add<BoolOption>("verbose", 0, "verbose", "", "verbose output");
}


CommandLine::CommandLine(const std::string& name, int mansection)
    : BaseCommandLine(name, mansection)
{
    using namespace arki::utils::commandline;

	status = infoOpts->add<BoolOption>("status", 0, "status", "",
			"print to standard error a line per every file with a summary of how it was handled");

    // Used only if requested
    inputOpts = createGroup("Options controlling input data");

    outputOpts = createGroup("Options controlling output style");
    yaml = outputOpts->add<BoolOption>("yaml", 0, "yaml", "",
            "dump the metadata as human-readable Yaml records");
    yaml->longNames.push_back("dump");
    json = outputOpts->add<BoolOption>("json", 0, "json", "",
            "dump the metadata in JSON format");
    annotate = outputOpts->add<BoolOption>("annotate", 0, "annotate", "",
            "annotate the human-readable Yaml output with field descriptions");
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
    archive = outputOpts->add<OptvalStringOption>("archive", 0, "archive", "format",
            "output an archive with the given format (default: tar, supported: tar, tar.gz, tar.xz, zip)");
	sort = outputOpts->add<StringOption>("sort", 0, "sort", "period:order",
			"sort order.  Period can be year, month, day, hour or minute."
			" Order can be a list of one or more metadata"
			" names (as seen in --yaml field names), with a '-'"
			" prefix to indicate reverse order.  For example,"
			" 'day:origin, -timerange, reftime' orders one day at a time"
			" by origin first, then by reverse timerange, then by reftime."
			" Default: do not sort");

	postproc_data = inputOpts->add< VectorOption<ExistingFile> >("postproc-data", 0, "postproc-data", "file",
		"when querying a remote server with postprocessing, upload a file"
		" to be used by the postprocessor (can be given more than once)");
}

ScanCommandLine::ScanCommandLine(const std::string& name, int mansection)
    : CommandLine(name, mansection)
{
    using namespace arki::utils::commandline;

    dispatchOpts = createGroup("Options controlling dispatching data to datasets");

    moveok = inputOpts->add<StringOption>("moveok", 0, "moveok", "directory",
            "move input files imported successfully to the given directory");
    moveko = inputOpts->add<StringOption>("moveko", 0, "moveko", "directory",
            "move input files with problems to the given directory");
    movework = inputOpts->add<StringOption>("movework", 0, "movework", "directory",
            "move input files here before opening them. This is useful to "
            "catch the cases where arki-scan crashes without having a "
            "chance to handle errors.");

    copyok = inputOpts->add<StringOption>("copyok", 0, "copyok", "directory",
            "copy the data from input files that was imported successfully to the given directory");
    copyko = inputOpts->add<StringOption>("copyko", 0, "copyko", "directory",
            "copy the data from input files that had problems to the given directory");

    ignore_duplicates = dispatchOpts->add<BoolOption>("ignore-duplicates", 0, "ignore-duplicates", "",
            "do not consider the run unsuccessful in case of duplicates");
    validate = dispatchOpts->add<StringOption>("validate", 0, "validate", "checks",
            "run the given checks on the input data before dispatching"
            " (comma-separated list; use 'list' to get a list)");
    dispatch = dispatchOpts->add< VectorOption<String> >("dispatch", 0, "dispatch", "conffile",
            "dispatch the data to the datasets described in the "
            "given configuration file (or a dataset path can also "
            "be given), then output the metadata of the data that "
            "has been dispatched (can be specified multiple times)");
    testdispatch = dispatchOpts->add< VectorOption<String> >("testdispatch", 0, "testdispatch", "conffile",
            "simulate dispatching the files right after scanning, using the given configuration file "
            "or dataset directory (can be specified multiple times)");
}

QueryCommandLine::QueryCommandLine(const std::string& name, int mansection)
    : CommandLine(name, mansection)
{
    using namespace arki::utils::commandline;
    dataInline = outputOpts->add<BoolOption>("inline", 0, "inline", "",
            "output the binary metadata together with the data (pipe through "
            " arki-dump or arki-grep to estract the data afterwards)");
    dataOnly = outputOpts->add<BoolOption>("data", 0, "data", "",
            "output only the data");
}

CommandLine::~CommandLine()
{
    if (output) delete output;
}

void CommandLine::add_scan_options()
{
    using namespace arki::utils::commandline;

    files = inputOpts->add<StringOption>("files", 0, "files", "file",
            "read the list of files to scan from the given file instead of the command line");
}

void CommandLine::add_query_options()
{
    using namespace arki::utils::commandline;

    cfgfiles = inputOpts->add< VectorOption<String> >("config", 'C', "config", "file",
            "read configuration about input sources from the given file (can be given more than once)");
    exprfile = inputOpts->add<StringOption>("file", 'f', "file", "file",
            "read the expression from the given file");

    postprocess = outputOpts->add<StringOption>("postproc", 'p', "postproc", "command",
            "output only the data, postprocessed with the given filter");
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
    if (dispatchOpts)
        add(dispatchOpts);

    if (StandardParserWithManpage::parse(argc, argv))
        return true;

    nag::init(verbose->isSet(), debug->isSet());

    if (postprocess && targetfile && postprocess->isSet() && targetfile->isSet())
        throw commandline::BadOption("--postprocess conflicts with --targetfile");
    if (postproc_data && postprocess && postproc_data->isSet() && !postprocess->isSet())
        throw commandline::BadOption("--upload only makes sense with --postprocess");

    return false;
}

bool ScanCommandLine::parse(int argc, const char* argv[])
{
    if (CommandLine::parse(argc, argv))
        return true;
    processor::verify_option_consistency(*this);
    return false;
}

bool QueryCommandLine::parse(int argc, const char* argv[])
{
    if (CommandLine::parse(argc, argv))
        return true;
    processor::verify_option_consistency(*this);
    return false;
}

void CommandLine::setupProcessing()
{
    // Parse the matcher query
    if (exprfile)
    {
        if (exprfile->isSet())
        {
            // Read the entire file into memory and parse it as an expression
            strquery = runtime::read_file(exprfile->stringValue());
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

    if (cfgfiles) // From -C options, looking for config files or datasets
        for (const auto& pathname : cfgfiles->values())
            inputs.add_config_file(pathname);

    if (files && files->isSet())    // From --files option, looking for data files or datasets
        inputs.add_pathnames_from_file(files->stringValue());

    while (hasNext()) // From command line arguments, looking for data files or datasets
        inputs.add_pathname(next());

    if (inputs.empty())
        throw commandline::BadOption("you need to specify at least one input file or dataset");

    if (summary && summary->isSet() && summary_short && summary_short->isSet())
        throw commandline::BadOption("--summary and --summary-short cannot be used together");

    // Filter the dataset list
    if (restr && restr->isSet())
    {
        Restrict rest(restr->stringValue());
        inputs.remove_unallowed(rest);
        if (inputs.empty())
            throw commandline::BadOption("no accessible datasets found for the given --restrict value");
    }

    // Some things cannot be done when querying multiple datasets at the same time
    if (inputs.size() > 1 && !(qmacro && qmacro->isSet()))
    {
        if (postprocess && postprocess->boolValue())
            throw commandline::BadOption("postprocessing is not possible when querying more than one dataset at the same time");
        if (report && report->boolValue())
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
            for (const auto& i: inputs)
            {
                string server = i.value("server");
                if (servers_seen.find(server) != servers_seen.end()) continue;
                string got;
                try {
                    if (server.empty())
                    {
                        got = Matcher::parse(strquery).toStringExpanded();
                        resolved_by = "local system";
                    } else {
                        got = dataset::http::Reader::expandMatcher(strquery, server);
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

    if (postproc_data && postproc_data->isSet())
    {
        // Pass files for the postprocessor in the environment
        string val = str::join(":", postproc_data->values().begin(), postproc_data->values().end());
        setenv("ARKI_POSTPROC_FILES", val.c_str(), 1);
    } else
        unsetenv("ARKI_POSTPROC_FILES");
}

bool CommandLine::foreach_source(std::function<bool(Source&)> dest)
{
    bool all_successful = true;

    if (merged && merged->boolValue())
    {
        MergedSource source(*this);
        nag::verbose("Processing %s...", source.name().c_str());
        source.open();
        try {
            all_successful = dest(source);
        } catch (std::exception& e) {
            nag::warning("%s failed: %s", source.name().c_str(), e.what());
            all_successful = false;
        }
        source.close(all_successful);
    } else if (qmacro && qmacro->isSet()) {
        QmacroSource source(*this);
        nag::verbose("Processing %s...", source.name().c_str());
        source.open();
        try {
            all_successful = dest(source);
        } catch (std::exception& e) {
            nag::warning("%s failed: %s", source.name().c_str(), e.what());
            all_successful = false;
        }
        source.close(all_successful);
    } else {
        // Query all the datasets in sequence
        for (const auto& cfg: inputs)
        {
            FileSource source(*this, cfg);
            nag::verbose("Processing %s...", source.name().c_str());
            source.open();
            bool success;
            try {
                success = dest(source);
            } catch (std::exception& e) {
                nag::warning("%s failed: %s", source.name().c_str(), e.what());
                success = false;
            }
            source.close(success);
            if (!success) all_successful = false;
        }
    }

    return all_successful;
}

}
}
