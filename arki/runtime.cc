#include "config.h"
#include "arki/runtime.h"
#include "arki/runtime/config.h"
#include "arki/runtime/processor.h"
#include "arki/runtime/io.h"
#include "arki/runtime/module.h"
#include "arki/runtime/dispatch.h"
#include "arki/types-init.h"
#include "arki/iotrace.h"
#include "arki/nag.h"
#include "arki/utils/string.h"

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


BaseCommandLine::BaseCommandLine(const std::string& name, int mansection)
    : StandardParserWithManpage(name, PACKAGE_VERSION, mansection, PACKAGE_BUGREPORT)
{
    using namespace arki::utils::commandline;

    infoOpts = addGroup("Options controlling verbosity");
    debug = infoOpts->add<BoolOption>("debug", 0, "debug", "", "debug output");
    verbose = infoOpts->add<BoolOption>("verbose", 0, "verbose", "", "verbose output");
}

BaseCommandLine::~BaseCommandLine()
{
    for (auto m: modules)
        delete m;
}

bool BaseCommandLine::parse(int argc, const char* argv[])
{
    if (StandardParserWithManpage::parse(argc, argv))
        return true;

    nag::init(verbose->isSet(), debug->isSet());

    for (auto m: modules)
        if (m->handle_parsed_options())
            return true;

    return false;
}


CommandLine::CommandLine(const std::string& name, int mansection)
    : BaseCommandLine(name, mansection)
{
    using namespace arki::utils::commandline;

    // Used only if requested
    inputOpts = createGroup("Options controlling input data");
    stdin_input = inputOpts->add<StringOption>("stdin", 0, "stdin", "format",
            "read input from standard input");

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
}

ScanCommandLine::ScanCommandLine(const std::string& name, int mansection)
    : CommandLine(name, mansection)
{
    using namespace arki::utils::commandline;

    files = inputOpts->add<StringOption>("files", 0, "files", "file",
            "read the list of files to scan from the given file instead of the command line");

    dispatch_options = add_module(std::unique_ptr<DispatchOptions>(new DispatchOptions(*this)));
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
    merged = outputOpts->add<BoolOption>("merged", 0, "merged", "",
            "if multiple datasets are given, merge their data and output it in"
            " reference time order.  Note: sorting does not work when using"
            " --postprocess, --data or --report");
    exprfile = inputOpts->add<StringOption>("file", 'f', "file", "file",
            "read the expression from the given file");
    qmacro = add<StringOption>("qmacro", 0, "qmacro", "name",
            "run the given query macro instead of a plain query");
    cfgfiles = inputOpts->add< VectorOption<String> >("config", 'C', "config", "file",
            "read configuration about input sources from the given file (can be given more than once)");
    restr = add<StringOption>("restrict", 0, "restrict", "names",
            "restrict operations to only those datasets that allow one of the given (comma separated) names");
    postprocess = outputOpts->add<StringOption>("postproc", 'p', "postproc", "command",
            "output only the data, postprocessed with the given filter");
    postproc_data = outputOpts->add< VectorOption<ExistingFile> >("postproc-data", 0, "postproc-data", "file",
        "when querying a remote server with postprocessing, upload a file"
        " to be used by the postprocessor (can be given more than once)");
}

bool CommandLine::parse(int argc, const char* argv[])
{
    add(inputOpts);
    add(outputOpts);

    if (BaseCommandLine::parse(argc, argv))
        return true;

    return false;
}

bool ScanCommandLine::parse(int argc, const char* argv[])
{
    if (CommandLine::parse(argc, argv))
        return true;
    processor::verify_option_consistency(*this);

    if (dispatch_options->dispatch->isSet())
    {
        if (stdin_input->isSet())
            throw commandline::BadOption("--stdin cannot be used together with --dispatch");
    }

    return false;
}

bool QueryCommandLine::parse(int argc, const char* argv[])
{
    if (CommandLine::parse(argc, argv))
        return true;
    processor::verify_option_consistency(*this);

    if (postprocess->isSet() && targetfile->isSet())
        throw commandline::BadOption("--postprocess conflicts with --targetfile");
    if (postproc_data->isSet() && !postprocess->isSet())
        throw commandline::BadOption("--postproc-data only makes sense with --postprocess");

    if (postproc_data->isSet())
    {
        // Pass files for the postprocessor in the environment
        string val = str::join(":", postproc_data->values().begin(), postproc_data->values().end());
        setenv("ARKI_POSTPROC_FILES", val.c_str(), 1);
    } else
        unsetenv("ARKI_POSTPROC_FILES");

    // Parse the matcher query
    if (qmacro->isSet())
    {
        strquery = "";

        if (exprfile->isSet())
        {
            // Read the entire file into memory and parse it as an expression
            qmacro_query = runtime::read_file(exprfile->stringValue());
        } else {
            // Read from the first commandline argument
            if (!hasNext())
                throw commandline::BadOption("you need to specify a " + qmacro->stringValue() + " query or use --" + exprfile->longNames[0]);

            // And parse it as an expression
            qmacro_query = next();
        }
    } else {
        if (exprfile->isSet())
        {
            // Read the entire file into memory and parse it as an expression
            strquery = runtime::read_file(exprfile->stringValue());
        } else {
            // Read from the first commandline argument
            if (!hasNext())
                throw commandline::BadOption("you need to specify a filter expression or use --" + exprfile->longNames[0]);

            // And parse it as an expression
            strquery = next();
        }
    }

    return false;
}

}
}
