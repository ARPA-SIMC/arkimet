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

bool CommandLine::parse(int argc, const char* argv[])
{
    add(inputOpts);
    add(outputOpts);

    if (BaseCommandLine::parse(argc, argv))
        return true;

    return false;
}

}
}
