#ifndef ARKI_RUNTIME_H
#define ARKI_RUNTIME_H

/// Common code used in most arkimet executables

#include <arki/utils/commandline/parser.h>
#include <arki/runtime/io.h>
#include <arki/runtime/config.h>
#include <arki/runtime/inputs.h>
#include <arki/metadata.h>
#include <arki/dataset/fwd.h>
#include <arki/matcher.h>
#include <arki/configfile.h>
#include <string>
#include <memory>
#include <vector>
#include <sys/time.h>

namespace arki {
class Summary;
class Formatter;
class Targetfile;
class Validator;
class Querymacro;

namespace runtime {
class Source;
class MetadataDispatch;

/**
 * Initialise the libraries that we use and parse the matcher alias database.
 */
void init();

std::unique_ptr<dataset::Reader> make_qmacro_dataset(const ConfigFile& ds_cfg, const ConfigFile& dispatch_cfg, const std::string& qmacroname, const std::string& query, const std::string& url=std::string());

/**
 * Exception raised when the command line parser has handled the current
 * command invocation.
 *
 * For example, this happens when using --validate=list, which prints a list of
 * validators then exits.
 */
struct HandledByCommandLineParser
{
    /// The exit status that we should return from main
    int status;

    HandledByCommandLineParser(int status=0);
    ~HandledByCommandLineParser();
};

struct BaseCommandLine : public utils::commandline::StandardParserWithManpage
{
    utils::commandline::OptionGroup* infoOpts = nullptr;
    utils::commandline::BoolOption* verbose = nullptr;
    utils::commandline::BoolOption* debug = nullptr;

    BaseCommandLine(const std::string& name, int mansection=1);
};

struct CommandLine : public BaseCommandLine
{
    utils::commandline::OptionGroup* inputOpts = nullptr;
    utils::commandline::OptionGroup* outputOpts = nullptr;
    utils::commandline::OptionGroup* dispatchOpts = nullptr;

    utils::commandline::BoolOption* status = nullptr;
    utils::commandline::BoolOption* yaml = nullptr;
    utils::commandline::BoolOption* json = nullptr;
    utils::commandline::BoolOption* annotate = nullptr;
    utils::commandline::BoolOption* summary = nullptr;
    utils::commandline::BoolOption* summary_short = nullptr;
    utils::commandline::BoolOption* merged = nullptr;
    utils::commandline::StringOption* restr = nullptr;
    utils::commandline::StringOption* exprfile = nullptr;
    utils::commandline::StringOption* qmacro = nullptr;
    utils::commandline::StringOption* outfile = nullptr;
    utils::commandline::StringOption* targetfile = nullptr;
    utils::commandline::StringOption* postprocess = nullptr;
    utils::commandline::StringOption* report = nullptr;
    utils::commandline::OptvalStringOption* archive = nullptr;
    utils::commandline::StringOption* sort = nullptr;
    utils::commandline::StringOption* files = nullptr;
    utils::commandline::StringOption* summary_restrict = nullptr;
    utils::commandline::VectorOption<utils::commandline::ExistingFile>* postproc_data = nullptr;
    utils::commandline::VectorOption<utils::commandline::String>* cfgfiles = nullptr;

    Inputs inputs;
    std::string strquery;
    Matcher query;
    utils::sys::NamedFileDescriptor* output = nullptr;

    CommandLine(const std::string& name, int mansection=1);
    ~CommandLine();

    /// Add scan-type options (--files, --moveok, --movework, --moveko)
    void add_scan_options();

    /// Add query-type options (--merged, --file, --cfgfiles)
    void add_query_options();

    /// Parse the command line
    bool parse(int argc, const char* argv[]);

	/**
	 * Set up processing after the command line has been parsed and
	 * additional tweaks have been applied
	 */
	void setupProcessing();

    /**
     * Instantiate all sources requested on command line.
     *
     * Return true if dest returned true (successful) on all sources.
     */
    bool foreach_source(std::function<bool(Source&)> dest);
};

struct ScanCommandLine : public CommandLine
{
    utils::commandline::StringOption* moveok = nullptr;
    utils::commandline::StringOption* moveko = nullptr;
    utils::commandline::StringOption* movework = nullptr;
    utils::commandline::StringOption* copyok = nullptr;
    utils::commandline::StringOption* copyko = nullptr;
    utils::commandline::BoolOption* ignore_duplicates = nullptr;
    utils::commandline::StringOption* validate = nullptr;
    utils::commandline::VectorOption<utils::commandline::String>* dispatch = nullptr;
    utils::commandline::VectorOption<utils::commandline::String>* testdispatch = nullptr;

    ScanCommandLine(const std::string& name, int mansection=1);

    bool parse(int argc, const char* argv[]);
};

struct QueryCommandLine : public CommandLine
{
    utils::commandline::BoolOption* dataInline = nullptr;
    utils::commandline::BoolOption* dataOnly = nullptr;

    QueryCommandLine(const std::string& name, int mansection=1);

    bool parse(int argc, const char* argv[]);
};

}
}
#endif
