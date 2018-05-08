#ifndef ARKI_RUNTIME_H
#define ARKI_RUNTIME_H

#include <arki/utils/commandline/parser.h>

namespace arki {
namespace runtime {

/**
 * Initialise the libraries that we use and parse the matcher alias database.
 */
void init();

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
    utils::commandline::StringOption* outfile = nullptr;
    utils::commandline::StringOption* targetfile = nullptr;
    utils::commandline::StringOption* report = nullptr;
    utils::commandline::OptvalStringOption* archive = nullptr;
    utils::commandline::StringOption* sort = nullptr;
    utils::commandline::StringOption* summary_restrict = nullptr;

    CommandLine(const std::string& name, int mansection=1);

    /// Parse the command line
    bool parse(int argc, const char* argv[]);
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
    utils::commandline::StringOption* files = nullptr;

    ScanCommandLine(const std::string& name, int mansection=1);

    bool parse(int argc, const char* argv[]);
};

struct QueryCommandLine : public CommandLine
{
    utils::commandline::BoolOption* dataInline = nullptr;
    utils::commandline::BoolOption* dataOnly = nullptr;
    utils::commandline::BoolOption* merged = nullptr;
    utils::commandline::StringOption* exprfile = nullptr;
    utils::commandline::StringOption* qmacro = nullptr;
    utils::commandline::VectorOption<utils::commandline::String>* cfgfiles = nullptr;
    utils::commandline::StringOption* restr = nullptr;
    utils::commandline::StringOption* postprocess = nullptr;
    utils::commandline::VectorOption<utils::commandline::ExistingFile>* postproc_data = nullptr;

    std::string strquery;
    std::string qmacro_query;

    QueryCommandLine(const std::string& name, int mansection=1);

    bool parse(int argc, const char* argv[]);
};

}
}
#endif
