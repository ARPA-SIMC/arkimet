#ifndef ARKI_RUNTIME_H
#define ARKI_RUNTIME_H

#include <arki/utils/commandline/parser.h>
#include <memory>

namespace arki {
namespace runtime {
struct Module;

/**
 * Initialise the libraries that we use and parse the matcher alias database.
 */
void init();

struct BaseCommandLine : public utils::commandline::StandardParserWithManpage
{
    std::vector<Module*> modules;

    utils::commandline::OptionGroup* infoOpts = nullptr;
    utils::commandline::BoolOption* verbose = nullptr;
    utils::commandline::BoolOption* debug = nullptr;

    BaseCommandLine(const std::string& name, int mansection=1);
    ~BaseCommandLine();

    template<typename MODULE>
    MODULE* add_module(std::unique_ptr<MODULE>&& module)
    {
        MODULE* res;
        modules.push_back(res = module.release());
        return res;
    }

    bool parse(int argc, const char* argv[]);
};

}
}
#endif
