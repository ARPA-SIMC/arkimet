#ifndef ARKI_RUNTIME_INPUTS_H
#define ARKI_RUNTIME_INPUTS_H

#include <arki/configfile.h>
#include <vector>

namespace arki {
namespace runtime {

struct Restrict;

struct Inputs : public std::vector<ConfigFile>
{
    void add_sections(const ConfigFile& cfg);
    void add_config_file(const std::string& pathname);
    void add_pathname(const std::string& pathname);
    void add_pathnames_from_file(const std::string& pathname);
    void remove_unallowed(const Restrict& restrict);

    ConfigFile as_config() const;
};

}
}

#endif
