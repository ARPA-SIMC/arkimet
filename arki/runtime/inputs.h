#ifndef ARKI_RUNTIME_INPUTS_H
#define ARKI_RUNTIME_INPUTS_H

#include <arki/configfile.h>
#include <vector>

namespace arki {
namespace runtime {
struct Restrict;
struct ScanCommandLine;
struct QueryCommandLine;

struct Inputs
{
    ConfigFile merged;

    Inputs() = default;
    Inputs(ScanCommandLine& args);
    Inputs(QueryCommandLine& args);

    bool empty() const { return merged.section_begin() == merged.section_end(); }

    void add_section(const ConfigFile& section);
    void add_sections(const ConfigFile& cfg);
    void add_config_file(const std::string& pathname);
    void add_pathname(const std::string& pathname);
    void add_pathnames_from_file(const std::string& pathname);
    void remove_unallowed(const Restrict& restrict);
    void remove_system_datasets();

    std::string expand_remote_query(const std::string& query);

    ConfigFile as_config() const;
};

}
}

#endif
