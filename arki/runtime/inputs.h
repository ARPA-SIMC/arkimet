#ifndef ARKI_RUNTIME_INPUTS_H
#define ARKI_RUNTIME_INPUTS_H

#include <arki/core/cfg.h>
#include <vector>

namespace arki {
namespace runtime {
struct Restrict;

struct Inputs
{
    core::cfg::Sections& merged;

    Inputs(core::cfg::Sections& merged);

    bool empty() const { return merged.empty(); }

    void add_section(const core::cfg::Section& section, const std::string& name=std::string());
    void add_sections(const core::cfg::Sections& cfg);
    void add_config_file(const std::string& pathname);
    void add_pathname(const std::string& pathname);
    void add_pathnames_from_file(const std::string& pathname);
    void remove_unallowed(const Restrict& restrict);
    void remove_system_datasets();
};

}
}

#endif
