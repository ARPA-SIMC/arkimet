#ifndef ARKI_QUERYMACRO_H
#define ARKI_QUERYMACRO_H

/// Macros implementing special query strategies

#include <arki/core/cfg.h>
#include <arki/dataset.h>
#include <string>
#include <functional>

namespace arki {
namespace qmacro {

struct Options
{
    core::cfg::Section macro_cfg;
    core::cfg::Sections datasets_cfg;
    std::string macro_name;
    std::string macro_args;
    std::string query;

    Options(const core::cfg::Sections& datasets_cfg, const std::string& name, const std::string& query);
    Options(const core::cfg::Section& macro_cfg, const core::cfg::Sections& datasets_cfg, const std::string& name, const std::string& query);
};


struct Base : public dataset::Reader
{
protected:
    std::shared_ptr<const dataset::Config> m_config;

public:
    core::cfg::Sections datasets_cfg;

    Base(const Options& opts);
    ~Base();

    const dataset::Config& config() const override { return *m_config; }
    std::string type() const override { return "querymacro"; }
};


/**
 * Get a Querymacro dataset reader.
 *
 * macro_cfg: the configuration of the querymacro reader
 * datasets_cfg: the datasets available to the querymacro code
 * name: the macro name and arguments, space separated
 * query: the macro script
 */
std::shared_ptr<dataset::Reader> get(const Options& opts);

void register_parser(
        const std::string& ext,
        std::function<std::shared_ptr<dataset::Reader>(const std::string& source, const Options& opts)> parser);

void init();

}
}
#endif
