#ifndef ARKI_QUERYMACRO_H
#define ARKI_QUERYMACRO_H

/// Macros implementing special query strategies

#include <arki/core/fwd.h>
#include <arki/dataset/fwd.h>
#include <string>

namespace arki {
namespace qmacro {

/**
 * Get a Querymacro dataset reader.
 *
 * ds_cfg: the configuration of the querymacro reader
 * dispatch_cfg: the datasets available to the querymacro code
 * name: the macro name and arguments, space separated
 * query: the macro script
 */
std::shared_ptr<dataset::Reader> get(const core::cfg::Section& ds_cfg, const core::cfg::Sections& dispatch_cfg, const std::string& name, const std::string& query);

}
}
#endif
