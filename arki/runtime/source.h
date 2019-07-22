#ifndef ARKI_RUNTIME_SOURCE_H
#define ARKI_RUNTIME_SOURCE_H

#include <arki/dataset/fwd.h>
#include <arki/scan/fwd.h>
#include <arki/core/cfg.h>
#include <string>
#include <vector>

namespace arki {
namespace runtime {

bool foreach_stdin(const std::string& format, std::function<void(dataset::Reader&)> dest);
bool foreach_merged(const core::cfg::Sections& input, std::function<void(dataset::Reader&)> dest);
bool foreach_qmacro(const std::string& macro_name, const std::string& macro_query, const core::cfg::Sections& inputs, std::function<void(dataset::Reader&)> dest);
bool foreach_sections(const core::cfg::Sections& inputs, std::function<void(dataset::Reader&)> dest);

}
}
#endif
