#ifndef ARKI_RUNTIME_SOURCE_H
#define ARKI_RUNTIME_SOURCE_H

#include <arki/dataset/fwd.h>
#include <arki/scan/fwd.h>
#include <arki/core/cfg.h>
#include <string>
#include <vector>

namespace arki {
namespace runtime {

/**
 * Data source from the path to a file or dataset
 */
struct FileSource
{
    std::shared_ptr<dataset::Reader> reader;

    core::cfg::Section cfg;
    std::string movework;
    std::string moveok;
    std::string moveko;

    FileSource(const core::cfg::Section& info);

    void open();
    void close(bool successful);
};


bool foreach_stdin(const std::string& format, std::function<void(dataset::Reader&)> dest);
bool foreach_merged(const core::cfg::Sections& input, std::function<void(dataset::Reader&)> dest);
bool foreach_qmacro(const std::string& macro_name, const std::string& macro_query, const core::cfg::Sections& inputs, std::function<void(dataset::Reader&)> dest);
bool foreach_sections(const core::cfg::Sections& inputs, std::function<void(dataset::Reader&)> dest);

}
}
#endif
