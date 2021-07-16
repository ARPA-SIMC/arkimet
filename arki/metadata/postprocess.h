#ifndef ARKI_METADATA_POSTPROCESS_H
#define ARKI_METADATA_POSTPROCESS_H

#include <arki/core/fwd.h>
#include <arki/stream/fwd.h>
#include <arki/metadata/fwd.h>
#include <string>
#include <vector>
#include <sstream>
#include <functional>
#include <memory>

namespace arki {

namespace stream {
class FilterProcess;
}

namespace metadata {

class Postprocess
{
public:
    /**
     * Split the command into the argv components, resolve argv0 to a full
     * path, check that it is allowed to run
     */
    static std::vector<std::string> validate_command(const std::string& command, const core::cfg::Section& cfg);

    static bool send(std::shared_ptr<Metadata> md, StreamOutput& out);
};

}
}
#endif
