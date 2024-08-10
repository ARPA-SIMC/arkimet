#include "config.h"
#include "postprocess.h"
#include "arki/exceptions.h"
#include "arki/stream.h"
#include "arki/core/file.h"
#include "arki/core/cfg.h"
#include "arki/metadata.h"
#include "arki/metadata/data.h"
#include "arki/utils/string.h"
#include "arki/utils/regexp.h"
#include "arki/runtime.h"
#include "arki/stream/filter.h"
#include <fcntl.h>
#include <unistd.h>
#include <signal.h>
#include <cerrno>
#include <set>


#if __xlC__
typedef void (*sighandler_t)(int);
#endif

using namespace std;
using namespace arki::utils;
using namespace arki::core;

namespace arki {
namespace metadata {

std::vector<std::string> Postprocess::validate_command(const std::string& command, const core::cfg::Section& cfg)
{
    // Parse command into its components
    std::vector<std::string> args;
    Splitter sp("[[:space:]]+", REG_EXTENDED);
    for (Splitter::const_iterator j = sp.begin(command); j != sp.end(); ++j)
        args.push_back(*j);

    // Build the set of allowed postprocessors
    std::set<std::string> allowed;
    bool has_cfg = cfg.has("postprocess");
    if (has_cfg)
    {
        Splitter sp("[[:space:]]*,[[:space:]]*|[[:space:]]+", REG_EXTENDED);
        std::string pp = cfg.value("postprocess");
        for (Splitter::const_iterator j = sp.begin(pp); j != sp.end(); ++j)
            allowed.insert(*j);
    }

    // Validate the command
    if (args.empty())
        throw std::runtime_error("cannot initialize postprocessing filter: postprocess command is empty");

    std::filesystem::path argv0(args[0]);
    auto scriptname = argv0.filename();
    if (has_cfg && allowed.find(scriptname) == allowed.end())
    {
        stringstream ss;
        ss << "cannot initialize postprocessing filter: postprocess command "
           << command
           << " is not supported by all the requested datasets (allowed postprocessors are: " + str::join(", ", allowed.begin(), allowed.end())
           << ")";
        throw std::runtime_error(ss.str());
    }

    // Expand args[0] to the full pathname and check that the program exists
    args[0] = Config::get().dir_postproc.find_file(argv0, true);

    return args;
}

bool Postprocess::send(std::shared_ptr<Metadata> md, StreamOutput& out)
{
    // if (m_child->subproc.get_stdin() == -1)
    //     return false;

    // Make the data inline, so that the reader on the other side, knows that
    // we are sending that as well
    md->makeInline();

    auto encoded = md->encodeBinary();
    stream::SendResult res = out.send_buffer(encoded.data(), encoded.size());
    if (res.flags & stream::SendResult::SEND_PIPE_EOF_DEST)
        return false;

    // TODO: we can sendfile() here
    const auto& data = md->get_data();
    const auto buf = data.read();
    res = out.send_buffer(buf.data(), buf.size());
    if (res.flags & stream::SendResult::SEND_PIPE_EOF_DEST)
        return false;

    return true;
}

}
}
