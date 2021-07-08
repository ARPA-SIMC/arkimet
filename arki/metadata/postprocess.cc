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

Postprocess::Postprocess(const std::string& command, StreamOutput& out)
    : m_command(command)
{
    // Parse command into its components
    std::vector<std::string> args;
    Splitter sp("[[:space:]]+", REG_EXTENDED);
    for (Splitter::const_iterator j = sp.begin(command); j != sp.end(); ++j)
        args.push_back(*j);

    // Expand args[0] to the full pathname and check that the program exists
    args[0] = Config::get().dir_postproc.find_file(args[0], true);

    m_child = new stream::FilterProcess(args);
    m_child->m_stream = &out;
}

Postprocess::~Postprocess()
{
    // If the child still exists, it means that we have not flushed: be
    // aggressive here to cleanup after whatever error condition has happened
    // in the caller
    if (m_child)
    {
        m_child->terminate();
        delete m_child;
    }
}

void Postprocess::validate(const core::cfg::Section& cfg)
{
    // Build the set of allowed postprocessors
    set<string> allowed;
    bool has_cfg = cfg.has("postprocess");
    if (has_cfg)
    {
        Splitter sp("[[:space:]]*,[[:space:]]*|[[:space:]]+", REG_EXTENDED);
        string pp = cfg.value("postprocess");
        for (Splitter::const_iterator j = sp.begin(pp); j != sp.end(); ++j)
            allowed.insert(*j);
    }

    // Validate the command
    if (m_child->cmd.args.empty())
        throw std::runtime_error("cannot initialize postprocessing filter: postprocess command is empty");
    string scriptname = str::basename(m_child->cmd.args[0]);
    if (has_cfg && allowed.find(scriptname) == allowed.end())
    {
        stringstream ss;
        ss << "cannot initialize postprocessing filter: postprocess command "
           << m_command
           << " is not supported by all the requested datasets (allowed postprocessors are: " + str::join(", ", allowed.begin(), allowed.end())
           << ")";
        throw std::runtime_error(ss.str());
    }
}

void Postprocess::start()
{
    m_child->start();
}

bool Postprocess::process(std::shared_ptr<Metadata> md)
{
    if (m_child->subproc.get_stdin() == -1)
        return false;

    // Make the data inline, so that the reader on the other side, knows that
    // we are sending that as well
    md->makeInline();

    auto encoded = md->encodeBinary();
    if (m_child->send(encoded) < encoded.size())
        return false;

    const auto& data = md->get_data();
    const auto buf = data.read();
    if (m_child->send(buf.data(), buf.size()) < buf.size())
        return false;

    return true;
}

stream::SendResult Postprocess::flush()
{
    std::unique_ptr<stream::FilterProcess> child(m_child);
    m_child = 0;
    child->stop();
    return child->stream_result;
}

}
}
