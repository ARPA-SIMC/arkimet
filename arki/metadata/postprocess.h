#ifndef ARKI_METADATA_POSTPROCESS_H
#define ARKI_METADATA_POSTPROCESS_H

#include <arki/core/fwd.h>
#include <arki/stream/fwd.h>
#include <arki/metadata/fwd.h>
#include <string>
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
protected:
    /// Subprocess that filters our data
    stream::FilterProcess* m_child = nullptr;
    /// Command line run in the subprocess
    std::string m_command;

public:
    /**
     * Create a postprocessor running the command \a command, and sending the
     * output to the file descriptor \a outfd.
     *
     * The current runtime configuration is used to validate if command is
     * allowed or not.
     *
     * \a out is the output stream where we send data coming from the
     * postprocessor
     */
    Postprocess(const std::string& command, StreamOutput& out);
    virtual ~Postprocess();

    /**
     * Validate this postprocessor against the given configuration
     */
    void validate(const core::cfg::Section& cfg);

    /// Fork the child process setting up the various pipes
    void start();

    // Process one metadata
    bool process(std::shared_ptr<Metadata> md);

    // End of processing: flush all pending data
    stream::SendResult flush();
};

}
}
#endif
