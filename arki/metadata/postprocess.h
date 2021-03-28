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
namespace metadata {
namespace postproc {
class Child;
}

class Postprocess
{
protected:
    /// Subprocess that filters our data
    postproc::Child* m_child = nullptr;
    /// Command line run in the subprocess
    std::string m_command;
    /// Captured stderr from the child (unless sent elsewhere)
    std::stringstream m_errors;

public:
    /**
     * Create a postprocessor running the command \a command, and sending the
     * output to the file descriptor \a outfd.
     *
     * The configuration \a cfg is used to validate if command is allowed or
     * not.
     */
    Postprocess(const std::string& command);
    virtual ~Postprocess();

    /**
     * Validate this postprocessor against the given configuration
     */
    void validate(const core::cfg::Section& cfg);

    /// Fork the child process setting up the various pipes
    void start();

    /// Set the output file descriptor where we send data coming from the postprocessor
    void set_output(StreamOutput& out);

    /// Set the output stream where we send the postprocessor stderr
    void set_error(std::ostream& err);

    // Process one metadata
    bool process(std::shared_ptr<Metadata> md);

    // End of processing: flush all pending data
    void flush();
};

}
}
#endif
