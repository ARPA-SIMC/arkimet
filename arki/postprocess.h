#ifndef ARKI_POSTPROCESS_H
#define ARKI_POSTPROCESS_H

#include <string>
#include <map>
#include <sstream>
#include <functional>
#include <memory>

namespace arki {
class Metadata;

namespace postproc {
class Child;
}

class Postprocess
{
protected:
    /// Subprocess that filters our data
    postproc::Child* m_child;
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
    /*
    Postprocess(const std::string& command, int outfd, const std::map<std::string, std::string>& cfg);
    Postprocess(const std::string& command, std::ostream& out);
    Postprocess(const std::string& command, std::ostream& out, const std::map<std::string, std::string>& cfg);
    */
    virtual ~Postprocess();

    /**
     * Validate this postprocessor against the given configuration
     */
    void validate(const std::map<std::string, std::string>& cfg);

    /// Fork the child process setting up the various pipes
    void start();

    /// Set the output file descriptor where we send data coming from the postprocessor
    void set_output(int outfd);

    /// Set the output stream where we send the postprocessor stderr
    void set_error(std::ostream& err);

    /**
     * Set hook to be called when the child process has produced its first
     * data, just before the data is sent to the next consumer
     */
    void set_data_start_hook(std::function<void()> hook);

    // Process one metadata
    bool process(std::unique_ptr<Metadata>&& md);

    // End of processing: flush all pending data
    void flush();
};


}

// vim:set ts=4 sw=4:
#endif
