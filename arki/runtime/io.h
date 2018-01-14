#ifndef ARKI_RUNTIME_IO_H
#define ARKI_RUNTIME_IO_H

/// Common I/O code used in most arkimet executables

#include <arki/utils/commandline/parser.h>
#include <arki/core/file.h>
#include <arki/defs.h>
#include <string>

namespace arki {
namespace metadata {
struct Hook;
}
namespace runtime {

struct InputFile : public core::File
{
    InputFile(const std::string& file);
};

struct File : public core::File
{
    File(const std::string pathname, bool append=false);
};

/**
 * Open an input file.
 *
 * If there is a commandline parameter available in the parser, use that as a
 * file name; else use the standard input.
 */
std::unique_ptr<core::NamedFileDescriptor> make_input(utils::commandline::Parser& opts);
std::unique_ptr<core::NamedFileDescriptor> make_output(utils::commandline::Parser& opts);
std::unique_ptr<core::NamedFileDescriptor> make_output(utils::commandline::StringOption& opt);


/**
 * Open a temporary file.
 *
 * If a path is not given, $ARKI_TMPDIR is tried, then $TMPDIR, then /tmp
 *
 * By default, the temporary file will be deleted when the object is deleted.
 */
class Tempfile : public core::File
{
protected:
    bool m_unlink_on_exit = true;

public:
    Tempfile();
    ~Tempfile();

    /// Change the unlink-on-exit behaviour
    void unlink_on_exit(bool val);

    /// Unlink the file right now
    void unlink();
};

}
}
#endif
