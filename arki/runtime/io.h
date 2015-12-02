#ifndef ARKI_RUNTIME_IO_H
#define ARKI_RUNTIME_IO_H

/// Common I/O code used in most arkimet executables

#include <arki/utils/commandline/parser.h>
#include <arki/utils/sys.h>
#include <arki/defs.h>
#include <fstream>
#include <string>

namespace arki {
namespace metadata {
struct Hook;
}
namespace runtime {

/**
 * Open an input file.
 *
 * If there is a commandline parameter available in the parser, use that as a
 * file name; else use the standard input.
 */
class Input
{
	std::istream* m_in;
	std::ifstream m_file_in;
	std::string m_name;

public:
	Input(arki::utils::commandline::Parser& opts);
	Input(const std::string& file);

	std::istream& stream() { return *m_in; }
	const std::string& name() const { return m_name; }
};

struct Stdout : public utils::sys::NamedFileDescriptor
{
    Stdout();
};

struct Stderr : public utils::sys::NamedFileDescriptor
{
    Stderr();
};

struct File : public utils::sys::File
{
    File(const std::string pathname, bool append=false);
};

std::unique_ptr<utils::sys::NamedFileDescriptor> make_output(utils::commandline::Parser& opts);
std::unique_ptr<utils::sys::NamedFileDescriptor> make_output(utils::commandline::StringOption& opt);


/**
 * Open a temporary file.
 *
 * If a path is not given, $ARKI_TMPDIR is tried, then $TMPDIR, then /tmp
 *
 * By default, the temporary file will be deleted when the object is deleted.
 */
class Tempfile : public utils::sys::File
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
