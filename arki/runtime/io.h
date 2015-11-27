#ifndef ARKI_RUNTIME_IO_H
#define ARKI_RUNTIME_IO_H

/// Common I/O code used in most arkimet executables

#include <arki/utils/commandline/parser.h>
#include <arki/wibble/stream/posix.h>
#include <arki/defs.h>
#include <arki/core.h>
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

struct PosixBufWithHooks : public wibble::stream::PosixBuf
{
    /**
     * Called when the PosixBuf is about to write data
     *
     * @return true if it should still be called, false if there is no need
     * anymore
     */
    metadata::Hook* pwhook;

    PosixBufWithHooks();

protected:
    virtual int sync();
};

/**
 * Open an output file.
 *
 * If there is a commandline parameter available in the parser, use that as a
 * file name; else use the standard output.
 *
 * If a file is used, in case it already exists it will be overwritten.
 */
class Output : public arki::Output
{
	PosixBufWithHooks posixBuf;
	std::ostream *m_out;
	std::string m_name;
	void closeCurrent();
	bool own_stream;

public:
	Output();
	// Output directed to a file
	Output(const std::string& fileName);
	// Output directed to an open file descriptor
	Output(int fd, const std::string& fname);
	Output(arki::utils::commandline::Parser& opts);
	Output(arki::utils::commandline::StringOption& opt);
	~Output();

	// Close existing file (if any) and open a new one
	void openFile(const std::string& fname, bool append = false);
	void openStdout();

    /**
     * Set a hook for the posixBuf, valid until it returns false.
     *
     * This replaces an existing buf, if present.
     */
    void set_hook(metadata::Hook& hook);

    std::ostream& stream() override { return *m_out; }
    const std::string& name() const override { return m_name; }
};

/**
 * Open a temporary file.
 *
 * If a path is not given, $ARKI_TMPDIR is tried, then $TMPDIR, then /tmp
 *
 * By default, the temporary file will be deleted when the object is deleted.
 */
class Tempfile : public arki::Output
{
	wibble::stream::PosixBuf posixBuf;
	std::ostream *m_out;
	std::string m_name;
	bool m_unlink_on_exit;

public:
	Tempfile(const std::string& dirname = std::string(), bool dirname_is_pathname = false);
	~Tempfile();

	/// Change the unlink-on-exit behaviour
	void unlink_on_exit(bool val);

	/// Unlink the file right now
	void unlink();

    std::ostream& stream() override { return *m_out; }
    const std::string& name() const override { return m_name; }
};

}
}

// vim:set ts=4 sw=4:
#endif
