#ifndef ARKI_RUNTIME_IO_H
#define ARKI_RUNTIME_IO_H

/*
 * runtime/io - Common I/O code used in most arkimet executables
 *
 * Copyright (C) 2007--2010  ARPA-SIM <urpsim@smr.arpa.emr.it>
 *
 * This program is free software; you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation; either version 2 of the License, or
 * (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License along
 * with this program; if not, write to the Free Software Foundation, Inc.,
 * 51 Franklin Street, Fifth Floor, Boston, MA 02110-1301 USA.
 *
 * Author: Enrico Zini <enrico@enricozini.com>
 */

#include <wibble/commandline/parser.h>
#include <wibble/stream/posix.h>
#include <fstream>
#include <string>

namespace arki {
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
	Input(wibble::commandline::Parser& opts);
	Input(const std::string& file);

	std::istream& stream() { return *m_in; }
	const std::string& name() const { return m_name; }
};

/**
 * Open an output file.
 *
 * If there is a commandline parameter available in the parser, use that as a
 * file name; else use the standard output.
 *
 * If a file is used, in case it already exists it will be overwritten.
 */
class Output
{
	wibble::stream::PosixBuf posixBuf;
	std::ostream *m_out;
	std::string m_name;
	void closeCurrent();

public:
	Output();
	Output(const std::string& fileName);
	Output(wibble::commandline::Parser& opts);
	Output(wibble::commandline::StringOption& opt);
	~Output();

	// Close existing file (if any) and 
	void openFile(const std::string& fname, bool append = false);
	void openStdout();

	std::ostream& stream() { return *m_out; }
	const std::string& name() const { return m_name; }
};

/**
 * Open a temporary file.
 *
 * If a path is not given, $ARKI_TMPDIR is tried, then $TMPDIR, then /tmp
 */
class Tempfile
{
	wibble::stream::PosixBuf posixBuf;
	std::ostream *m_out;
	std::string m_name;
	bool m_unlink_on_exit;

public:
	Tempfile(const std::string& dirname = std::string());
	~Tempfile();

	void unlink_on_exit(bool val);
	void unlink();

	std::ostream& stream() { return *m_out; }
	const std::string& name() const { return m_name; }
};

}
}

// vim:set ts=4 sw=4:
#endif
