/*
 * runtime - Common code used in most xgribarch executables
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

#include <arki/runtime/io.h>
#include <arki/metadata/consumer.h>

#include <wibble/exception.h>
#include <wibble/string.h>
#include <cstdlib>
#include <cstring>
#include <sys/types.h>
#include <sys/stat.h>
#include <fcntl.h>

using namespace std;
using namespace wibble;
using namespace wibble::commandline;

namespace arki {
namespace runtime {

Input::Input(wibble::commandline::Parser& opts)
	: m_in(&cin), m_name("(stdin)")
{
	if (opts.hasNext())
	{
		string file = opts.next();
		if (file != "-")
		{
			m_file_in.open(file.c_str(), ios::in);
			if (!m_file_in.is_open() || m_file_in.fail())
				throw wibble::exception::File(file, "opening file for reading");
			m_in = &m_file_in;
			m_name = file;
		}
	}
}

Input::Input(const std::string& file)
	: m_in(&cin), m_name("(stdin)")
{
	if (file != "-")
	{
		m_file_in.open(file.c_str(), ios::in);
		if (!m_file_in.is_open() || m_file_in.fail())
			throw wibble::exception::File(file, "opening file for reading");
		m_in = &m_file_in;
		m_name = file;
	}
}

PosixBufWithHooks::PosixBufWithHooks()
    : pwhook(0)
{
}

int PosixBufWithHooks::sync()
{
    if (pptr() > pbase())
    {
        if (pwhook)
        {
            (*pwhook)();
            pwhook = 0;
        }
    }

    return PosixBuf::sync();
}

Output::Output() : m_out(0), own_stream(true)
{
	openStdout();
}

Output::Output(int fd, const std::string& fname) : m_out(0), own_stream(false)
{
	m_name = fname;
	posixBuf.attach(fd);
	m_out = new ostream(&posixBuf);
}

Output::Output(const std::string& fileName) : m_out(0), own_stream(true)
{
	openFile(fileName);
}

Output::Output(wibble::commandline::Parser& opts) : m_out(0), own_stream(true)
{
	if (opts.hasNext())
	{
		string file = opts.next();
		if (file != "-")
			openFile(file);
	}
	if (!m_out)
		openStdout();
}

Output::Output(wibble::commandline::StringOption& opt) : m_out(0), own_stream(true)
{
	if (opt.isSet())
	{
		string file = opt.value();
		if (file != "-")
			openFile(file);
	}
	if (!m_out)
		openStdout();
}

Output::~Output()
{
	if (!own_stream)
		posixBuf.detach();
	if (m_out) delete m_out;
}

void Output::closeCurrent()
{
	if (!m_out) return;
	int fd = posixBuf.detach();
	if (fd != 1 && !own_stream) ::close(fd);
}

void Output::openStdout()
{
	if (m_name == "-") return;
	closeCurrent();
	m_name = "-";
	posixBuf.attach(1);
	if (!m_out) m_out = new ostream(&posixBuf);
}

void Output::openFile(const std::string& fname, bool append)
{
	if (m_name == fname) return;
	closeCurrent();
	int fd;
	if (append)
		fd = open(fname.c_str(), O_WRONLY | O_CREAT | O_APPEND, 0666);
	else
		fd = open(fname.c_str(), O_WRONLY | O_CREAT | O_TRUNC, 0666);
	if (fd == -1)
		throw wibble::exception::File(fname, "opening file for writing");
	m_name = fname;
	posixBuf.attach(fd);
	if (!m_out) m_out = new ostream(&posixBuf);
}

void Output::set_hook(metadata::Hook& hook)
{
    posixBuf.pwhook = &hook;
}

Tempfile::Tempfile(const std::string& dirname, bool dirname_is_pathname) : m_out(0), m_unlink_on_exit(true)
{
	int fd;
	if (dirname_is_pathname)
	{
		m_name = dirname;
		fd = open(m_name.c_str(), O_RDWR | O_CREAT | O_EXCL, 0666);
		if (fd < 0)
			throw wibble::exception::File(m_name, "creating file");
	} else {
		// Start with the temp dir name
		string tpl = dirname;
		if (tpl.empty())
		{
			char* dir = getenv("ARKI_TMPDIR");
			if (dir != NULL)
				tpl = dir;
			else
			{
				dir = getenv("TMPDIR");
				if (dir != NULL)
					tpl = dir;
				else
					tpl = "/tmp";
			}
		}

		tpl += "/arkimet.XXXXXX";
		char name[tpl.size() + 1];
		memcpy(name, tpl.c_str(), tpl.size() + 1);
		fd = mkstemp(name);
		if (fd < 0)
			throw wibble::exception::File(name, "creating/opening temporary file");
		m_name = name;
	}

	posixBuf.attach(fd);
	if (!m_out) m_out = new ostream(&posixBuf);
}

Tempfile::~Tempfile()
{
	if (m_out) delete m_out;
	if (m_unlink_on_exit)
		::unlink(m_name.c_str());
}

void Tempfile::unlink_on_exit(bool val)
{
	m_unlink_on_exit = val;
}

void Tempfile::unlink()
{
	if (::unlink(m_name.c_str()) < 0)
		throw wibble::exception::File(m_name, "deleting file");
}

}
}
// vim:set ts=4 sw=4:
