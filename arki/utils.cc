/*
 * utils - General utility functions
 *
 * Copyright (C) 2007--2011  ARPA-SIM <urpsim@smr.arpa.emr.it>
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

#define _XOPEN_SOURCE 700
#define _XOPEN_SOURCE_EXTENDED 1

#include <arki/utils.h>
#include <wibble/sys/fs.h>
#include <wibble/sys/process.h>

#include <fstream>
#include <cctype>
#include <cstdlib>
#include <cstring>
#include <sys/types.h>
#include <sys/stat.h>
#include <fcntl.h>
#include <unistd.h>
#include <errno.h>
#include "config.h"

using namespace std;
using namespace wibble;

#ifndef HAVE_MKDTEMP
/* Replacement mkdtemp if not provided by libc */
char *mkdtemp(char *dir_template) {
  if (dir_template == NULL) { errno=EINVAL; return NULL; };
  if (strlen(dir_template) < 6 ) { errno=EINVAL; return NULL; };
  if (strcmp(dir_template + strlen(dir_template) - 6, "XXXXXX") != 0)
{ errno=EINVAL; return NULL; };
  char * pos = dir_template + strlen(dir_template) - 6;

  do {
      int num = rand() % 1000000;
      int res = 0;
      sprintf(pos, "%06d", num);
      res = mkdir(dir_template, 0755);
      if (res == 0) { return dir_template; }; // success
      if (errno == EEXIST) { continue; }; // try with a different number
      return NULL; // pass mkdir errorcode otherwise
  } while (0);
  return NULL;
}
#endif

namespace arki {
namespace utils {

std::string readFile(std::istream& input, const std::string& filename)
{
	static const size_t bufsize = 4096;
	char buf[bufsize];
	string res;
	while (true)
	{
		input.read(buf, bufsize);
		res += string(buf, input.gcount());
		if (input.eof())
			break;
		if (input.fail())
			throw wibble::exception::File(filename, "reading data");
	}
	return res;
}

void createFlagfile(const std::string& pathname)
{
	int fd = ::open(pathname.c_str(), O_WRONLY | O_CREAT | O_NOCTTY, 0666);
	if (fd == -1)
		throw wibble::exception::File(pathname, "opening/creating file");
	::close(fd);
}

void createNewFlagfile(const std::string& pathname)
{
	int fd = ::open(pathname.c_str(), O_WRONLY | O_CREAT | O_NOCTTY | O_EXCL, 0666);
	if (fd == -1)
		throw wibble::exception::File(pathname, "creating file");
	::close(fd);
}

void hexdump(const char* name, const std::string& str)
{
	fprintf(stderr, "%s ", name);
	for (string::const_iterator i = str.begin(); i != str.end(); ++i)
	{
		fprintf(stderr, "%02x ", (int)(unsigned char)*i);
	}
	fprintf(stderr, "\n");
}

void hexdump(const char* name, const unsigned char* str, int len)
{
	fprintf(stderr, "%s ", name);
	for (int i = 0; i < len; ++i)
	{
		fprintf(stderr, "%02x ", str[i]);
	}
	fprintf(stderr, "\n");
}


MoveToTempDir::MoveToTempDir(const std::string& pattern)
{
    old_dir = sys::process::getcwd();
    char buf[pattern.size() + 1];
    memcpy(buf, pattern.c_str(), pattern.size() + 1);
    if (mkdtemp(buf) == NULL)
        throw wibble::exception::System("cannot create temporary directory");
    tmp_dir = buf;
    sys::process::chdir(tmp_dir);
}

MoveToTempDir::~MoveToTempDir()
{
    sys::process::chdir(old_dir);
    sys::fs::rmtree(tmp_dir);
}

}
}
// vim:set ts=4 sw=4:
