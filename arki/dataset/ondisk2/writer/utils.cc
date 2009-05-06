/*
 * dataset/ondisk2/writer/utils - ondisk2 maintenance utilities
 *
 * Copyright (C) 2007,2008,2009  ARPA-SIM <urpsim@smr.arpa.emr.it>
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

#include "config.h"
#include <arki/dataset/ondisk2/writer/utils.h>

#include <wibble/exception.h>
#include <wibble/string.h>
#include <wibble/sys/fs.h>

#include <algorithm>

#include <sys/types.h>
#include <sys/stat.h>
#include <fcntl.h>
#include <unistd.h>
#include <cerrno>

using namespace std;
using namespace wibble;

namespace arki {
namespace dataset {
namespace ondisk2 {
namespace writer {

bool sorter(const std::string& a, const std::string& b)
{
	return b < a;
}

DirScanner::DirScanner(const std::string& root)
	: m_root(root)
{
	// Trim trailing '/'
	while (m_root.size() > 1 and m_root[m_root.size()-1] == '/')
		m_root.resize(m_root.size() - 1);

	scan(m_root);

	sort(names.begin(), names.end(), sorter);
}

void DirScanner::scan(const std::string& root, int level)
{
	sys::fs::Directory dir(root);

	for (sys::fs::Directory::const_iterator i = dir.begin();
			i != dir.end(); ++i)
	{
		// Skip '.', '..' and hidden files
		if ((*i)[0] == '.')
			continue;

		// stat(2) the file
		string pathname = str::joinpath(root, *i);
		std::auto_ptr<struct stat> st = sys::fs::stat(pathname);

		if (S_ISDIR(st->st_mode))
		{
			// If it is a directory, recurse into it
			scan(pathname, level + 1);
		} else if (level > 0 && S_ISREG(st->st_mode)) {
			// Skip files in the root dir
			// We point to a good file, keep it
			names.push_back(pathname.substr(m_root.size() + 1));
		}
	}
}

std::string DirScanner::cur() const
{
	if (names.empty())
		return string();
	else
		return names.back();
}

void DirScanner::next()
{
	if (!names.empty())
		names.pop_back();
}

}
}
}
}
// vim:set ts=4 sw=4:
