/*
 * scan/dir - Find data files inside directories
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

#include "config.h"
#include <arki/scan/dir.h>
#include <arki/scan/any.h>

#include <wibble/exception.h>
#include <wibble/string.h>
#include <wibble/sys/fs.h>

#include <sys/types.h>
#include <sys/stat.h>
#include <fcntl.h>
#include <unistd.h>
#include <cerrno>

using namespace std;
using namespace wibble;

namespace arki {
namespace scan {

static void scan(vector<string>& res, const std::string& top, const std::string& root, bool files_in_root, int level = 0)
{
	sys::fs::Directory dir(root);

	for (sys::fs::Directory::const_iterator i = dir.begin();
			i != dir.end(); ++i)
	{
		// Skip '.', '..' and hidden files
		if ((*i)[0] == '.')
			continue;
		// Skip compressed data index files
		if (str::endsWith(*i, ".gz.idx"))
			continue;

        // stat(2) the file
        string pathname = str::joinpath(root, *i);

        if (i.isdir())
        {
            // If it is a directory, recurse into it
            scan(res, top, pathname, files_in_root, level + 1);
        } else if ((files_in_root || level > 0) && i.isreg()) {
            if (str::endsWith(pathname, ".gz"))
                pathname = pathname.substr(0, pathname.size() - 3);
            if (scan::canScan(pathname))
                // Skip files in the root dir
                // We point to a good file, keep it
                res.push_back(pathname.substr(top.size() + 1));
        }
    }
}

std::vector<std::string> dir(const std::string& root, bool files_in_root)
{
	string m_root = root;

	// Trim trailing '/'
	while (m_root.size() > 1 and m_root[m_root.size()-1] == '/')
		m_root.resize(m_root.size() - 1);

	vector<string> res;
	scan(res, m_root, m_root, files_in_root);
	return res;
}

}
}
// vim:set ts=4 sw=4:
