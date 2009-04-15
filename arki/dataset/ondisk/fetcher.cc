/*
 * dataset/ondisk/fetcher - Optimised data fetcher
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

#include <arki/dataset/ondisk/fetcher.h>
#include <wibble/exception.h>
#include <wibble/string.h>
// TODO: remove these two headers when possible
#include <arki/types/origin.h>
#include <arki/types/product.h>
#include <arki/utils.h>
#include <arki/utils/files.h>
#include <wibble/string.h>
#include <cstdio>

using namespace std;
using namespace wibble;
using namespace arki::utils::files;

namespace arki {
namespace dataset {
namespace ondisk {

Fetcher::Fetcher(const std::string& root) : m_root(root)
{
	m_md.create();
}

Fetcher::~Fetcher()
{
	if (!m_cur.empty())
		m_in.close();
}

void Fetcher::fetch(const std::string& file, int offset, MetadataConsumer& out)
{
	// Open/reopen the input file if needed
	if (file != m_cur)
	{
		if (!m_cur.empty())
			m_in.close();
		m_cur.clear();
		// Skip if the appending flagfile is there
		if (hasRebuildFlagfile(wibble::str::joinpath(m_root, file)))
		{
			skipFile = true;
		} else {
			string filename = wibble::str::joinpath(m_root, file + ".metadata");
			m_in.open(filename.c_str(), ios::in);
			if (!m_in.is_open() || !m_in.good())
				throw wibble::exception::File(filename, "opening file for reading");
			skipFile = false;
		}
		m_cur = file;
	}
	if (skipFile)
		return;
	m_in.seekg(offset);
	if (!m_in.good())
		throw wibble::exception::File(m_cur + ".metadata", "seeking to position " + str::fmt(offset));
	m_md.read(m_in, m_cur + ".metadata");
	m_md.prependPath(wibble::str::dirname(m_cur));
	out(m_md);
}

}
}
}
// vim:set ts=4 sw=4:
