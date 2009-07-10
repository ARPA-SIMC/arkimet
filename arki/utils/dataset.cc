/*
 * utils/dataset - Useful functions for working with datasets
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

#include <arki/utils/dataset.h>

#include <wibble/sys/signal.h>
#include <wibble/sys/buffer.h>

using namespace std;
using namespace wibble;

namespace arki {
namespace utils {
namespace ds {

bool DataInliner::operator()(Metadata& md)
{
	// Read the data
	wibble::sys::Buffer buf = md.getData();
	// Change the source as inline
	md.setInlineData(md.source->format, buf);
	return next(md);
}

bool DataOnly::operator()(Metadata& md)
{
	wibble::sys::Buffer buf = md.getData();
	sys::sig::ProcMask pm(blocked);
	out.write((const char*)buf.data(), buf.size());
	out.flush();
	return true;
}

}
}
}
// vim:set ts=4 sw=4:
