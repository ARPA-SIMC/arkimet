/*
 * utils/compress - Compression/decompression utilities
 *
 * Copyright (C) 2010  ARPA-SIM <urpsim@smr.arpa.emr.it>
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

#include <arki/utils/compress.h>
#include <wibble/exception.h>

#include "config.h"

#ifdef HAVE_LZO
#include "lzo/lzoconf.h"
#include "lzo/lzo1x.h"
#endif

using namespace std;
using namespace wibble;

namespace arki {
namespace utils {
namespace compress {

static void require_lzo_init()
{
	static bool done = false;
	if (!done)
	{
		if (lzo_init() != LZO_E_OK)
			throw wibble::exception::Consistency("initializing LZO library",
					"lzo_init() failed (this usually indicates a compiler bug)");
		done = true;
	}
}


wibble::sys::Buffer lzo(const void* in, size_t in_size)
{
#ifdef HAVE_LZO
	require_lzo_init();

	// LZO work memory
	sys::Buffer wrkmem(LZO1X_1_MEM_COMPRESS);

	// Output buffer
	sys::Buffer out(in_size + in_size / 16 + 64 + 3);
	lzo_uint out_len = out.size();

	// Compress
	int r = lzo1x_1_compress(
			(lzo_bytep)in, in_size,
			(lzo_bytep)out.data(), &out_len,
			(lzo_bytep)wrkmem.data());
	if (r != LZO_E_OK)
		throw wibble::exception::Consistency("compressing data with LZO",
				"LZO internal error " + str::fmt(r));

	// If the size did not decrease, return the uncompressed data
	if (out_len >= in_size)
		return sys::Buffer(in, in_size);

	// Resize output to match the compressed length
	out.resize(out_len);

	return out;
#else
	return sys::Buffer(in, in_size);
#endif
}

wibble::sys::Buffer unlzo(const void* in, size_t in_size, size_t out_size)
{
#ifdef HAVE_LZO
	require_lzo_init();

	sys::Buffer out(out_size);
	lzo_uint new_len = out_size;
	int r = lzo1x_decompress_safe((lzo_bytep)in, in_size, (lzo_bytep)out.data(), &new_len, NULL);
	if (r != LZO_E_OK || new_len != out_size)
		throw wibble::exception::Consistency("decompressing data with LZO",
				"LZO internal error " + str::fmt(r));

	return out;
#else
	throw wibble::exception::Consistency(
			"decompressing data with LZO",
			"LZO support was not available at compile time");
#endif
}

}
}
}
// vim:set ts=4 sw=4:
