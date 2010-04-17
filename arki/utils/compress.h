#ifndef ARKI_UTILS_COMPRESS_H
#define ARKI_UTILS_COMPRESS_H

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

#include <wibble/sys/buffer.h>

// zlib forward declaration
struct z_stream_s;
typedef struct z_stream_s z_stream;

namespace arki {
namespace utils {

/**
 * Compression/decompression algorithms.
 *
 * If an algorithm is not implemented, it will return the uncompressed data.
 *
 * It makes sense to always check that the compressed data is actually shorter
 * than the uncompressed one, and if not, keep using the uncompressed data.
 */
namespace compress {

/**
 * Compress with LZO.
 *
 * Fast.
 */
wibble::sys::Buffer lzo(const void* in, size_t in_size);

/**
 * Uncompress LZO compressed data.
 *
 * @param size
 *   The size of the uncompressed data
 */
wibble::sys::Buffer unlzo(const void* in, size_t in_size, size_t out_size);

/**
 * Compressor engine based on Zlib
 */
class ZlibCompressor
{
protected:
	z_stream* strm;

public:
	ZlibCompressor();
	~ZlibCompressor();

	/**
	 * Set the data for the encoder/decoder
	 */
	void feedData(void* buf, size_t len);

	/**
	 * Run an encoder loop filling in the given buffer
	 * 
	 * @returns the count of data written (if the same as len, you need to
	 *          call run() again before feedData)
	 */
	size_t get(void* buf, size_t len, bool flush = false);
	size_t get(wibble::sys::Buffer& buf, bool flush = false);

	/**
	 * Restart compression after a flush
	 */
	void restart();
};

}
}
}

// vim:set ts=4 sw=4:
#endif
