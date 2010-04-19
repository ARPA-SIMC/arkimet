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
#include <arki/utils.h>
#include <wibble/exception.h>

#include "config.h"

#include <sys/types.h>
#include <sys/stat.h>
#include <fcntl.h>
#include <utime.h>

#include <zlib.h>

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

ZlibCompressor::ZlibCompressor() : strm(0)
{
	/* allocate deflate state */
	strm = new z_stream;
	strm->zalloc = Z_NULL;
	strm->zfree = Z_NULL;
	strm->opaque = Z_NULL;
	int ret = deflateInit2(strm, 9, Z_DEFLATED, 15 + 16, 9, Z_DEFAULT_STRATEGY);
	if (ret != Z_OK)
		throw wibble::exception::Consistency("initializing zlib", "initialization failed");
}

ZlibCompressor::~ZlibCompressor()
{
	if (strm)
	{
		(void)deflateEnd(strm);
		delete strm;
	}
}

void ZlibCompressor::feedData(void* buf, size_t len)
{
	strm->avail_in = len;
	strm->next_in = (Bytef*)buf;
}

size_t ZlibCompressor::get(void* buf, size_t len, bool flush)
{
	int z_flush = flush ? Z_FINISH : Z_NO_FLUSH;
	strm->avail_out = len;
	strm->next_out = (Bytef*)buf;
	int ret = deflate(strm, z_flush);    /* no bad return value */
	if (ret == Z_STREAM_ERROR)
		throw wibble::exception::Consistency("running zlib deflate", "run failed");
	return len - strm->avail_out;
}

size_t ZlibCompressor::get(wibble::sys::Buffer& buf, bool flush)
{
	return get(buf.data(), buf.size(), flush);
}

void ZlibCompressor::restart()
{
	if (deflateReset(strm) != Z_OK)
		throw wibble::exception::Consistency("resetting zlib deflate stream", "stream error");
}

void gunzip(int rdfd, const std::string& rdfname, int wrfd, const std::string& wrfname, size_t bufsize)
{
	// (Re)open the compressed file
	int rdfd1 = dup(rdfd);
	gzFile gzfd = gzdopen(rdfd1, "rb");
	if (gzfd == NULL)
		throw wibble::exception::File(rdfname, "opening file with zlib");
	
	char* buf = new char[bufsize];

	try {
		while (true)
		{
			int res = gzread(gzfd, buf, bufsize);
			if (res < 0)
				throw wibble::exception::Consistency("reading from " + rdfname, "read failed");

			ssize_t wres = write(wrfd, buf, res);
			if (wres < 0)
				throw wibble::exception::File(wrfname, "write failed");
			if (wres != res)
				throw wibble::exception::Consistency("writing to " + wrfname, "wrote only " + str::fmt(wres) + "/" + str::fmt(res) + " bytes");

			if ((size_t)res < bufsize)
				break;
		}
	} catch (...) {
		delete[] buf;
		gzclose(gzfd);
		throw;
	}

	delete[] buf;
	gzclose(gzfd);

	// Let the caller close file descriptors
}

TempUnzip::TempUnzip(const std::string& fname)
	: fname(fname)
{
	// zcat gzfname > fname
	string gzfname = fname + ".gz";
	int rdfd = open(gzfname.c_str(), O_RDONLY);
	utils::HandleWatch hwrd(gzfname, rdfd);

	int wrfd = open(fname.c_str(), O_WRONLY | O_CREAT | O_EXCL, 0666);
	utils::HandleWatch hwwr(fname, wrfd);

	gunzip(rdfd, gzfname, wrfd, fname);

	// Set the same timestamp as the compressed file
	std::auto_ptr<struct stat> st = sys::fs::stat(gzfname);
	struct utimbuf times;
	times.actime = st->st_atime;
	times.modtime = st->st_mtime;
	utime(fname.c_str(), &times);
}

TempUnzip::~TempUnzip()
{
	::unlink(fname.c_str());
}

}
}
}
// vim:set ts=4 sw=4:
