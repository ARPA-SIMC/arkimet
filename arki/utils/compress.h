#ifndef ARKI_UTILS_COMPRESS_H
#define ARKI_UTILS_COMPRESS_H

/*
 * utils/compress - Compression/decompression utilities
 *
 * Copyright (C) 2010--2015  ARPA-SIM <urpsim@smr.arpa.emr.it>
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

#include <arki/libconfig.h>
#include <arki/metadata/consumer.h>
#include <wibble/sys/buffer.h>
#include <sys/types.h>
#include <string>
#include <vector>

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

/**
 * Gunzip the file opened at \a rdfd sending data to the file opened at \a wrfd
 */
void gunzip(int rdfd, const std::string& rdfname, int wrfd, const std::string& wrfname, size_t bufsize = 4096);

/**
 * At constructor time, create an uncompressed version of the given file
 *
 * At destructor time, delete the uncompressed version
 */
struct TempUnzip
{
	std::string fname;

	/**
	 * @param fname
	 *   Refers to the uncompressed file name (i.e. without the trailing .gz)
	 */
	TempUnzip(const std::string& fname);
	~TempUnzip();
};

struct SeekIndex
{
	std::vector<size_t> ofs_unc;
	std::vector<size_t> ofs_comp;

	/// Return the index of the block containing the given uncompressed
	/// offset
	size_t lookup(size_t unc) const;

	/// Read the index from the given file descriptor
	void read(int fd, const std::string& fname);

	/**
	 * Read the index from the given file
	 *
	 * @returns true if the index was read, false if the index does not
	 * exists, throws an exception in case of other errors
	 */
	bool read(const std::string& fname);
};

/**
 * Return the uncompressed size of a file
 *
 * @param fnam
 *   Refers to the uncompressed file name (i.e. without the trailing .gz)
 */
off_t filesize(const std::string& file);

/**
 * Create a file with a compressed version of the data described by the
 * metadata that it receives.
 *
 * It also creates a compressed file index for faster seeking in the compressed
 * file.
 */
class DataCompressor : public metadata::Eater
{
protected:
	// Output file name
	std::string outfile;
	// Number of data items in a compressed block
	size_t groupsize;
	// Compressed output
	int outfd;
	// Index output
	int outidx;
	// Compressor
	ZlibCompressor compressor;
	// Output buffer for the compressor
	wibble::sys::Buffer outbuf;
	// Offset of end of last uncompressed data read
	off_t unc_ofs;
	// Offset of end of last uncompressed block written
	off_t last_unc_ofs;
	// Offset of end of last compressed data written
	off_t last_ofs;
	// Number of data compressed so far
	size_t count;

	// End one compressed block
	void endBlock(bool final=false);

public:
	DataCompressor(const std::string& outfile, size_t groupsize = 512);
	~DataCompressor();

    bool eat(std::auto_ptr<Metadata> md) override;

    void add(wibble::sys::Buffer buf);

	void flush();
};

}
}
}

// vim:set ts=4 sw=4:
#endif
