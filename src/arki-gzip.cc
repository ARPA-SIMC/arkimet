/*
 * arki-gzip - Compress data files
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
 * You should have received a copy of the GNU General Public License
 * along with this program; if not, write to the Free Software
 * Foundation, Inc., 51 Franklin St, Fifth Floor, Boston, MA  02110-1301 USA
 *
 * Author: Enrico Zini <enrico@enricozini.com>
 */

#include <wibble/exception.h>
#include <wibble/commandline/parser.h>
#include <wibble/sys/fs.h>
#include <arki/metadata.h>
#include <arki/utils/metadata.h>
#include <arki/runtime.h>

#include <fstream>
#include <iostream>
#include <cstring>
#include <cerrno>

#include <sys/types.h>
#include <sys/stat.h>
#include <fcntl.h>
#include <arpa/inet.h>

#include <zlib.h>

#include "config.h"

using namespace std;
using namespace arki;
using namespace wibble;

namespace wibble {
namespace commandline {

struct Options : public StandardParserWithManpage
{
	Options() : StandardParserWithManpage("arki-gzip", PACKAGE_VERSION, 1, PACKAGE_BUGREPORT)
	{
		usage = "[options] [input,d [inputmd...]]";
		description =
			"Gzip the data file pointed by the given metadata files";
	}
};

}
}

class Compressor
{
protected:
	z_stream strm;

public:
	Compressor()
	{
		/* allocate deflate state */
		strm.zalloc = Z_NULL;
		strm.zfree = Z_NULL;
		strm.opaque = Z_NULL;
		int ret = deflateInit2(&strm, 9, Z_DEFLATED, 15 + 16, 9, Z_DEFAULT_STRATEGY);
		if (ret != Z_OK)
			throw wibble::exception::Consistency("initializing zlib", "initialization failed");
	}

	~Compressor()
	{
		(void)deflateEnd(&strm);
	}

	/**
	 * Set the data for the encoder/decoder
	 */
	void feedData(void* buf, size_t len)
	{
		strm.avail_in = len;
		strm.next_in = (Bytef*)buf;
	}

	/**
	 * Run an encoder loop filling in the given buffer
	 * 
	 * @returns the count of data written (if the same as len, you need to
	 *          call run() again before feedData)
	 */
	size_t get(void* buf, size_t len, bool flush = false)
	{
		int z_flush = flush ? Z_FINISH : Z_NO_FLUSH;
		strm.avail_out = len;
		strm.next_out = (Bytef*)buf;
		int ret = deflate(&strm, z_flush);    /* no bad return value */
		if (ret == Z_STREAM_ERROR)
			throw wibble::exception::Consistency("running zlib deflate", "run failed");
		return len - strm.avail_out;
	}

	size_t get(wibble::sys::Buffer& buf, bool flush = false)
	{
		return get(buf.data(), buf.size(), flush);
	}

	void restart()
	{
		if (deflateReset(&strm) != Z_OK)
			throw wibble::exception::Consistency("resetting zlib deflate stream", "stream error");
	}
};

static void do_mdfile(const std::string& mdfile, size_t groupsize = 128)
{
	using namespace utils::metadata;

	Collector mdc;
	Metadata::readFile(mdfile, mdc);

	// Check that the metadata completely cover the data file
	string last_file;
	size_t last_end = 0;
	for (Collector::const_iterator i = mdc.begin(); i != mdc.end(); ++i)
	{
		Item<types::source::Blob> s = i->source.upcast<types::source::Blob>();
		if (s->offset != last_end)
			throw wibble::exception::Consistency("validating metadata file " + mdfile,
					"metadata element points to data that does not start at the end of the previous element");
		if (i == mdc.begin())
		{
			last_file = s->filename;
		} else {
			if (last_file != s->filename)
				throw wibble::exception::Consistency("validating metadata file " + mdfile,
						"metadata element points at another data file (previous: " + last_file + ", this: " + s->filename + ")");
		}
		last_end += s->size;
	}
	std::auto_ptr<struct stat> st = sys::fs::stat(last_file);
	if (st->st_size != last_end)
		throw wibble::exception::Consistency("validating metadata file " + mdfile,
				"metadata do not cover the entire data file");

	// Open output compressed file
	string outfname = last_file + ".gz";
	int outfd = open(outfname.c_str(), O_WRONLY | O_CREAT | O_EXCL, 0666);
	if (outfd < 0)
		throw wibble::exception::File(outfname, "creating file");

	// Open output index
	string idxfname = last_file + ".gz.idx";
	int outidx = open(idxfname.c_str(), O_WRONLY | O_CREAT | O_EXCL, 0666);
	if (outidx < 0)
		throw wibble::exception::File(idxfname, "creating file");

	// Start compressor
	Compressor compressor;

	sys::Buffer outbuf(4096*2);

	// Compress data
	off_t last_ofs = 0;
	for (size_t i = 0; i < mdc.size(); ++i)
	{
//		fprintf(stderr, "W %zd\n", i);
		wibble::sys::Buffer buf = mdc[i].getData();
		bool flush = (i == mdc.size() - 1) || (i > 0 && (i % groupsize) == 0);

//fprintf(stderr, "ITEM %zd IN %zd FLUSH %d\n", i, buf.size(), (int)flush);
		compressor.feedData(buf.data(), buf.size());
		while (true)
		{
			size_t len = compressor.get(outbuf, flush);
//fprintf(stderr, " OUT %zd\n", len);
			if (len > 0)
			{
				if (write(outfd, outbuf.data(), len) != len)
					throw wibble::exception::File(outfname, "writing data");
			}
			if (len < outbuf.size())
				break;
		}

		if (flush && i < mdc.size() - 1)
		{
			// Write last block size to the index
			off_t cur = lseek(outfd, 0, SEEK_CUR);
			uint32_t last_size = htonl(cur - last_ofs);
			::write(outidx, &last_size, sizeof(last_size));
			last_ofs = cur;

			compressor.restart();
		}
	}

	close(outfd);
	close(outidx);
}

int main(int argc, const char* argv[])
{
	wibble::commandline::Options opts;
	try {
		if (opts.parse(argc, argv))
			return 0;

		runtime::init();

		while (opts.hasNext())
		{
			do_mdfile(opts.next());
		}

		return 0;
	} catch (wibble::exception::BadOption& e) {
		cerr << e.desc() << endl;
		opts.outputHelp(cerr);
		return 1;
	} catch (std::exception& e) {
		cerr << e.what() << endl;
		return 1;
	}
}

// vim:set ts=4 sw=4:
