/*
 * utils/metadata - Useful functions for working with metadata
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

#include <arki/utils/metadata.h>
#include <arki/utils/dataset.h>
#include <arki/utils/compress.h>
#include <arki/utils/codec.h>
#include <arki/utils.h>
#include <arki/summary.h>
#include <arki/sort.h>
#include <arki/postprocess.h>
#include <fstream>
#include <memory>
#include <sys/types.h>
#include <sys/stat.h>
#include <fcntl.h>
#include <arpa/inet.h>
#include <sys/types.h>
#include <utime.h>

#include "config.h"

#ifdef HAVE_LUA
#include <arki/report.h>
#endif

using namespace std;
using namespace wibble;

namespace arki {
namespace utils {
namespace metadata {

static void compressAndWrite(const std::string& buf, std::ostream& out, const std::string& fname)
{
	wibble::sys::Buffer obuf = compress::lzo(buf.data(), buf.size());
	if (obuf.size() < buf.size() + 8)
	{
		// Write a metadata group
		string tmp;
		codec::Encoder enc(tmp);
		enc.addString("MG");
		enc.addUInt(0, 2);	// Version 0: LZO compressed
		enc.addUInt(obuf.size() + 4, 4); // Compressed len
		enc.addUInt(buf.size(), 4); // Uncompressed len
		out.write(tmp.data(), tmp.size());
		out.write((const char*)obuf.data(), obuf.size());
	} else
		// Write the plain metadata
		out.write(buf.data(), buf.size());
}

void Collector::writeTo(std::ostream& out, const std::string& fname) const
{
	static const size_t blocksize = 256;

	string buf;
	for (size_t i = 0; i < size(); ++i)
	{
		if (i > 0 && (i % blocksize) == 0)
		{
			compressAndWrite(buf, out, fname);
			buf.clear();
		}
		buf += (*this)[i].encode();
	}
	if (!buf.empty())
		compressAndWrite(buf, out, fname);
}

void Collector::writeAtomically(const std::string& fname) const
{
	AtomicWriter writer(fname);
	writeTo(writer.out(), writer.tmpfname);
	writer.commit();
}

void Collector::appendTo(const std::string& fname) const
{
	std::ofstream outmd;
	outmd.open(fname.c_str(), ios::out | ios::app);
	if (!outmd.is_open() || outmd.fail())
		throw wibble::exception::File(fname, "opening file for appending");
	writeTo(outmd, fname);
	outmd.close();
}

void Collector::queryData(const dataset::DataQuery& q, MetadataConsumer& consumer)
{
	// First ask the index.  If it can do something useful, iterate with it
	//
	// If the index would just do a linear scan of everything, then instead
	// scan the directories in sorted order.
	//
	// For each directory try to match its summary first, and if it matches
	// then produce all the contents.
	MetadataConsumer* c = &consumer;
	auto_ptr<sort::Stream> sorter;
	auto_ptr<ds::TemporaryDataInliner> inliner;

	if (q.withData)
	{
		inliner.reset(new ds::TemporaryDataInliner(*c));
		c = inliner.get();
	}

	if (q.sorter)
	{
		sorter.reset(new sort::Stream(*q.sorter, *c));
		c = sorter.get();
	}

	for (std::vector<Metadata>::iterator i = begin();
			i != end(); ++i)
		if (q.matcher(*i))
			if (!(*c)(*i))
				break;
}

void Collector::querySummary(const Matcher& matcher, Summary& summary)
{
	using namespace wibble::str;

	for (std::vector<Metadata>::iterator i = begin();
			i != end(); ++i)
		if (matcher(*i))
			summary.add(*i);
}

std::string Collector::ensureContiguousData(const std::string& source) const
{
	// Check that the metadata completely cover the data file
	if (empty()) return std::string();

	string last_file;
	off_t last_end = 0;
	for (const_iterator i = begin(); i != end(); ++i)
	{
		Item<types::source::Blob> s = i->source.upcast<types::source::Blob>();
		if (s->offset != (size_t)last_end)
			throw wibble::exception::Consistency("validating " + source,
					"metadata element points to data that does not start at the end of the previous element");
		if (i == begin())
		{
			last_file = s->filename;
		} else {
			if (last_file != s->filename)
				throw wibble::exception::Consistency("validating " + source,
						"metadata element points at another data file (previous: " + last_file + ", this: " + s->filename + ")");
		}
		last_end += s->size;
	}
	string fname = (*this)[0].completePathname(last_file);
	std::auto_ptr<struct stat> st = sys::fs::stat(fname);
	if (st.get() == NULL)
		throw wibble::exception::File(fname, "validating data described in " + source);
	if (st->st_size != last_end)
		throw wibble::exception::Consistency("validating " + source,
				"metadata do not cover the entire data file " + fname);
	return fname;
}

void Collector::compressDataFile(size_t groupsize, const std::string& source) const
{
	string datafile = ensureContiguousData(source);

	// Open output compressed file
	string outfname = datafile + ".gz";
	int outfd = open(outfname.c_str(), O_WRONLY | O_CREAT | O_EXCL, 0666);
	if (outfd < 0)
		throw wibble::exception::File(outfname, "creating file");
	utils::HandleWatch hwoutfd(outfname, outfd);

	// Open output index
	string idxfname = datafile + ".gz.idx";
	int outidx = open(idxfname.c_str(), O_WRONLY | O_CREAT | O_EXCL, 0666);
	if (outidx < 0)
		throw wibble::exception::File(idxfname, "creating file");
	utils::HandleWatch hwoutidx(idxfname, outidx);

	// Start compressor
	utils::compress::ZlibCompressor compressor;

	// Output buffer for the compressor
	sys::Buffer outbuf(4096*2);

	// Compress data
	off_t last_unc_ofs = 0;
	off_t last_ofs = 0;
	for (size_t i = 0; i < size(); ++i)
	{
		wibble::sys::Buffer buf = (*this)[i].getData();
		bool flush = (i == size() - 1) || (i > 0 && (i % groupsize) == 0);

		compressor.feedData(buf.data(), buf.size());
		while (true)
		{
			size_t len = compressor.get(outbuf, flush);
			if (len > 0)
			{
				if (write(outfd, outbuf.data(), (size_t)len) != (ssize_t)len)
					throw wibble::exception::File(outfname, "writing data");
			}
			if (len < outbuf.size())
				break;
		}

		if (flush && i < size() - 1)
		{
			// Write last block size to the index
			Item<types::source::Blob> src = (*this)[i].source.upcast<types::source::Blob>();
			size_t unc_ofs = src->offset + src->size;

			off_t cur = lseek(outfd, 0, SEEK_CUR);
			uint32_t ofs = htonl(unc_ofs - last_unc_ofs);
			uint32_t last_size = htonl(cur - last_ofs);
			::write(outidx, &ofs, sizeof(ofs));
			::write(outidx, &last_size, sizeof(last_size));
			last_ofs = cur;
			last_unc_ofs = unc_ofs;

			compressor.restart();
		}
	}

	// Set the same timestamp as the uncompressed file
	std::auto_ptr<struct stat> st = sys::fs::stat(datafile);
	struct utimbuf times;
	times.actime = st->st_atime;
	times.modtime = st->st_mtime;
	utime(outfname.c_str(), &times);

	hwoutfd.close();
	hwoutidx.close();

	// TODO: delete uncompressed version
}

AtomicWriter::AtomicWriter(const std::string& fname)
	: fname(fname), tmpfname(fname + ".tmp"), outmd(0)
{
	outmd = new std::ofstream;
	outmd->open(tmpfname.c_str(), ios::out | ios::trunc);
	if (!outmd->is_open() || outmd->fail())
		throw wibble::exception::File(tmpfname, "opening file for writing");
}

AtomicWriter::~AtomicWriter()
{
	rollback();
}

/*
bool AtomicWriter::operator()(Metadata& md)
{
	md.write(*outmd, tmpfname);
	return true;
}

bool AtomicWriter::operator()(const Metadata& md)
{
	md.write(*outmd, tmpfname);
	return true;
}
*/

void AtomicWriter::commit()
{
	if (outmd)
	{
		outmd->close();
		delete outmd;
		outmd = 0;
		if (rename(tmpfname.c_str(), fname.c_str()) < 0)
			throw wibble::exception::System("Renaming " + tmpfname + " to " + fname);
	}
}

void AtomicWriter::rollback()
{
	if (outmd)
	{
		outmd->close();
		delete outmd;
		outmd = 0;
		::unlink(tmpfname.c_str());
	}
}

bool Summarise::operator()(Metadata& md)
{
	s.add(md);
	return true;
}

}
}
}
// vim:set ts=4 sw=4:
