/*
 * metadata/collection - In-memory collection of metadata
 *
 * Copyright (C) 2007--2010  ARPA-SIM <urpsim@smr.arpa.emr.it>
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

#include <arki/metadata/collection.h>
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
#include <utime.h>

#include "config.h"

#ifdef HAVE_LUA
#include <arki/report.h>
#endif

using namespace std;
using namespace wibble;
using namespace arki::utils;

namespace arki {
namespace metadata {

/**
 * Write metadata to a file, atomically.
 *
 * The file will be created with a temporary name, and then renamed to its
 * final name.
 *
 * Note: the temporary file name will NOT be created securely.
 */
struct AtomicWriter
{
	std::string fname;
	std::string tmpfname;
	std::ofstream* outmd;

	AtomicWriter(const std::string& fname);
	~AtomicWriter();

	std::ofstream& out() { return *outmd; }

	void commit();
	void rollback();
};


static void compressAndWrite(const std::string& buf, std::ostream& out, const std::string& fname)
{
	wibble::sys::Buffer obuf = compress::lzo(buf.data(), buf.size());
	if (obuf.size() + 8 < buf.size())
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

void Collection::writeTo(std::ostream& out, const std::string& fname) const
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

void Collection::writeAtomically(const std::string& fname) const
{
	AtomicWriter writer(fname);
	writeTo(writer.out(), writer.tmpfname);
	writer.commit();
}

void Collection::appendTo(const std::string& fname) const
{
	std::ofstream outmd;
	outmd.open(fname.c_str(), ios::out | ios::app);
	if (!outmd.is_open() || outmd.fail())
		throw wibble::exception::File(fname, "opening file for appending");
	writeTo(outmd, fname);
	outmd.close();
}

void Collection::queryData(const dataset::DataQuery& q, metadata::Consumer& consumer)
{
	// First ask the index.  If it can do something useful, iterate with it
	//
	// If the index would just do a linear scan of everything, then instead
	// scan the directories in sorted order.
	//
	// For each directory try to match its summary first, and if it matches
	// then produce all the contents.
	metadata::Consumer* c = &consumer;
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

void Collection::querySummary(const Matcher& matcher, Summary& summary)
{
	using namespace wibble::str;

	for (std::vector<Metadata>::iterator i = begin();
			i != end(); ++i)
		if (matcher(*i))
			summary.add(*i);
}

std::string Collection::ensureContiguousData(const std::string& source) const
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

void Collection::compressDataFile(size_t groupsize, const std::string& source)
{
	string datafile = ensureContiguousData(source);

	utils::compress::DataCompressor compressor(datafile, groupsize);
	sendTo(compressor);
	compressor.flush();

	// Set the same timestamp as the uncompressed file
	std::auto_ptr<struct stat> st = sys::fs::stat(datafile);
	struct utimbuf times;
	times.actime = st->st_atime;
	times.modtime = st->st_mtime;
	utime((datafile + ".gz").c_str(), &times);
	utime((datafile + ".gz.idx").c_str(), &times);
	// TODO: delete uncompressed version
}

void Collection::sort(const sort::Compare& cmp)
{
	std::sort(begin(), end(), sort::STLCompare(cmp));
}

void Collection::sort(const std::string& cmp)
{
	std::auto_ptr<sort::Compare> c = sort::Compare::parse(cmp);
	sort(*c);
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

}
}
// vim:set ts=4 sw=4:
