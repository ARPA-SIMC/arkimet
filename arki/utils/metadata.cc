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
#include <arki/summary.h>
#include <arki/sort.h>
#include <arki/postprocess.h>
#include <fstream>
#include <memory>

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
