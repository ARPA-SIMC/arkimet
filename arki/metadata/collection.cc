/*
 * metadata/collection - In-memory collection of metadata
 *
 * Copyright (C) 2007--2015  ARPA-SIM <urpsim@smr.arpa.emr.it>
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

#include "collection.h"
#include <arki/types/source/blob.h>
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
using namespace arki::types;

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

Collection::Collection() {}
Collection::~Collection()
{
    for (vector<Metadata*>::iterator i = vals.begin(); i != vals.end(); ++i)
        delete *i;
}

void Collection::clear()
{
    for (vector<Metadata*>::iterator i = vals.begin(); i != vals.end(); ++i)
        delete *i;
    vals.clear();
}

void Collection::pop_back()
{
    if (empty()) return;
    delete vals.back();
    vals.pop_back();
}

bool Collection::observe(const Metadata& md)
{
    return eat(Metadata::create_copy(md));
}

bool Collection::eat(auto_ptr<Metadata> md)
{
    md->drop_cached_data();
    vals.push_back(md.release());
    return true;
}

void Collection::writeTo(std::ostream& out, const std::string& fname) const
{
	static const size_t blocksize = 256;

	string buf;
	for (size_t i = 0; i < vals.size(); ++i)
	{
		if (i > 0 && (i % blocksize) == 0)
		{
			compressAndWrite(buf, out, fname);
			buf.clear();
		}
		buf += vals[i]->encodeBinary();
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

std::string Collection::ensureContiguousData(const std::string& source) const
{
	// Check that the metadata completely cover the data file
	if (empty()) return std::string();

	string fname;
	off_t last_end = 0;
    for (vector<Metadata*>::const_iterator i = vals.begin(); i != vals.end(); ++i)
    {
        const source::Blob& s = (*i)->sourceBlob();
        if (s.offset != (size_t)last_end)
            throw wibble::exception::Consistency("validating " + source,
                    "metadata element points to data that does not start at the end of the previous element");
        if (i == vals.begin())
        {
            fname = s.absolutePathname();
        } else {
            if (fname != s.absolutePathname())
                throw wibble::exception::Consistency("validating " + source,
                        "metadata element points at another data file (previous: " + fname + ", this: " + s.absolutePathname() + ")");
        }
        last_end += s.size;
    }
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
    for (const_iterator i = vals.begin(); i != vals.end(); ++i)
        compressor.add((*i)->getData());
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

void Collection::add_to_summary(Summary& out) const
{
    for (const_iterator i = vals.begin(); i != vals.end(); ++i)
        out.add(**i);
}

bool Collection::send_to_observer(Observer& out) const
{
    for (const_iterator i = vals.begin(); i != vals.end(); ++i)
        if (!out.observe(**i))
            return false;
    return true;
}

bool Collection::copy_to_eater(Eater& out) const
{
    for (const_iterator i = vals.begin(); i != vals.end(); ++i)
        if (!out.eat(Metadata::create_copy(**i)))
            return false;
    return true;
}

namespace {
struct ClearOnEnd
{
    vector<Metadata*>& vals;
    ClearOnEnd(vector<Metadata*>& vals) : vals(vals) {}
    ~ClearOnEnd()
    {
        for (vector<Metadata*>::iterator i = vals.begin(); i != vals.end(); ++i)
            delete *i;
        vals.clear();
    }
};
}

bool Collection::move_to_eater(Eater& out)
{
    // Ensure that at the end of this method we clear vals, deallocating all
    // leftovers
    ClearOnEnd coe(vals);

    for (vector<Metadata*>::iterator i = vals.begin(); i != vals.end(); ++i)
    {
        // Move the pointer to an auto_ptr
        auto_ptr<Metadata> md(*i);
        *i = 0;
        // Pass it on to the eater
        if (!out.eat(md))
            return false;
    }
    return true;
}

void Collection::strip_source_paths()
{
    for (vector<Metadata*>::iterator i = vals.begin(); i != vals.end(); ++i)
    {
        const source::Blob& source = (*i)->sourceBlob();
        (*i)->set_source(upcast<Source>(source.fileOnly()));
    }
}

void Collection::sort(const sort::Compare& cmp)
{
    std::sort(vals.begin(), vals.end(), sort::STLCompare(cmp));
}

void Collection::sort(const std::string& cmp)
{
	refcounted::Pointer<sort::Compare> c = sort::Compare::parse(cmp);
	sort(*c);
}

void Collection::sort()
{
	sort("");
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
