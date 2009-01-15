/*
 * arki-sort - Sort metadata files
 *
 * Copyright (C) 2008  ARPA-SIM <urpsim@smr.arpa.emr.it>
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
#include <wibble/regexp.h>
#include <wibble/sys/mmap.h>
#include <arki/types.h>
#include <arki/metadata.h>
#include <arki/summary.h>
#include <arki/runtime.h>

#include <fstream>
#include <iostream>

#include <sys/types.h>
#include <sys/stat.h>
#include <fcntl.h>

#include "config.h"

using namespace std;
using namespace arki;
using namespace wibble;

namespace wibble {
namespace commandline {

struct Options : public runtime::OutputOptions
{
	BoolOption* verbose;
	StringOption* order;

	Options() : runtime::OutputOptions("arki-sort")
	{
		usage = "[options] [input]";
		description =
			"Read one or more metadata files, sort their contents"
			" and dump them to standard output";

		verbose = add<BoolOption>("verbose", 0, "verbose", "",
			"verbose output");
		order = add<StringOption>("order", 0, "order", "",
			"sort order.  It can be a list of one or more metadata"
			" names (as seen in --yaml field names), with a '-'"
			" prefix to indicate reverse order.  For example,"
			" 'origin, -timerange, reftime' orders by origin first,"
			" then by reverse timerange, then by reftime.  Default:"
			" 'reftime'");
	}
};

}
}

struct SortOrder
{
	types::Code code;
	bool reverse;

	SortOrder(types::Code code, bool reverse) : code(code), reverse(reverse) {}
};

struct Key
{
	vector< UItem<> > items;
	const unsigned char* buf;
	size_t size;

	Key(Metadata& md, const vector<SortOrder>& order, const unsigned char* buf, size_t size)
		: buf(buf), size(size)
	{
		for (vector<SortOrder>::const_iterator i = order.begin();
			i != order.end(); ++i)
			items.push_back(md.get(i->code));
	}
};

struct Sorter
{
	const vector<SortOrder>& order;
	Sorter(const vector<SortOrder>& order) : order(order) {}

	bool operator()(const Key& a, const Key& b)
	{
		for (size_t i = 0; i < order.size(); ++i)
		{
			int cmp = a.items[i].compare(b.items[i]);
			if (order[i].reverse) cmp = -cmp;
			if (cmp < 0) return true;
			if (cmp > 0) return false;
		}
		return false;
	}
};


sys::Buffer slurp(int fd, const std::string& fname)
{
	const size_t blocksize = 4096 * 8;
	size_t cur = 0;
	sys::Buffer buf(blocksize);
	while (true)
	{
		int res = read(fd, (unsigned char*)buf.data() + cur, blocksize);
		if (res == -1)
			throw wibble::exception::File(fname, "reading data");
		if (res == 0)
			break;
		cur += res;
		buf.resize(cur + blocksize);
	}
	// Shrink to actual size
	buf.resize(cur);
	return buf;
}

sys::Buffer slurp(const std::string& fname)
{
	int fd = open(fname.c_str(), O_RDONLY);
	if (fd == -1)
		throw wibble::exception::File(fname, "opening file for reading");
	sys::Buffer res;
	try {
		res = slurp(fd, fname);
	} catch (...) {
		close(fd);
		throw;
	}
	close(fd);
	return res;
}

int main(int argc, const char* argv[])
{
	wibble::commandline::Options opts;
	try {
		if (opts.parse(argc, argv))
			return 0;

		bool verbose = opts.verbose->isSet();

		string order = "reftime";
		if (opts.order->isSet())
			order = opts.order->stringValue();

		vector<SortOrder> orders;
		Splitter sp("[[:space:]]*([[:space:]]|,)[[:space:]]*", REG_EXTENDED);
		for (Splitter::const_iterator i = sp.begin(order);
			i != sp.end(); ++i)
			if ((*i)[0] == '-')
				orders.push_back(SortOrder(types::parseCodeName(i->substr(1)), true));
			else
				orders.push_back(SortOrder(types::parseCodeName(*i), false));

		// Build the list of input files
		vector<string> inputs;
		while (opts.hasNext())
			inputs.push_back(opts.next());
		if (inputs.empty())
			inputs.push_back("-");

		// Read the metadata
		vector<Key> keys;
		vector<sys::Buffer> buffers;
		vector<sys::MMap> mmaps;
		for (vector<string>::const_iterator i = inputs.begin();
			i != inputs.end(); ++i)
		{
			const unsigned char* buf;
			size_t len;

			if (*i == "-")
			{
				if (verbose)
					cerr << "trying to read the entire standard input into memory" << endl;
				buffers.push_back(slurp(0, "(stdin)"));
				buf = (const unsigned char*)buffers.back().data();
				len = buffers.back().size();
			} else {
				try {
					mmaps.push_back(sys::MMap(*i));
					buf = (const unsigned char*)mmaps.back().buf;
					len = mmaps.back().size;
				} catch (wibble::exception::System& e) {
					if (verbose)
						cerr << *i << ": mmap failed, trying to read the entire file into memory" << endl;
					buffers.push_back(slurp(*i));
					buf = (const unsigned char*)buffers.back().data();
					len = buffers.back().size();
				}
			}

			Metadata md;
			const unsigned char* obuf = buf;
			size_t olen = len;
			while (md.read(buf, len, *i))
			{
				keys.push_back(Key(md, orders, obuf, olen-len));
				obuf = buf;
				olen = len;
			}
		}
	
		sort(keys.begin(), keys.end(), Sorter(orders));

		if (opts.isBinaryMetadataOutput())
		{
			// If binary metadata output is requested, we can just
			// dump the encoded data
			ostream& out = opts.output().stream();
			for (vector<Key>::const_iterator i = keys.begin();
				i != keys.end(); ++i)
				out.write((const char*)i->buf, i->size);
		} else {
			// Otherwise, we need to reparse
			Metadata md;
			for (vector<Key>::const_iterator i = keys.begin();
				i != keys.end(); ++i)
			{
				const unsigned char* buf = i->buf;
				size_t len = i->size;
				if (!md.read(buf, len, "(memory)"))
					throw wibble::exception::Consistency("reparsing metadata", "metadata has not been found: this is a programming error, since it has been parsed before");
				opts.consumer()(md);
			}
			opts.consumer().flush();
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
