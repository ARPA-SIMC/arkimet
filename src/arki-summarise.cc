/*
 * arki-summarise - Summarise xgribarch metadata
 *
 * Copyright (C) 2007  ARPA-SIM <urpsim@smr.arpa.emr.it>
 *
 * This program is free software; you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation; either version 2 of the License.
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
#include <arki/metadata.h>
#include <arki/summary.h>
#include <arki/runtime.h>

#include <fstream>
#include <iostream>
#include <cstdlib>

#include "config.h"

using namespace std;
using namespace arki;
using namespace wibble;

namespace wibble {
namespace commandline {

struct Options : public StandardParserWithManpage
{
	//StringOption* nowThreshold;
	VectorOption<String>* addTo;
	StringOption* outfile;

	Options() : StandardParserWithManpage("arki-summarise", PACKAGE_VERSION, 1, PACKAGE_BUGREPORT)
	{
		usage = "[options] [input1 [input2...]]";
		description =
			"Read metadata from the given input file (or stdin), and"
			" write a summary of it to the given output file (or stdout).";

		addTo = add< VectorOption<String> >("add-to", 'a', "add-to", "file",
			"add the summarised data to this other summary information "
			"(can be specified more than once, '-' means standard input)");

		outfile = add<StringOption>("output", 'o', "output", "file",
			"write the output to the given file instead of standard output");

#if 0
		nowThreshold = add<StringOption>("now", 0, "now", "time",
			"if the summary end time is less than 'time' ago, it is considered as 'now'."
			" Default is 7d; allowed suffixes are 's' (second), 'm' (minute), 'h' (hour), 'd' (day).  A value of '0' disables this feature.");
#endif
	}
};

}
}

int parseInterval(const std::string& s)
{
	int mul;
	if (s.empty())
		throw wibble::exception::BadOption("the empty string is not a valid time interval");
	if (s == "0")
		return 0;
	
	switch (s[s.size() - 1])
	{
		case 's': mul = 1; break;
		case 'm': mul = 60; break;
		case 'h': mul = 3600; break;
		case 'd': mul = 3600 * 24; break;
		default:
			throw wibble::exception::BadOption(string("invalid time suffix '")+s[s.size()-1]+"': valid ones are 's', 'm', 'h' or 'd'");
	}

	const char* beg = s.c_str();
	char* end = 0;
	unsigned int val = strtoul(beg, &end, 10);
	if ((size_t)(end-beg) != s.size() - 1)
		throw wibble::exception::BadOption("invalid time interval '"+s+"': it should be '0', or a positive number followed by 's', 'm', 'h' or 'd'");

	return val*mul;
}

int main(int argc, const char* argv[])
{
	wibble::commandline::Options opts;
	try {
		if (opts.parse(argc, argv))
			return 0;

		#if 0
		int nowThreshold = 3600*24*7;
		if (opts.nowThreshold->isSet())
			nowThreshold = parseInterval(opts.nowThreshold->stringValue());
		#endif

		// Open the output file
		runtime::Output out(*opts.outfile);

		Summary summary;
		Metadata md;

		// Preinit the summary with what is specified with --add-to
		wibble::sys::Buffer buf;
		string signature;
		unsigned version;
		const std::vector<std::string>& addTo = opts.addTo->values();
		for (std::vector<std::string>::const_iterator i = addTo.begin();
				i != addTo.end(); ++i)
		{
			runtime::Input ain(*i);	
			while (types::readBundle(ain.stream(), ain.name(), buf, signature, version))
			{
				if (signature == "MD" || signature == "!D")
				{
					md.read(buf, version, ain.name());
					if (md.source->style() == types::Source::INLINE)
						md.readInlineData(ain.stream(), ain.name());
					summary.add(md);
				}
				else if (signature == "SU")
				{
					summary.read(buf, version, ain.name());
				}
			}
		}

		while (opts.hasNext())
		{
			// Open the input file
			runtime::Input in(opts.next());

			// Read all messages and output a summary
			Metadata md;
			while (md.read(in.stream(), in.name()))
				summary.add(md);
		}

		#if 0
		if (nowThreshold)
			summary.setEndtimeToNow(nowThreshold);
		#endif

		summary.write(out.stream(), out.name());

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
