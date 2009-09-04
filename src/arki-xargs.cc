/*
 * arki-xargs - Runs a command on every blob of data found in the input files
 *
 * Copyright (C) 2007--2009  ARPA-SIM <urpsim@smr.arpa.emr.it>
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
#include <arki/metadata.h>
#include <arki/scan/any.h>
#include <arki/runtime.h>

#include <sys/types.h>
#include <sys/stat.h>
#include <fcntl.h>

#include "config.h"

using namespace std;
using namespace arki;

namespace wibble {
namespace commandline {

struct Options : public StandardParserWithManpage
{
	wibble::commandline::VectorOption<wibble::commandline::String>* inputfiles;

	Options() : StandardParserWithManpage("arki-xargs", PACKAGE_VERSION, 1, PACKAGE_BUGREPORT)
	{
		usage = "[options] command [initial-arguments]";
		description =
			"For every item of data read from standard input, save "
			"it on a temporary file and run "
			"'command [initial-arguments] filename' on it";
		inputfiles = add< VectorOption<String> >("input", 'i', "input", "file",
			"read data from this file instead of standard input (can be given more than once)");
	}
};

}
}

static void process(const vector<string>& args, int infd, const std::string& inname)
{
}

int main(int argc, const char* argv[])
{
	wibble::commandline::Options opts;
	try {
		if (opts.parse(argc, argv))
			return 0;

		if (!opts.hasNext())
			throw wibble::exception::BadOption("please specify a command to run");

		vector<string> args;
		while (opts.hasNext())
			args.push_back(opts.next());

		runtime::init();

		if (opts.inputfiles->values().empty())
		{
			// Process stdin
			process(args, 0, "(stdin)");
		} else {
			// Process the files
			for (vector<string>::const_iterator i = opts.inputfiles->values().begin();
					i != opts.inputfiles->values().end(); ++i)
			{
				int fd = open(i->c_str(), O_RDONLY);
				if (fd < 0)
					throw wibble::exception::File(*i, "opening file");
				process(args, fd, *i);
				close(fd);
			}
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
