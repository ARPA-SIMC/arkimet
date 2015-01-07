/*
 * arki-xargs - Runs a command on every blob of data found in the input files
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
 * You should have received a copy of the GNU General Public License
 * along with this program; if not, write to the Free Software
 * Foundation, Inc., 51 Franklin St, Fifth Floor, Boston, MA  02110-1301 USA
 *
 * Author: Enrico Zini <enrico@enricozini.com>
 */

#include "config.h"

#include <arki/metadata.h>
#include <arki/metadata/xargs.h>
#include <arki/types/reftime.h>
#include <arki/types/timerange.h>
#include <arki/scan/any.h>
#include <arki/runtime.h>

#include <wibble/exception.h>
#include <wibble/commandline/parser.h>
#include <wibble/string.h>
#include <wibble/sys/fs.h>
#include <wibble/sys/exec.h>

#include <cstdlib>
#include <cstring>
#include <unistd.h>

using namespace std;
using namespace arki;
using namespace arki::types;
using namespace wibble;

namespace wibble {
namespace commandline {

struct Options : public StandardParserWithManpage
{
	VectorOption<String>* inputfiles;
	IntOption* max_args;
	StringOption* max_bytes;
	StringOption* time_interval;
    BoolOption* split_timerange;

	Options() : StandardParserWithManpage("arki-xargs", PACKAGE_VERSION, 1, PACKAGE_BUGREPORT)
	{
		usage = "[options] command [initial-arguments]";
		description =
			"For every item of data read from standard input, save "
			"it on a temporary file and run "
			"'command [initial-arguments] filename' on it";
		inputfiles = add< VectorOption<String> >("input", 'i', "input", "file",
			"read data from this file instead of standard input (can be given more than once)");

		max_args = add<IntOption>("max-args", 'n', "max-args", "count",
			"group at most this amount of data items per command invocation");
		max_bytes = add<StringOption>("max-size", 's', "max-size", "size",
			"create data files no bigger than this size. This may "
			"NOT be respected if there is one single data item "
			"greater than the size specified.  size may be followed "
			"by the following multiplicative suffixes: c=1, w=2, "
			"b=512, kB=1000, K=1024, MB=1000*1000, M=1024*1024, "
			"xM=M GB=1000*1000*1000, G=1024*1024*1024, and so on "
			"for T, P, E, Z, Y");
		time_interval = add<StringOption>("time-interval", 0, "time-interval", "interval",
			"create one data file per 'interval', where interval "
			"can be minute, hour, day, month or year");
        split_timerange = add<BoolOption>("split-timerange", 0, "split-timerange", "",
            "start a new data file when the time range changes");
		// We need to pass switches in [initial-arguments] untouched
		no_switches_after_first_arg = true;
	}
};

}
}

static void process(metadata::Consumer& consumer, runtime::Input& in)
{
	wibble::sys::Buffer buf;
	string signature;
	unsigned version;

	while (types::readBundle(in.stream(), in.name(), buf, signature, version))
	{
		if (signature != "MD") continue;

		// Read the metadata
		Metadata md;
		md.read(buf, version, in.name());
        if (md.source().style() == Source::INLINE)
            md.readInlineData(in.stream(), in.name());
		if (!consumer(md))
			break;
	}
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

        metadata::Xargs consumer;
        consumer.command = args;
        if (opts.max_args->isSet())
            consumer.max_count = opts.max_args->intValue();
        if (opts.max_bytes->isSet())
            consumer.set_max_bytes(opts.max_bytes->stringValue());
        if (opts.time_interval->isSet())
            consumer.set_interval(opts.time_interval->stringValue());
		if (opts.split_timerange->boolValue())
			consumer.split_timerange = true;

		if (opts.inputfiles->values().empty())
		{
			// Process stdin
			runtime::Input in("-");
			process(consumer, in);
			consumer.flush();
		} else {
			// Process the files
			for (vector<string>::const_iterator i = opts.inputfiles->values().begin();
					i != opts.inputfiles->values().end(); ++i)
			{
				runtime::Input in(i->c_str());
				process(consumer, in);
            }
            consumer.flush();
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
