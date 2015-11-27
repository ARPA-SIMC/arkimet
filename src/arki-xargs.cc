/// Runs a command on every blob of data found in the input files
#include "config.h"
#include <arki/metadata.h>
#include <arki/metadata/xargs.h>
#include <arki/types/reftime.h>
#include <arki/types/timerange.h>
#include <arki/scan/any.h>
#include <arki/runtime.h>
#include <arki/utils/sys.h>
#include <arki/wibble/exception.h>
#include <arki/utils/commandline/parser.h>
#include <arki/wibble/sys/exec.h>
#include <iostream>
#include <cstdlib>
#include <cstring>
#include <unistd.h>

using namespace std;
using namespace arki;
using namespace arki::types;
using namespace arki::utils;

namespace arki {
namespace utils {
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
}

static void process(metadata::Eater& consumer, runtime::Input& in)
{
    metadata::ReadContext rc(sys::getcwd(), in.name());
    Metadata::readFile(in.stream(), rc, consumer);
}

int main(int argc, const char* argv[])
{
    commandline::Options opts;
    try {
        if (opts.parse(argc, argv))
            return 0;

        if (!opts.hasNext())
            throw commandline::BadOption("please specify a command to run");

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
    } catch (commandline::BadOption& e) {
        cerr << e.what() << endl;
        opts.outputHelp(cerr);
        return 1;
	} catch (std::exception& e) {
		cerr << e.what() << endl;
		return 1;
	}
}
