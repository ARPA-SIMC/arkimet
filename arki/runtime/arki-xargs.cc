#include "arki/../config.h"
#include "arki/runtime/arki-xargs.h"
#include "arki/utils/commandline/parser.h"
#include "arki/utils/sys.h"
#include "arki/runtime.h"
#include "arki/runtime/io.h"
#include "arki/metadata.h"
#include "arki/metadata/xargs.h"
#include <iostream>

using namespace arki::utils;

namespace arki {
namespace runtime {

namespace {

using namespace arki::utils::commandline;

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
            "by SI or IEC multiplicative suffixes: b,c=1, "
            "K,Kb=1000, Ki=1024, M,Mb=1000*1000, Mi=1024*1024, "
            "G,Gb=1000*1000*1000, Gi=1024*1024*1024, and so on "
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

template<typename Consumer>
static void process(Consumer& consumer, sys::NamedFileDescriptor& in)
{
    metadata::ReadContext rc(sys::getcwd(), in.name());
    Metadata::read_file(in, rc, [&](std::unique_ptr<Metadata> md) { return consumer.eat(move(md)); });
}

int ArkiXargs::run(int argc, const char* argv[])
{
    Options opts;
    try {
        if (opts.parse(argc, argv))
            return 0;

        if (!opts.hasNext())
            throw commandline::BadOption("please specify a command to run");

        std::vector<std::string> args;
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
            core::Stdin in;
            process(consumer, in);
            consumer.flush();
        } else {
            // Process the files
            for (const auto& i: opts.inputfiles->values())
            {
                runtime::InputFile in(i.c_str());
                process(consumer, in);
            }
            consumer.flush();
        }

        return 0;
    } catch (commandline::BadOption& e) {
        std::cerr << e.what() << std::endl;
        opts.outputHelp(std::cerr);
        return 1;
    } catch (std::exception& e) {
        std::cerr << e.what() << std::endl;
        return 1;
    }
}

}
}
