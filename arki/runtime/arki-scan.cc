#include "arki/runtime/arki-scan.h"
#include "arki/runtime.h"
#include "arki/runtime/processor.h"
#include "arki/runtime/source.h"
#include "arki/runtime/dispatch.h"
#include <arki/runtime/inputs.h>
#include <arki/runtime/io.h>
#include "arki/utils/commandline/parser.h"
#include <iostream>

using namespace std;
using namespace arki::utils;

namespace arki {
namespace runtime {

namespace {

struct Options : public runtime::ScanCommandLine
{
    Options() : runtime::ScanCommandLine("arki-scan")
    {
        usage = "[options] [input...]";
        description =
            "Read one or more files or datasets and process their data "
            "or import them in a dataset.";
    }
};

}

ArkiScan::~ArkiScan()
{
    delete processor;
    delete dispatcher;
}

std::function<bool(runtime::Source& source)> ArkiScan::make_dest_func(bool& dispatch_ok)
{
    if (dispatcher)
    {
        return [&](runtime::Source& source) {
            dispatch_ok = source.dispatch(*dispatcher) && dispatch_ok;
            return true;
        };
    } else {
        return [&](runtime::Source& source) {
            dispatch_ok = source.process(*processor) && dispatch_ok;
            return true;
        };
    }
}

bool ArkiScan::run_scan_stdin(const std::string& format)
{
    bool dispatch_ok = true;
    auto dest = make_dest_func(dispatch_ok);
    return foreach_stdin(format, dest) && dispatch_ok;
}

bool ArkiScan::run_scan_inputs(const std::string& moveok, const std::string& moveko, const std::string& movework)
{
    bool dispatch_ok = true;
    auto dest = make_dest_func(dispatch_ok);
    return foreach_sections(inputs, moveok, moveko, movework, dest) && dispatch_ok;
}

int ArkiScan::run(int argc, const char* argv[])
{
    Options opts;
    try {
        if (opts.parse(argc, argv))
            return 0;

        runtime::init();

        Inputs inputs(this->inputs, opts);
        auto output = make_output(*opts.outfile);

        bool all_successful;
        processor = processor::create(opts, *output).release();
        if (opts.dispatch_options->dispatch_requested())
            dispatcher = new MetadataDispatch(*opts.dispatch_options, *processor);

        if (opts.stdin_input->isSet()) {
            all_successful = run_scan_stdin(opts.stdin_input->stringValue());
        } else {
            all_successful = run_scan_inputs(opts.dispatch_options->moveok->stringValue(), opts.dispatch_options->moveko->stringValue(), opts.dispatch_options->movework->stringValue());
        }

        processor->end();

        if (all_successful)
            return 0;
        else
            return 2;
    } catch (commandline::BadOption& e) {
        cerr << e.what() << endl;
        opts.outputHelp(cerr);
        return 1;
    } catch (std::exception& e) {
        cerr << e.what() << endl;
        return 1;
    }
}

}
}
