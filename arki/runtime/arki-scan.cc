#include "arki/runtime/arki-scan.h"
#include "arki/runtime.h"
#include "arki/runtime/processor.h"
#include "arki/runtime/source.h"
#include "arki/runtime/dispatch.h"
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
        add_scan_options();
    }
};

}

int arki_scan(int argc, const char* argv[])
{
    Options opts;
    try {
        if (opts.parse(argc, argv))
            return 0;

        runtime::init();

        opts.setupProcessing();
        MetadataDispatch::process_quick_actions(opts);

        bool all_successful;
        auto processor = processor::create(opts, opts.query, *opts.output);
        if (opts.dispatch->isSet() || opts.testdispatch->isSet())
        {
            MetadataDispatch dispatcher(opts, *processor);
            all_successful = opts.foreach_source([&](runtime::Source& source) {
                source.dispatch(dispatcher);
                return true;
            });
        } else {
            all_successful = opts.foreach_source([&](runtime::Source& source) {
                source.process(*processor);
                return true;
            });
        }

        processor->end();

        if (all_successful)
            return 0;
        else
            return 2;
    } catch (runtime::HandledByCommandLineParser& e) {
        return e.status;
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
