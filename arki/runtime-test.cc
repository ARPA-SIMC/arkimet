#include "config.h"

#include <arki/tests/tests.h>
#include <arki/utils/sys.h>
#include <arki/runtime.h>

namespace {
using namespace std;
using namespace arki;
using namespace arki::tests;

def_tests(arki_runtime);

void Tests::register_tests() {

add_method("files", [] {
    // Reproduce https://github.com/ARPA-SIMC/arkimet/issues/19

    utils::sys::write_file("import.lst", "grib:inbound/test.grib1\n");
    utils::sys::write_file("config", "[error]\ntype=discard\n");

    runtime::CommandLine opts("arki-scan");
    opts.addScanOptions();

    const char* argv[] = { "arki-scan", "--dispatch=config", "--dump", "--status", "--summary", "--files=import.lst", nullptr };
    int argc = sizeof(argv) / sizeof(argv[0]) - 1;
    wassert(actual(opts.parse(argc, argv)).isfalse());

    runtime::init();

    opts.setupProcessing();

/*
    try {

        bool all_successful = true;
        for (ConfigFile::const_section_iterator i = opts.inputInfo.sectionBegin();
                i != opts.inputInfo.sectionEnd(); ++i)
        {
            unique_ptr<dataset::Reader> ds = opts.openSource(*i->second);

			bool success = true;
			try {
				success = opts.processSource(*ds, i->second->value("path"));
			} catch (std::exception& e) {
				// FIXME: this is a quick experiment: a better message can
				// print some of the stats to document partial imports
				cerr << i->second->value("path") << " failed: " << e.what() << endl;
				success = false;
			}

            opts.closeSource(move(ds), success);

			// Take note if something went wrong
			if (!success) all_successful = false;
		}

		opts.doneProcessing();

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
*/
});

}

}
