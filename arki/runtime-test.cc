#include "config.h"
#include <fstream>
#include <algorithm>
#include "arki/tests/tests.h"
#include "arki/metadata/collection.h"
#include "arki/utils/sys.h"
#include "arki/runtime.h"
#include "arki/runtime/processor.h"
#include "arki/runtime/dispatch.h"
#include "arki/dataset/file.h"

namespace {
using namespace std;
using namespace arki;
using namespace arki::utils;
using namespace arki::tests;

class Tests : public TestCase
{
    using TestCase::TestCase;
    void register_tests() override;
} test("arki_runtime");

struct CollectProcessor : public runtime::DatasetProcessor
{
    metadata::Collection mdc;

    void process(dataset::Reader& ds, const std::string& name) override {
        mdc.add(ds, Matcher());
    }
    std::string describe() const { return "[test]CollectProcessor"; }
};

void Tests::register_tests() {

add_method("files", [] {
    // Reproduce https://github.com/ARPAE-SIMC/arkimet/issues/19

    utils::sys::write_file("import.lst", "grib:inbound/test.grib1\n");
    utils::sys::write_file("config", "[error]\ntype=discard\n");

    runtime::ScanCommandLine opts("arki-scan");

    const char* argv[] = { "arki-scan", "--dispatch=config", "--dump", "--status", "--summary", "--files=import.lst", nullptr };
    int argc = sizeof(argv) / sizeof(argv[0]) - 1;
    wassert(actual(opts.parse(argc, argv)).isfalse());

    runtime::init();

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
