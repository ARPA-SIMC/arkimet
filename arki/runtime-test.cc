#include "config.h"
#include "arki/tests/tests.h"
#include "arki/utils/sys.h"
#include "arki/runtime.h"
#include "arki/runtime/processor.h"
#include "arki/dataset/file.h"

namespace {
using namespace std;
using namespace arki;
using namespace arki::utils;
using namespace arki::tests;

def_tests(arki_runtime);

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

add_method("copyok", [] {
    for (auto path: { "test200", "test80", "error", "duplicates", "copyok" })
        if (sys::exists(path))
            sys::rmtree(path);;
    ConfigFile cfg(R"(
[test200]
type = ondisk2
step = daily
filter = origin: GRIB1,200
index = origin, reftime
name = test200
path = test200

[test80]
type = ondisk2
step = daily
filter = origin: GRIB1,80
index = origin, reftime
name = test80
path = test80

[error]
type = error
step = daily
name = error
path = error

[duplicates]
type = duplicates
step = daily
name = duplicates
path = duplicates
    )");
    CollectProcessor output;
    runtime::MetadataDispatch dispatch(cfg, output);
    dispatch.dir_copyok = "copyok/copyok";
    dispatch.dir_copyko = "copyok/copyko";
    sys::makedirs(dispatch.dir_copyok);
    sys::makedirs(dispatch.dir_copyko);

    ConfigFile in_cfg;
    wassert(actual_file("inbound/test.grib1").exists());
    dataset::File::readConfig("inbound/test.grib1", in_cfg);
    auto in_config = dataset::FileConfig::create(*in_cfg.sectionBegin()->second);
    auto reader = in_config->create_reader();

    wassert(actual(dispatch.process(*reader, "test.grib1")).isfalse());

    /*
    for (const auto& md: output.mdc)
        for (const auto& n: md->notes())
            fprintf(stderr, "%s\n", n.content.c_str());
    */

    wassert(actual_file("copyok/copyok/test.grib1").exists());
    wassert(actual(sys::size("copyok/copyok/test.grib1")) == 42178u);
    wassert(actual_file("copyok/copyko/test.grib1").exists());
    wassert(actual(sys::size("copyok/copyko/test.grib1")) == 2234u);
});

}

}
