#include "config.h"

#include <arki/wibble/exception.h>
#include <arki/wibble/string.h>

#include <arki/metadata.h>
#include <arki/matcher.h>
#include <arki/dataset.h>
#include <arki/dataset/file.h>
#include <arki/runtime.h>

#include <memory>
#include <iostream>
#include <cstdio>

using namespace std;
using namespace arki;
using namespace wibble;

namespace wibble {
namespace commandline {

struct Options : public runtime::CommandLine
{
	Options() : runtime::CommandLine("arki-scan")
	{
		usage = "[options] [input...]";
		description =
			"Read one or more files or datasets and process their data "
			"or import them in a dataset.";
		addScanOptions();
	}
};

}
}

int main(int argc, const char* argv[])
{
	wibble::commandline::Options opts;
	
	try {
		if (opts.parse(argc, argv))
			return 0;

		runtime::init();

		opts.setupProcessing();

		bool all_successful = true;
		for (ConfigFile::const_section_iterator i = opts.inputInfo.sectionBegin();
				i != opts.inputInfo.sectionEnd(); ++i)
		{
			unique_ptr<ReadonlyDataset> ds = opts.openSource(*i->second);

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
	} catch (wibble::exception::BadOption& e) {
		cerr << e.desc() << endl;
		opts.outputHelp(cerr);
		return 1;
	} catch (std::exception& e) {
		cerr << e.what() << endl;
		return 1;
	}
}
