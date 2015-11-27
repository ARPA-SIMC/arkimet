#include "config.h"
#include <arki/wibble/exception.h>
#include <arki/wibble/commandline/parser.h>
#include <arki/scan/any.h>
#include <arki/utils/sys.h>
#include <arki/runtime.h>
#include <fstream>
#include <iostream>
#include <cstring>
#include <cerrno>
#include <sys/types.h>
#include <sys/stat.h>
#include <fcntl.h>
#include <arpa/inet.h>

using namespace std;
using namespace arki;
using namespace arki::utils;

namespace wibble {
namespace commandline {

struct Options : public StandardParserWithManpage
{
	BoolOption* keep;

	Options() : StandardParserWithManpage("arki-gzip", PACKAGE_VERSION, 1, PACKAGE_BUGREPORT)
	{
		usage = "[options] [inputfile [inputfile...]]";
		description = "Compress the given data files";

		keep = add<BoolOption>("keep", 'k', "keep", "", "do not delete uncompressed file");
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

        while (opts.hasNext())
        {
            string fname = opts.next();
            scan::compress(fname);
            if (!opts.keep->boolValue())
                sys::unlink_ifexists(fname);
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
