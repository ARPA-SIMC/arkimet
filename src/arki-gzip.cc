#include "config.h"
#include <arki/utils/commandline/parser.h>
#include <arki/scan/any.h>
#include <arki/utils/sys.h>
#include <arki/runtime.h>
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

namespace arki {
namespace utils {
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
}

int main(int argc, const char* argv[])
{
    commandline::Options opts;
    try {
        if (opts.parse(argc, argv))
            return 0;

        runtime::init();

        while (opts.hasNext())
        {
            string fname = opts.next();
            scan::compress(fname, std::make_shared<core::lock::Null>());
            if (!opts.keep->boolValue())
                sys::unlink_ifexists(fname);
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
