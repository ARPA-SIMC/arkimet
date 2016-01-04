/// Manage a remote inbound queue
#include "config.h"
#include <arki/wibble/exception.h>
#include <arki/metadata.h>
#include <arki/metadata/consumer.h>
#include <arki/matcher.h>
#include <arki/dataset.h>
#include <arki/dataset/http.h>
#include <arki/runtime.h>
#include <memory>
#include <iostream>
#include <cstdlib>
#include <cstdio>

using namespace std;
using namespace arki;
using namespace arki::utils;

namespace arki {
namespace utils {
namespace commandline {

struct Options : public StandardParserWithManpage
{
    BoolOption* do_list;
    BoolOption* do_scan;
    BoolOption* do_test;
    BoolOption* do_import;
    StringOption* url;

    Options() : StandardParserWithManpage("arki-inbound", PACKAGE_VERSION, 1, PACKAGE_BUGREPORT)
    {
        usage = "[options] [filename...]";
        description = "Manage a remote inbound directory";

        do_list = add<BoolOption>("list", 0, "list", "", "list files in the inbound queue");
        do_scan = add<BoolOption>("scan", 0, "scan", "", "scan files from the inbound queue");
        do_test = add<BoolOption>("test", 0, "test", "", "test dispatching files from the inbound queue");
        do_import = add<BoolOption>("import", 0, "import", "", "import files from the inbound queue");
        url = add<StringOption>("url", 0, "url", "URL", "URL of the arki-server managing the queue (default: $ARKI_INBOUND)");
    }
};

}
}
}

struct Printer
{
    virtual void flush() {}
    virtual bool eat(unique_ptr<Metadata>&& md) = 0;
    static unique_ptr<Printer> create(commandline::Options& opts);
};

struct BinaryPrinter : public Printer
{
    bool eat(unique_ptr<Metadata>&& md) override
    {
        md->write(1, "(stdout)");
        return true;
    }
};

unique_ptr<Printer> Printer::create(commandline::Options& opts)
{
    return unique_ptr<Printer>(new BinaryPrinter);
}

int main(int argc, const char* argv[])
{
    commandline::Options opts;

    try {
        if (opts.parse(argc, argv))
            return 0;

        runtime::init();

        // Fetch the remote URL
        if (opts.url->isSet())
            runtime::Config::get().url_inbound = opts.url->stringValue();
        if (runtime::Config::get().url_inbound.empty())
            throw commandline::BadOption("please specify --url or set ARKI_INBOUND in the environment");

        dataset::HTTPInbound inbound(runtime::Config::get().url_inbound);

        if (opts.do_list->isSet())
        {
            vector<string> files;
            inbound.list(files);
            for (vector<string>::const_iterator i = files.begin();
                    i != files.end(); ++i)
                cout << *i << endl;
        } else if (opts.do_scan->isSet()) {
            unique_ptr<Printer> printer = Printer::create(opts);

            while (opts.hasNext())
            {
                string format;
                string fname = opts.next();
                size_t pos = fname.find(":");
                if (pos != string::npos)
                {
                    format = fname.substr(0, pos);
                    fname = fname.substr(pos+1);
                }
                inbound.scan(fname, format, [&](unique_ptr<Metadata> md) { return printer->eat(move(md)); });
            }
        } else if (opts.do_test->isSet()) {
            while (opts.hasNext())
            {
                string format;
                string fname = opts.next();
                size_t pos = fname.find(":");
                if (pos != string::npos)
                {
                    format = fname.substr(0, pos);
                    fname = fname.substr(pos+1);
                }
                inbound.testdispatch(fname, format, 1);
            }
        } else if (opts.do_import->isSet()) {
            unique_ptr<Printer> printer = Printer::create(opts);

            while (opts.hasNext())
            {
                string format;
                string fname = opts.next();
                size_t pos = fname.find(":");
                if (pos != string::npos)
                {
                    format = fname.substr(0, pos);
                    fname = fname.substr(pos+1);
                }
                inbound.dispatch(fname, format, [&](unique_ptr<Metadata> md) { return printer->eat(move(md)); });
            }
        } else {
            throw commandline::BadOption("please specify an action with --list, --scan, --test or --import");
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
