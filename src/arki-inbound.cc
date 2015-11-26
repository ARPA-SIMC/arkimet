/*
 * arki-inbound - Manage a remote inbound queue
 *
 * Copyright (C) 2010--2011  ARPA-SIM <urpsim@smr.arpa.emr.it>
 *
 * This program is free software; you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation; either version 2 of the License, or
 * (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License along
 * with this program; if not, write to the Free Software Foundation, Inc.,
 * 51 Franklin Street, Fifth Floor, Boston, MA 02110-1301 USA.
 *
 * Author: Enrico Zini <enrico@enricozini.com>
 */

#include "config.h"

#include <arki/wibble/exception.h>
#include <arki/wibble/string.h>

#include <arki/metadata.h>
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
using namespace wibble;

namespace wibble {
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

struct Printer : public metadata::Eater
{
    virtual void flush() {}

    static unique_ptr<Printer> create(wibble::commandline::Options& opts);
};

struct BinaryPrinter : public Printer
{
    bool eat(unique_ptr<Metadata>&& md) override
    {
        md->write(cout, "(stdout)");
        return true;
    }
};

unique_ptr<Printer> Printer::create(wibble::commandline::Options& opts)
{
    return unique_ptr<Printer>(new BinaryPrinter);
}

int main(int argc, const char* argv[])
{
    wibble::commandline::Options opts;

    try {
        if (opts.parse(argc, argv))
            return 0;

        runtime::init();

        // Fetch the remote URL
        if (opts.url->isSet())
            runtime::Config::get().url_inbound = opts.url->stringValue();
        if (runtime::Config::get().url_inbound.empty())
            throw wibble::exception::BadOption("please specify --url or set ARKI_INBOUND in the environment");

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
                inbound.scan(fname, format, *printer);
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
                inbound.testdispatch(fname, format, cout);
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
                inbound.dispatch(fname, format, *printer);
            }
        } else {
            throw wibble::exception::BadOption("please specify an action with --list, --scan, --test or --import");
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

// vim:set ts=4 sw=4:
