/*
 * arki-inbound - Manage a remote inbound queue
 *
 * Copyright (C) 2010  ARPA-SIM <urpsim@smr.arpa.emr.it>
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

#include <wibble/exception.h>
#include <wibble/string.h>

#include <arki/metadata.h>
#include <arki/matcher.h>
#include <arki/dataset.h>
#include <arki/dataset/http.h>
#include <arki/runtime.h>

#include "config.h"

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

struct Printer : public metadata::Consumer
{
    virtual void flush() {}

    static auto_ptr<Printer> create(wibble::commandline::Options& opts);
};

struct BinaryPrinter : public Printer
{
    virtual bool operator()(Metadata& md)
    {
        md.write(cout, "(stdout)");
        return true;
    }
};

auto_ptr<Printer> Printer::create(wibble::commandline::Options& opts)
{
    return auto_ptr<Printer>(new BinaryPrinter);
}

int main(int argc, const char* argv[])
{
    wibble::commandline::Options opts;

    try {
        if (opts.parse(argc, argv))
            return 0;

        runtime::init();

        // Fetch the remote URL
        string url;
        if (opts.url->isSet())
            url = opts.url->stringValue();
        else if (const char* envurl = getenv("ARKI_INBOUND"))
            url = envurl;
        else
            throw wibble::exception::BadOption("please specify --url or set ARKI_INBOUND in the environment");

        dataset::HTTPInbound inbound(url);

        if (opts.do_list->isSet())
        {
            vector<string> files;
            inbound.list(files);
            for (vector<string>::const_iterator i = files.begin();
                    i != files.end(); ++i)
                cout << *i << endl;
        } else if (opts.do_scan->isSet()) {
            auto_ptr<Printer> printer = Printer::create(opts);

            while (opts.hasNext())
            {
                string fname = opts.next();
                size_t pos = fname.find(":");
                if (pos != string::npos)
                    inbound.scan(fname.substr(pos+1), fname.substr(0, pos), *printer);
                else
                    inbound.scan(fname, "", *printer);
            }
        } else {
            /*
            opts.setupProcessing();

            bool all_successful = true;
            for (ConfigFile::const_section_iterator i = opts.inputInfo.sectionBegin();
                    i != opts.inputInfo.sectionEnd(); ++i)
            {
                auto_ptr<ReadonlyDataset> ds = opts.openSource(*i->second);

                bool success = true;
                try {
                    success = opts.processSource(*ds, i->second->value("path"));
                } catch (std::exception& e) {
                    // FIXME: this is a quick experiment: a better message can
                    // print some of the stats to document partial imports
                    cerr << i->second->value("path") << " failed: " << e.what() << endl;
                    success = false;
                }

                opts.closeSource(ds, success);

                // Take note if something went wrong
                if (!success) all_successful = false;
            }

            opts.doneProcessing();

            if (all_successful)
                return 0;
            else
                return 2;
            */
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
