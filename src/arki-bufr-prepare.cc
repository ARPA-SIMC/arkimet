/*
 * arki-bufr-prepare - Prepare BUFR messages for importing into arkimet
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
 * You should have received a copy of the GNU General Public License
 * along with this program; if not, write to the Free Software
 * Foundation, Inc., 51 Franklin St, Fifth Floor, Boston, MA  02110-1301 USA
 *
 * Author: Enrico Zini <enrico@enricozini.com>
 */

#include "config.h"

#include <arki/runtime.h>
#include <arki/nag.h>
#include <arki/scan/bufr.h>

#include <dballe/core/rawmsg.h>
#include <dballe/core/file.h>
#include <wreport/bulletin.h>
#include <dballe/msg/msgs.h>
#include <dballe/msg/codec.h>

#include <arpa/inet.h>
#include <cstring>

using namespace std;
using namespace arki;
using namespace wibble;
using namespace wreport;
using namespace dballe;

namespace wibble {
namespace commandline {

struct Options : public StandardParserWithManpage
{
    BoolOption* verbose;
    BoolOption* debug;
    StringOption* outfile;
    IntOption* force_usn;

    Options() : StandardParserWithManpage("arki-bufr-prepare", PACKAGE_VERSION, 1, PACKAGE_BUGREPORT)
    {
        usage = "[options] file1 file2...";
        description =
            "Read BUFR messages, encode each subsection in a "
            "separate message and add an optional section with "
            "information useful for arki-prepare";

        debug = add<BoolOption>("debug", 0, "debug", "", "debug output");
        verbose = add<BoolOption>("verbose", 0, "verbose", "", "verbose output");
        outfile = add<StringOption>("output", 'o', "output", "file",
                "write the output to the given file instead of standard output");
        force_usn = add<IntOption>("usn", 0, "usn", "number",
                "overwrite the update sequence number of every BUFR message with this value");
    }
};

}
}

class Copier
{
protected:
    std::map<std::string, int> to_rep_cod;
    bool override_usn_active;
    int override_usn_value;

    void copy_base_msg(BufrBulletin& dst, const BufrBulletin& src)
    {
        // Copy message attributes
        dst.type = src.type;
        dst.subtype = src.subtype;
        dst.localsubtype = src.localsubtype;
        dst.edition = src.edition;
        dst.master_table_number = src.master_table_number;
        dst.rep_year = src.rep_year;
        dst.rep_month = src.rep_month;
        dst.rep_day = src.rep_day;
        dst.rep_hour = src.rep_hour;
        dst.rep_minute = src.rep_minute;
        dst.rep_second = src.rep_second;
        dst.centre = src.centre;
        dst.subcentre = src.subcentre;
        dst.master_table = src.master_table;
        dst.local_table = src.local_table;
        // Do not compress, since it only makes sense for multisubset messages
        dst.compression = 0;
        if (override_usn_active)
            dst.update_sequence_number = override_usn_value;
        else
            dst.update_sequence_number = src.update_sequence_number;

        // FIXME: the original optional section is lost

        // Copy data descriptor section
        dst.datadesc = src.datadesc;

        // Load encoding tables
        dst.load_tables();
    }

    int extract_rep_cod(const Msg& msg)
    {
        const char* rep_memo = NULL;
        if (const Var* var = msg.get_rep_memo_var())
            rep_memo = var->value();
        else
            rep_memo = Msg::repmemo_from_type(msg.type);

        // Convert rep_memo to rep_cod
        std::map<std::string, int>::const_iterator rc = to_rep_cod.find(rep_memo);
        if (rc == to_rep_cod.end())
            return 0;
        else
            return rc->second;
    }

    void splitmsg(const Rawmsg& rmsg, const BufrBulletin& msg, msg::Importer& importer, File& outfile)
    {
        // Create new message with the same info as the old one
        auto_ptr<BufrBulletin> newmsg(BufrBulletin::create());
        copy_base_msg(*newmsg, msg);

        // Loop over subsets
        for (size_t i = 0; i < msg.subsets.size(); ++i)
        {
            // Create a bufrex_msg with the subset contents

            // Remove existing subsets
            newmsg->subsets.clear();

            // Copy subset
            newmsg->subsets.push_back(msg.subsets[i]);

            // Parse into dba_msg
            try {
                Msgs msgs;
                importer.from_bulletin(*newmsg, msgs);
                const Msg& m = *msgs[0];

                // Update reference time
                const Datetime& dt = m.datetime();
                if (!dt.is_missing())
                {
                    newmsg->rep_year = dt.year;
                    newmsg->rep_month = dt.month;
                    newmsg->rep_day = dt.day;
                    newmsg->rep_hour = dt.hour;
                    newmsg->rep_minute = dt.minute;
                    newmsg->rep_second = dt.second;
                }
            } catch (wreport::error& e) {
                // Don't bother with updating reference time if
                // we cannot understand the layout of this BUFR
            }

            // Write out the message
            Rawmsg newrmsg;
            newmsg->encode(newrmsg);
            outfile.write(newrmsg);
        }
    }

public:
    Copier() : override_usn_active(false)
    {
        to_rep_cod = scan::Bufr::read_map_to_rep_cod();
    }

    void override_usn(int value)
    {
        override_usn_active = true;
        override_usn_value = value;
    }

    void process(const std::string& filename, File& outfile)
    {
        // Use .release() so the code is the same even with the new C++11's dballe
        auto_ptr<File> file(File::create(BUFR, filename.c_str(), "r").release());
        auto_ptr<msg::Importer> importer(msg::Importer::create(BUFR).release());

        Rawmsg rmsg;
        while (file->read(rmsg))
        {
            // Decode message
            auto_ptr<BufrBulletin> msg(BufrBulletin::create());
            bool decoded;
            try {
                msg->decode(rmsg, rmsg.file.c_str(), rmsg.offset);
                decoded = true;
            } catch (std::exception& e) {
                nag::warning("%s:%ld: BUFR #%d failed to decode: %s. Passing it through unmodified.",
                    rmsg.file.c_str(), rmsg.offset, rmsg.index, e.what());
                decoded = false;
            }

            if ((!decoded || msg->subsets.size() == 1u) && !override_usn_active)
                outfile.write(rmsg);
            else
                splitmsg(rmsg, *msg, *importer, outfile);
        }
    }
};

int main(int argc, const char* argv[])
{
    wibble::commandline::Options opts;
    try {
        if (opts.parse(argc, argv))
            return 0;

        nag::init(opts.verbose->isSet(), opts.debug->isSet());

        runtime::init();

        Copier copier;

        if (opts.force_usn->isSet())
            copier.override_usn(opts.force_usn->intValue());

        auto_ptr<File> outfile;
        if (opts.outfile->isSet())
            outfile.reset(File::create(BUFR, opts.outfile->stringValue().c_str(), "wb").release());
        else
            outfile.reset(File::create(BUFR, "(stdout)", "wb").release());

        if (!opts.hasNext())
        {
            copier.process("(stdin)", *outfile);
        } else {
            while (opts.hasNext())
            {
                copier.process(opts.next().c_str(), *outfile);
            }
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
