/// Prepare BUFR messages for importing into arkimet

#include "config.h"
#include <arki/runtime.h>
#include <arki/nag.h>
#include <arki/scan/bufr.h>
#include <dballe/file.h>
#include <wreport/bulletin.h>
#include <dballe/message.h>
#include <dballe/msg/codec.h>
#include <arpa/inet.h>
#include <cstring>
#include <iostream>

using namespace std;
using namespace arki;
using namespace arki::utils;
using namespace wreport;
using namespace dballe;

namespace arki {
namespace utils {
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
}

class Copier
{
protected:
    bool override_usn_active;
    int override_usn_value;

    void copy_base_msg(BufrBulletin& dst, const BufrBulletin& src)
    {
        // Copy message attributes
        dst.master_table_number = src.master_table_number;
        dst.data_category = src.data_category;
        dst.data_subcategory = src.data_subcategory;
        dst.data_subcategory_local = src.data_subcategory_local;
        dst.originating_centre = src.originating_centre;
        dst.originating_subcentre = src.originating_subcentre;
        if (override_usn_active)
            dst.update_sequence_number = override_usn_value;
        else
            dst.update_sequence_number = src.update_sequence_number;
        dst.rep_year = src.rep_year;
        dst.rep_month = src.rep_month;
        dst.rep_day = src.rep_day;
        dst.rep_hour = src.rep_hour;
        dst.rep_minute = src.rep_minute;
        dst.rep_second = src.rep_second;
        dst.edition_number = src.edition_number;
        dst.master_table_version_number = src.master_table_version_number;
        dst.master_table_version_number_local = src.master_table_version_number_local;
        // Do not compress, since it only makes sense for multisubset messages
        dst.compression = 0;

        // FIXME: the original optional section is lost

        // Copy data descriptor section
        dst.datadesc = src.datadesc;

        // Load encoding tables
        dst.load_tables();
    }

    void splitmsg(const BinaryMessage& rmsg, const BufrBulletin& msg, msg::Importer& importer, dballe::File& outfile)
    {
        // Create new message with the same info as the old one
        auto newmsg(BufrBulletin::create());
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
                Messages msgs = importer.from_bulletin(*newmsg);

                // Update reference time
                const Datetime& dt = msgs[0].get_datetime();
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
            string newrmsg = newmsg->encode();
            outfile.write(newrmsg);
        }
    }

public:
    Copier() : override_usn_active(false)
    {
    }

    void override_usn(int value)
    {
        override_usn_active = true;
        override_usn_value = value;
    }

    void process(const std::string& filename, dballe::File& outfile)
    {
        // Use .release() so the code is the same even with the new C++11's dballe
        unique_ptr<dballe::File> file(dballe::File::create(dballe::File::BUFR, filename.c_str(), "r").release());
        unique_ptr<msg::Importer> importer(msg::Importer::create(dballe::File::BUFR).release());

        while (BinaryMessage rmsg = file->read())
        {
            // Decode message
            unique_ptr<BufrBulletin> msg;
            bool decoded;
            try {
                msg = BufrBulletin::decode(rmsg.data, rmsg.pathname.c_str(), rmsg.offset);
                decoded = true;
            } catch (std::exception& e) {
                nag::warning("%s:%ld: BUFR #%d failed to decode: %s. Passing it through unmodified.",
                    rmsg.pathname.c_str(), rmsg.offset, rmsg.index, e.what());
                decoded = false;
            }

            if ((!decoded || msg->subsets.size() == 1u) && !override_usn_active)
                outfile.write(rmsg.data);
            else
                splitmsg(rmsg, *msg, *importer, outfile);
        }
    }
};

int main(int argc, const char* argv[])
{
    commandline::Options opts;
    try {
        if (opts.parse(argc, argv))
            return 0;

        nag::init(opts.verbose->isSet(), opts.debug->isSet());

        runtime::init();

        Copier copier;

        if (opts.force_usn->isSet())
            copier.override_usn(opts.force_usn->intValue());

        unique_ptr<dballe::File> outfile;
        if (opts.outfile->isSet())
            outfile.reset(dballe::File::create(dballe::File::BUFR, opts.outfile->stringValue().c_str(), "wb").release());
        else
            outfile.reset(dballe::File::create(dballe::File::BUFR, "(stdout)", "wb").release());

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
    } catch (commandline::BadOption& e) {
        cerr << e.what() << endl;
        opts.outputHelp(cerr);
        return 1;
    } catch (std::exception& e) {
        cerr << e.what() << endl;
        return 1;
    }
}
