#include "arki/../config.h"
#include "arki/runtime/arki-bufr-prepare.h"
#include "arki/utils/commandline/parser.h"
#include "arki/nag.h"
#include "arki/runtime.h"
#include <dballe/file.h>
#include <wreport/bulletin.h>
#include <dballe/importer.h>
#include <dballe/message.h>
#include <iostream>

using namespace wreport;
using namespace dballe;

namespace arki {
namespace runtime {

namespace {

using namespace arki::utils::commandline;

struct Options : public StandardParserWithManpage
{
    BoolOption* verbose;
    BoolOption* debug;
    StringOption* outfile;
    StringOption* failfile;
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
        failfile = add<StringOption>("fail", 0, "fail", "file",
                "do not ignore BUFR data that could not be decoded, but write it to the given file");
        force_usn = add<IntOption>("usn", 0, "usn", "number",
                "overwrite the update sequence number of every BUFR message with this value");
    }
};

struct Output
{
    std::unique_ptr<dballe::File> output_ok;
    std::unique_ptr<dballe::File> output_fail;

    void open_ok_stdout()
    {
        output_ok = dballe::File::create(dballe::Encoding::BUFR, stdout, false, "[standard output]");
    }

    void open_ok_file(const std::string& pathname)
    {
        output_ok = dballe::File::create(dballe::Encoding::BUFR, pathname, "wb");
    }

    void open_fail_file(const std::string& pathname)
    {
        output_fail = dballe::File::create(dballe::Encoding::BUFR, pathname, "wb");
    }

    void ok(const std::string& buf)
    {
        if (!output_ok.get()) return;
        output_ok->write(buf);
    }

    void fail(const std::string& buf)
    {
        if (!output_fail.get()) return;
        output_fail->write(buf);
    }
};

struct Copier
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

    void splitmsg(const BinaryMessage& rmsg, const BufrBulletin& msg, Importer& importer, Output& output)
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
                auto msgs = importer.from_bulletin(*newmsg);

                // Update reference time
                const Datetime& dt = msgs[0]->get_datetime();
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
            std::string newrmsg = newmsg->encode();
            output.ok(newrmsg);
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

    void process_stdin(Output& output)
    {
        auto file(dballe::File::create(dballe::Encoding::BUFR, stdin, false, "[standard input]").release());
        process(*file, output);
    }

    void process(const std::string& filename, Output& output)
    {
        auto file(dballe::File::create(dballe::Encoding::BUFR, filename.c_str(), "r").release());
        process(*file, output);
    }

    void process(dballe::File& infile, Output& output)
    {
        // Use .release() so the code is the same even with the new C++11's dballe
        auto importer = Importer::create(dballe::Encoding::BUFR);

        while (BinaryMessage rmsg = infile.read())
        {
            // Decode message
            std::unique_ptr<BufrBulletin> msg;
            bool decoded;
            try {
                msg = BufrBulletin::decode(rmsg.data, rmsg.pathname.c_str(), rmsg.offset);
                decoded = true;
            } catch (std::exception& e) {
                nag::warning("%s:%ld: BUFR #%d failed to decode: %s.",
                    rmsg.pathname.c_str(), rmsg.offset, rmsg.index, e.what());
                decoded = false;
            }

            if (!decoded)
                output.fail(rmsg.data);
            else if (msg->subsets.size() == 1u && !override_usn_active)
                output.ok(rmsg.data);
            else
                splitmsg(rmsg, *msg, *importer, output);
        }
    }
};

}

int ArkiBUFRPrepare::run(int argc, const char* argv[])
{
    Options opts;
    try {
        if (opts.parse(argc, argv))
            return 0;

        nag::init(opts.verbose->isSet(), opts.debug->isSet());

        runtime::init();

        Copier copier;
        if (opts.force_usn->isSet())
            copier.override_usn(opts.force_usn->intValue());

        Output output;
        if (opts.outfile->isSet())
            output.open_ok_file(opts.outfile->stringValue());
        else
            output.open_ok_stdout();

        if (opts.failfile->isSet())
            output.open_fail_file(opts.failfile->stringValue());

        if (!opts.hasNext())
        {
            copier.process_stdin(output);
        } else {
            while (opts.hasNext())
                copier.process(opts.next().c_str(), output);
        }

        return 0;
    } catch (BadOption& e) {
        std::cerr << e.what() << std::endl;
        opts.outputHelp(std::cerr);
        return 1;
    } catch (std::exception& e) {
        std::cerr << e.what() << std::endl;
        return 1;
    }
}

}
}
