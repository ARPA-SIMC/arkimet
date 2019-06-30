#include "arki/../config.h"
#include "arki/runtime/arki-dump.h"
#include "arki/runtime.h"
#include "arki/runtime/io.h"
#include "arki/runtime/inputs.h"
#include "arki/runtime/config.h"
#include "arki/core/file.h"
#include "arki/utils/commandline/parser.h"
#include "arki/utils/sys.h"
#include "arki/utils/geos.h"
#include "arki/formatter.h"
#include "arki/metadata.h"
#include "arki/summary.h"
#include "arki/binary.h"
#include "arki/types/source.h"
#include "arki/matcher.h"
#include "arki/dataset/http.h"
#include <sstream>
#include <string>
#include <iostream>

using namespace arki::utils;

namespace arki {
namespace runtime {

namespace {

using namespace arki::utils::commandline;

struct Options : public StandardParserWithManpage
{
    BoolOption* reverse_data;
    BoolOption* reverse_summary;
    BoolOption* annotate;
    BoolOption* query;
    BoolOption* config;
    BoolOption* aliases;
    BoolOption* bbox;
    BoolOption* info;
    StringOption* outfile;

    Options() : StandardParserWithManpage("arki-dump", PACKAGE_VERSION, 1, PACKAGE_BUGREPORT)
    {
        usage = "[options] [input]";
        description =
            "Read data from the given input file (or stdin), and dump them"
            " in human readable format on stdout.";

        outfile = add<StringOption>("output", 'o', "output", "file",
                "write the output to the given file instead of standard output");
        annotate = add<BoolOption>("annotate", 0, "annotate", "",
                "annotate the human-readable Yaml output with field descriptions");

        reverse_data = add<BoolOption>("from-yaml-data", 0, "from-yaml-data", "",
            "read a Yaml data dump and write binary metadata");

        reverse_summary = add<BoolOption>("from-yaml-summary", 0, "from-yaml-summary", "",
            "read a Yaml summary dump and write a binary summary");

        query = add<BoolOption>("query", 0, "query", "",
            "print a query (specified on the command line) with the aliases expanded");

        config = add<BoolOption>("config", 0, "config", "",
            "print the arkimet configuration used to access the given file or dataset or URL");

        aliases = add<BoolOption>("aliases", 0, "aliases", "", "dump the alias database (to dump the aliases of a remote server, put the server URL on the command line)");

        info = add<BoolOption>("info", 0, "info", "", "dump configuration information");

#ifdef HAVE_GEOS
        bbox = add<BoolOption>("bbox", 0, "bbox", "", "dump the bounding box");
#else
        bbox = 0;
#endif

    }
};

std::unique_ptr<utils::sys::NamedFileDescriptor> make_input(utils::commandline::Parser& opts)
{
    if (!opts.hasNext())
        return std::unique_ptr<utils::sys::NamedFileDescriptor>(new core::Stdin);

    std::string pathname = opts.next();
    if (pathname == "-")
        return std::unique_ptr<utils::sys::NamedFileDescriptor>(new core::Stdin);

    return std::unique_ptr<utils::sys::NamedFileDescriptor>(new runtime::InputFile(pathname));
}


struct YamlPrinter
{
    sys::NamedFileDescriptor& out;
    Formatter* formatter = nullptr;

    YamlPrinter(sys::NamedFileDescriptor& out, bool formatted=false)
        : out(out)
    {
        if (formatted)
            formatter = Formatter::create().release();
    }
    ~YamlPrinter()
    {
        delete formatter;
    }

    bool print(const Metadata& md)
    {
        std::string res = serialize(md);
        out.write_all_or_throw(res.data(), res.size());
        return true;
    }

    bool print_summary(const Summary& s)
    {
        std::string res = serialize(s);
        out.write_all_or_throw(res.data(), res.size());
        return true;
    }

    std::string serialize(const Metadata& md)
    {
        std::stringstream ss;
        md.write_yaml(ss, formatter);
        ss << std::endl;
        return ss.str();
    }

    std::string serialize(const Summary& s)
    {
        std::stringstream ss;
        s.write_yaml(ss, formatter);
        ss << std::endl;
        return ss.str();
    }
};

#ifdef HAVE_GEOS
// Add to \a s the info from all data read from \a in
static void addToSummary(sys::NamedFileDescriptor& in, Summary& s)
{
    Metadata md;
    Summary summary;

    types::Bundle bundle;
    while (bundle.read_header(in))
    {
        if (bundle.signature == "MD" || bundle.signature == "!D")
        {
            if (!bundle.read_data(in)) break;
            BinaryDecoder dec(bundle.data);
            md.read_inner(dec, bundle.version, in.name());
            if (md.source().style() == types::Source::INLINE)
                md.read_inline_data(in);
            s.add(md);
        }
        else if (bundle.signature == "SU")
        {
            if (!bundle.read_data(in)) break;
            BinaryDecoder dec(bundle.data);
            summary.read_inner(dec, bundle.version, in.name());
            s.add(summary);
        }
        else if (bundle.signature == "MG")
        {
            if (!bundle.read_data(in)) break;
            BinaryDecoder dec(bundle.data);
            Metadata::read_group(dec, bundle.version, in.name(), [&](std::unique_ptr<Metadata> md) { s.add(*md); return true; });
        }
        else
            throw std::runtime_error(in.name() + ": metadata entry does not start with 'MD', '!D', 'SU', or 'MG'");
    }
}
#endif
}

int ArkiDump::run(int argc, const char* argv[])
{
    Options opts;
    try {
        if (opts.parse(argc, argv))
            return 0;

        runtime::init();

        // Validate command line options
        if (opts.query->boolValue() && opts.aliases->boolValue())
            throw commandline::BadOption("--query conflicts with --aliases");
        if (opts.query->boolValue() && opts.config->boolValue())
            throw commandline::BadOption("--query conflicts with --config");
        if (opts.query->boolValue() && opts.reverse_data->boolValue())
            throw commandline::BadOption("--query conflicts with --from-yaml-data");
        if (opts.query->boolValue() && opts.reverse_summary->boolValue())
            throw commandline::BadOption("--query conflicts with --from-yaml-summary");
        if (opts.query->boolValue() && opts.annotate->boolValue())
            throw commandline::BadOption("--query conflicts with --annotate");
        if (opts.query->boolValue() && opts.bbox && opts.bbox->isSet())
            throw commandline::BadOption("--query conflicts with --bbox");
        if (opts.query->boolValue() && opts.info && opts.info->isSet())
            throw commandline::BadOption("--query conflicts with --info");

        if (opts.config->boolValue() && opts.aliases->boolValue())
            throw commandline::BadOption("--config conflicts with --aliases");
        if (opts.config->boolValue() && opts.reverse_data->boolValue())
            throw commandline::BadOption("--config conflicts with --from-yaml-data");
        if (opts.config->boolValue() && opts.reverse_summary->boolValue())
            throw commandline::BadOption("--config conflicts with --from-yaml-summary");
        if (opts.config->boolValue() && opts.annotate->boolValue())
            throw commandline::BadOption("--config conflicts with --annotate");
        if (opts.config->boolValue() && opts.bbox && opts.bbox->isSet())
            throw commandline::BadOption("--config conflicts with --bbox");
        if (opts.config->boolValue() && opts.info && opts.info->isSet())
            throw commandline::BadOption("--config conflicts with --info");

        if (opts.aliases->boolValue() && opts.reverse_data->boolValue())
            throw commandline::BadOption("--aliases conflicts with --from-yaml-data");
        if (opts.aliases->boolValue() && opts.reverse_summary->boolValue())
            throw commandline::BadOption("--aliases conflicts with --from-yaml-summary");
        if (opts.aliases->boolValue() && opts.annotate->boolValue())
            throw commandline::BadOption("--aliases conflicts with --annotate");
        if (opts.aliases->boolValue() && opts.bbox && opts.bbox->isSet())
            throw commandline::BadOption("--aliases conflicts with --bbox");
        if (opts.aliases->boolValue() && opts.info && opts.info->isSet())
            throw commandline::BadOption("--aliases conflicts with --info");

        if (opts.reverse_data->boolValue() && opts.reverse_summary->boolValue())
            throw commandline::BadOption("--from-yaml-data conflicts with --from-yaml-summary");
        if (opts.annotate->boolValue() && opts.reverse_data->boolValue())
            throw commandline::BadOption("--annotate conflicts with --from-yaml-data");
        if (opts.annotate->boolValue() && opts.reverse_summary->boolValue())
            throw commandline::BadOption("--annotate conflicts with --from-yaml-summary");
        if (opts.annotate->boolValue() && opts.bbox && opts.bbox->isSet())
            throw commandline::BadOption("--annotate conflicts with --bbox");
        if (opts.annotate->boolValue() && opts.info && opts.info->isSet())
            throw commandline::BadOption("--annotate conflicts with --info");

        if (opts.query->boolValue())
        {
            if (!opts.hasNext())
                throw commandline::BadOption("--query wants the query on the command line");
            Matcher m = Matcher::parse(opts.next());
            std::cout << m.toStringExpanded() << std::endl;
            return 0;
        }

        if (opts.aliases->boolValue())
        {
            core::cfg::Sections cfg = opts.hasNext() ? dataset::http::Reader::getAliasDatabase(opts.next()) : MatcherAliasDatabase::serialise();

            // Output the merged configuration
            std::stringstream ss;
            cfg.write(ss, "memory");
            std::unique_ptr<sys::NamedFileDescriptor> out(runtime::make_output(*opts.outfile));
            out->write_all_or_throw(ss.str());
            out->close();

            return 0;
        }

        if (opts.config->boolValue())
        {
            core::cfg::Sections merged;
            runtime::Inputs inputs(merged);
            while (opts.hasNext())
                inputs.add_pathname(opts.next());

            // Output the merged configuration
            std::stringstream ss;
            merged.write(ss, "memory");
            std::unique_ptr<sys::NamedFileDescriptor> out(runtime::make_output(*opts.outfile));
            out->write_all_or_throw(ss.str());
            out->close();

            return 0;
        }

#ifdef HAVE_GEOS
        if (opts.bbox->boolValue())
        {
            // Open the input file
            auto in = make_input(opts);

            // Read everything into a single summary
            Summary summary;
            addToSummary(*in, summary);

            // Get the bounding box
            auto hull = summary.getConvexHull();

            // Print it out
            std::stringstream ss;
            if (hull.get())
                ss << hull->toString() << std::endl;
            else
                ss << "no bounding box could be computed." << std::endl;

            // Open the output file
            std::unique_ptr<sys::NamedFileDescriptor> out(runtime::make_output(*opts.outfile));
            out->write_all_or_throw(ss.str());
            out->close();

            return 0;
        }
#endif

        if (opts.info->boolValue())
        {
            std::cout << "arkimet runtime information:" << std::endl;
            runtime::Config::get().describe(std::cout);
            return 0;
        }

        // Open the input file
        auto in = make_input(opts);

        // Open the output channel
        std::unique_ptr<sys::NamedFileDescriptor> out(runtime::make_output(*opts.outfile));

        if (opts.reverse_data->boolValue())
        {
            Metadata md;
            auto reader = core::LineReader::from_fd(*in);
            while (md.readYaml(*reader, in->name()))
                md.write(*out);
        }
        else if (opts.reverse_summary->boolValue())
        {
            Summary summary;
            auto reader = core::LineReader::from_fd(*in);
            while (summary.readYaml(*reader, in->name()))
                summary.write(*out, out->name());
        }
        else
        {
            YamlPrinter writer(*out, opts.annotate->boolValue());

            Metadata md;
            Summary summary;

            types::Bundle bundle;
            while (bundle.read_header(*in))
            {
                if (bundle.signature == "MD" || bundle.signature == "!D")
                {
                    if (!bundle.read_data(*in)) break;
                    BinaryDecoder dec(bundle.data);
                    md.read_inner(dec, bundle.version, in->name());
                    if (md.source().style() == types::Source::INLINE)
                        md.read_inline_data(*in);
                    writer.print(md);
                }
                else if (bundle.signature == "SU")
                {
                    if (!bundle.read_data(*in)) break;
                    BinaryDecoder dec(bundle.data);
                    summary.read_inner(dec, bundle.version, in->name());
                    writer.print_summary(summary);
                }
                else if (bundle.signature == "MG")
                {
                    if (!bundle.read_data(*in)) break;
                    BinaryDecoder dec(bundle.data);
                    Metadata::read_group(dec, bundle.version, in->name(), [&](std::unique_ptr<Metadata> md) { return writer.print(*md); });
                }
                else
                    throw std::runtime_error(in->name() + ": metadata entry does not start with 'MD', '!D', 'SU', or 'MG'");
            }
// Uncomment as a quick hack to check memory usage at this point:
//system(str::fmtf("ps u %d >&2", getpid()).c_str());
//types::debug_intern_stats();

        }

        return 0;
    } catch (commandline::BadOption& e) {
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
