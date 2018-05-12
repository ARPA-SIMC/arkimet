/// Dump arkimet data files
#include "config.h"
#include <arki/utils/commandline/parser.h>
#include <arki/exceptions.h>
#include <arki/types/source.h>
#include <arki/metadata.h>
#include <arki/metadata/consumer.h>
#include <arki/matcher.h>
#include <arki/dataset/http.h>
#include <arki/summary.h>
#include <arki/formatter.h>
#include <arki/utils/geos.h>
#include <arki/utils/files.h>
#include <arki/binary.h>
#include <arki/runtime.h>
#include <arki/runtime/io.h>
#include <arki/runtime/config.h>
#include <arki/configfile.h>
#include <sstream>
#include <iostream>

using namespace std;
using namespace arki;
using namespace arki::core;
using namespace arki::types;
using namespace arki::utils;

namespace arki {
namespace utils {
namespace commandline {

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

}
}
}

namespace {

std::unique_ptr<utils::sys::NamedFileDescriptor> make_input(utils::commandline::Parser& opts)
{
    if (!opts.hasNext())
        return std::unique_ptr<utils::sys::NamedFileDescriptor>(new Stdin);

    string pathname = opts.next();
    if (pathname == "-")
        return std::unique_ptr<utils::sys::NamedFileDescriptor>(new Stdin);

    return std::unique_ptr<utils::sys::NamedFileDescriptor>(new runtime::InputFile(pathname));
}


struct YamlPrinter
{
    NamedFileDescriptor& out;
    Formatter* formatter = nullptr;

    YamlPrinter(NamedFileDescriptor& out, bool formatted=false)
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
        string res = serialize(md);
        out.write_all_or_throw(res.data(), res.size());
        return true;
    }

    bool print_summary(const Summary& s)
    {
        string res = serialize(s);
        out.write_all_or_throw(res.data(), res.size());
        return true;
    }

    std::string serialize(const Metadata& md)
    {
        stringstream ss;
        md.write_yaml(ss, formatter);
        ss << endl;
        return ss.str();
    }

    std::string serialize(const Summary& s)
    {
        stringstream ss;
        s.write_yaml(ss, formatter);
        ss << endl;
        return ss.str();
    }
};

}

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
            if (md.source().style() == Source::INLINE)
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
            Metadata::read_group(dec, bundle.version, in.name(), [&](unique_ptr<Metadata> md) { s.add(*md); return true; });
        }
        else
            throw_consistency_error("parsing file " + in.name(), "metadata entry does not start with 'MD', '!D', 'SU', or 'MG'");
    }
}
#endif

int main(int argc, const char* argv[])
{
    commandline::Options opts;
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
            cout << m.toStringExpanded() << endl;
            return 0;
        }

        if (opts.aliases->boolValue())
        {
            ConfigFile cfg;
            if (opts.hasNext())
            {
                dataset::http::Reader::getAliasDatabase(opts.next(), cfg);
            } else {
                MatcherAliasDatabase::serialise(cfg);
            }

            // Output the merged configuration
            string res = cfg.serialize();
            unique_ptr<sys::NamedFileDescriptor> out(runtime::make_output(*opts.outfile));
            out->write_all_or_throw(res);
            out->close();

            return 0;
        }

        if (opts.config->boolValue())
        {
            ConfigFile cfg;
            while (opts.hasNext())
                dataset::Reader::readConfig(opts.next(), cfg);

            // Output the merged configuration
            string res = cfg.serialize();
            unique_ptr<sys::NamedFileDescriptor> out(runtime::make_output(*opts.outfile));
            out->write_all_or_throw(res);
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
            stringstream ss;
            if (hull.get())
                ss << hull->toString() << endl;
            else
                ss << "no bounding box could be computed." << endl;

            // Open the output file
            unique_ptr<sys::NamedFileDescriptor> out(runtime::make_output(*opts.outfile));
            out->write_all_or_throw(ss.str());
            out->close();

            return 0;
        }
#endif

        if (opts.info->boolValue())
        {
            cout << "arkimet runtime information:" << endl;
            runtime::Config::get().describe(cout);
            return 0;
        }

        // Open the input file
        auto in = make_input(opts);

        // Open the output channel
        unique_ptr<sys::NamedFileDescriptor> out(runtime::make_output(*opts.outfile));

        if (opts.reverse_data->boolValue())
        {
            Metadata md;
            auto reader = LineReader::from_fd(*in);
            while (md.readYaml(*reader, in->name()))
                md.write(*out);
        }
        else if (opts.reverse_summary->boolValue())
        {
            Summary summary;
            auto reader = LineReader::from_fd(*in);
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
                    if (md.source().style() == Source::INLINE)
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
                    Metadata::read_group(dec, bundle.version, in->name(), [&](unique_ptr<Metadata> md) { return writer.print(*md); });
                }
                else
                    throw_consistency_error("parsing file " + in->name(), "metadata entry does not start with 'MD', '!D', 'SU', or 'MG'");
            }
// Uncomment as a quick hack to check memory usage at this point:
//system(str::fmtf("ps u %d >&2", getpid()).c_str());
//types::debug_intern_stats();

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
