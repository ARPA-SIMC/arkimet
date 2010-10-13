/*
 * arki-server - Arkimet server
 *
 * Copyright (C) 2007--2010  ARPA-SIM <urpsim@smr.arpa.emr.it>
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

#include <wibble/exception.h>
#include <wibble/commandline/parser.h>
#include <wibble/regexp.h>
#include <wibble/string.h>
#include <arki/configfile.h>
#if 0
#include <arki/metadata.h>
#include <arki/metadata/consumer.h>
#include <arki/matcher.h>
#include <arki/dataset/http.h>
#include <arki/summary.h>
#include <arki/formatter.h>
#include <arki/utils/geosdef.h>
#endif
#include <arki/runtime.h>
#include <arki/utils/server.h>
#include <arki/utils/http.h>

#include <string>
#include <vector>
#include <map>
#include <fstream>
#include <iostream>
#include <cctype>
#include <cerrno>
#include <cstdlib>

#include "config.h"

using namespace std;
using namespace arki;
using namespace wibble;

namespace wibble {
namespace commandline {

struct Options : public StandardParserWithManpage
{
#if 0
	BoolOption* reverse_data;
	BoolOption* reverse_summary;
	BoolOption* annotate;
	BoolOption* query;
	BoolOption* config;
	BoolOption* aliases;
	BoolOption* bbox;
	StringOption* outfile;
#endif
	Options() : StandardParserWithManpage("arki-server", PACKAGE_VERSION, 1, PACKAGE_BUGREPORT)
	{
#if 0
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
#endif
	}
};

}
}

struct HTTP : public utils::net::TCPServer
{
	HTTP(const std::string& server_name)
	{
		// Set CGI server-specific variables

		// SERVER_SOFTWARE — name/version of HTTP server.
		setenv("SERVER_SOFTWARE", "arki-server/" PACKAGE_VERSION, 1);
		// SERVER_NAME — host name of the server, may be dot-decimal IP address.
		setenv("SERVER_NAME", server_name.c_str(), 1);
		// GATEWAY_INTERFACE — CGI/version.
		setenv("GATEWAY_INTERFACE", "CGI/1.1", 1);
	}

	virtual void handle_client(int sock,
			const std::string& peer_hostname,
			const std::string& peer_hostaddr,
			const std::string& peer_port)
	{
		cout << "Connection from " << peer_hostname << " " << peer_hostaddr << ":" << peer_port << endl;
		// Set some request-specific variables

		// SERVER_PORT — TCP port (decimal).
		setenv("SERVER_PORT", port.c_str(), 1);
		// REMOTE_HOST — host name of the client, unset if server did not perform such lookup.
		setenv("REMOTE_HOST", peer_hostname.c_str(), 1);
		// REMOTE_ADDR — IP address of the client (dot-decimal).
		setenv("REMOTE_ADDR", peer_hostaddr.c_str(), 1);

		utils::http::Request req;
		while (req.read_request(sock))
		{
			// Request line and headers have been read
			// sock now points to the optional message body

			// Dump request
			cout << "Method: " << req.method << endl;
			cout << "URL: " << req.url << endl;
			cout << "Version: " << req.version << endl;
			cout << "Headers:" << endl;
			for (map<string, string>::const_iterator i = req.headers.begin();
					i != req.headers.end(); ++i)
				cout << " " << i->first <<  ": " << i->second << endl;

			req.set_cgi_env();

			// TODO: still need to set:
			// SCRIPT_NAME — relative path to the program, like /cgi-bin/script.cgi.
			// PATH_INFO — path suffix, if appended to URL after program name and a slash.
			unsetenv("PATH_INFO");
			// PATH_TRANSLATED — corresponding full path as supposed by server, if PATH_INFO is present.
			unsetenv("PATH_TRANSLATED");

			if (dup2(sock, 0) < 0)
				throw wibble::exception::System("redirecting input socket to stdin");

			system("set");

			// Here there can be some keep-alive bit
			break;
		}

		close(sock);
	}
};

int main(int argc, const char* argv[])
{
	wibble::commandline::Options opts;
	try {
		if (opts.parse(argc, argv))
			return 0;

		runtime::init();

		HTTP http("http://localhost:12345");
		http.bind("12345");
		cout << "Listening on " << http.host << ":" << http.port << endl;
		http.listen();
		http.accept_loop();
		cout << "Done." << endl;

#if 0
		// Validate command line options
		if (opts.query->boolValue() && opts.aliases->boolValue())
			throw wibble::exception::BadOption("--query conflicts with --aliases");
		if (opts.query->boolValue() && opts.config->boolValue())
			throw wibble::exception::BadOption("--query conflicts with --config");
		if (opts.query->boolValue() && opts.reverse_data->boolValue())
			throw wibble::exception::BadOption("--query conflicts with --from-yaml-data");
		if (opts.query->boolValue() && opts.reverse_summary->boolValue())
			throw wibble::exception::BadOption("--query conflicts with --from-yaml-summary");
		if (opts.query->boolValue() && opts.annotate->boolValue())
			throw wibble::exception::BadOption("--query conflicts with --annotate");
		if (opts.query->boolValue() && opts.bbox && opts.bbox->isSet())
			throw wibble::exception::BadOption("--query conflicts with --bbox");

		if (opts.config->boolValue() && opts.aliases->boolValue())
			throw wibble::exception::BadOption("--config conflicts with --aliases");
		if (opts.config->boolValue() && opts.reverse_data->boolValue())
			throw wibble::exception::BadOption("--config conflicts with --from-yaml-data");
		if (opts.config->boolValue() && opts.reverse_summary->boolValue())
			throw wibble::exception::BadOption("--config conflicts with --from-yaml-summary");
		if (opts.config->boolValue() && opts.annotate->boolValue())
			throw wibble::exception::BadOption("--config conflicts with --annotate");
		if (opts.config->boolValue() && opts.bbox && opts.bbox->isSet())
			throw wibble::exception::BadOption("--config conflicts with --bbox");

		if (opts.aliases->boolValue() && opts.reverse_data->boolValue())
			throw wibble::exception::BadOption("--aliases conflicts with --from-yaml-data");
		if (opts.aliases->boolValue() && opts.reverse_summary->boolValue())
			throw wibble::exception::BadOption("--aliases conflicts with --from-yaml-summary");
		if (opts.aliases->boolValue() && opts.annotate->boolValue())
			throw wibble::exception::BadOption("--aliases conflicts with --annotate");
		if (opts.aliases->boolValue() && opts.bbox && opts.bbox->isSet())
			throw wibble::exception::BadOption("--aliases conflicts with --bbox");

		if (opts.reverse_data->boolValue() && opts.reverse_summary->boolValue())
			throw wibble::exception::BadOption("--from-yaml-data conflicts with --from-yaml-summary");
		if (opts.annotate->boolValue() && opts.reverse_data->boolValue())
			throw wibble::exception::BadOption("--annotate conflicts with --from-yaml-data");
		if (opts.annotate->boolValue() && opts.reverse_summary->boolValue())
			throw wibble::exception::BadOption("--annotate conflicts with --from-yaml-summary");
		if (opts.annotate->boolValue() && opts.bbox && opts.bbox->isSet())
			throw wibble::exception::BadOption("--annotate conflicts with --bbox");

		if (opts.query->boolValue())
		{
			if (!opts.hasNext())
				throw wibble::exception::BadOption("--query wants the query on the command line");
			Matcher m = Matcher::parse(opts.next());
			cout << m.toStringExpanded() << endl;
			return 0;
		}
		
		if (opts.aliases->boolValue())
		{
			ConfigFile cfg;
			if (opts.hasNext())
			{
				dataset::HTTP::getAliasDatabase(opts.next(), cfg);
			} else {
				MatcherAliasDatabase::serialise(cfg);
			}
			
			// Open the output file
			runtime::Output out(*opts.outfile);

			// Output the merged configuration
			cfg.output(out.stream(), out.name());

			return 0;
		}

		if (opts.config->boolValue())
		{
			ConfigFile cfg;
			while (opts.hasNext())
			{
				ReadonlyDataset::readConfig(opts.next(), cfg);
			}
			
			// Open the output file
			runtime::Output out(*opts.outfile);

			// Output the merged configuration
			cfg.output(out.stream(), out.name());

			return 0;
		}

#ifdef HAVE_GEOS
		if (opts.bbox->boolValue())
		{
			// Open the input file
			runtime::Input in(opts);

			// Read everything into a single summary
			Summary summary;
			addToSummary(in, summary);

			// Get the bounding box
			ARKI_GEOS_GEOMETRYFACTORY gf;
			std::auto_ptr<ARKI_GEOS_GEOMETRY> hull = summary.getConvexHull(gf);

			// Open the output file
			runtime::Output out(*opts.outfile);

			// Print it out
			if (hull.get())
				out.stream() << hull->toString() << endl;
			else
				out.stream() << "no bounding box could be computed." << endl;

			return 0;
		}
#endif

		// Open the input file
		runtime::Input in(opts);

		// Open the output channel
		runtime::Output out(*opts.outfile);

		if (opts.reverse_data->boolValue())
		{
			Metadata md;
			while (md.readYaml(in.stream(), in.name()))
				md.write(out.stream(), out.name());
		}
		else if (opts.reverse_summary->boolValue())
		{
			Summary summary;
			while (summary.readYaml(in.stream(), in.name()))
				summary.write(out.stream(), out.name());
		}
		else
		{
			Formatter* formatter = 0;
			if (opts.annotate->boolValue())
				formatter = Formatter::create();
			YamlWriter writer(out, formatter);

			Metadata md;
			Summary summary;

			wibble::sys::Buffer buf;
			string signature;
			unsigned version;

			while (types::readBundle(in.stream(), in.name(), buf, signature, version))
			{
				if (signature == "MD" || signature == "!D")
				{
					md.read(buf, version, in.name());
					if (md.source->style() == types::Source::INLINE)
						md.readInlineData(in.stream(), in.name());
					writer(md);
				}
				else if (signature == "SU")
				{
					summary.read(buf, version, in.name());
					summary.writeYaml(out.stream(), formatter);
					out.stream() << endl;
				}
				else if (signature == "MG")
				{
					Metadata::readGroup(buf, version, in.name(), writer);
				}
			}
// Uncomment as a quick hack to check memory usage at this point:
//system(str::fmtf("ps u %d >&2", getpid()).c_str());
//types::debug_intern_stats();

		}
#endif
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
