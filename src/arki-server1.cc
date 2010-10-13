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

#include <fstream>
#include <iostream>
#include <cstring>

#include <sys/types.h>
#include <sys/socket.h>
#include <netdb.h>
#include <unistd.h>
#include <fcntl.h>

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

/**
 * Generic bind/listen/accept internet server
 */
struct Server
{
	// Human readable server hostname
	std::string host;
	// Human readable server port
	std::string port;
	// Server socket
	int sock;

	Server() : sock(-1)
	{
	}

	virtual ~Server()
	{
		if (sock >= 0) close(sock);
	}

	// Bind to a given port (and optionally interface hostname)
	void bind(const char* port, const char* host=NULL)
	{
		struct addrinfo hints;
		memset(&hints, 0, sizeof(addrinfo));
		hints.ai_family = AF_UNSPEC;
		hints.ai_socktype = SOCK_STREAM;
		hints.ai_protocol = 0;
		hints.ai_flags = AI_V4MAPPED | AI_ADDRCONFIG | AI_PASSIVE;

		struct addrinfo* res;
		int gaires = getaddrinfo(host, port, &hints, &res);
		if (gaires != 0)
			throw wibble::exception::Consistency(
					str::fmtf("resolving hostname %s:%s", host, port),
					gai_strerror(gaires));

		for (addrinfo* rp = res; rp != NULL; rp = rp->ai_next)
		{
			int sfd = socket(rp->ai_family, rp->ai_socktype, rp->ai_protocol);
			if (sfd == -1)
				continue;

			if (::bind(sfd, rp->ai_addr, rp->ai_addrlen) == 0)
			{
				// Success
				sock = sfd;

				// Resolve what we found back to human readable
				// form, to use for logging and error reporting
				char hbuf[NI_MAXHOST], sbuf[NI_MAXSERV];
				if (getnameinfo(rp->ai_addr, rp->ai_addrlen,
							hbuf, sizeof(hbuf),
							sbuf, sizeof(sbuf),
							NI_NUMERICHOST | NI_NUMERICSERV) == 0)
				{
					this->host = hbuf;
					this->port = sbuf;
				} else {
					this->host = "(unknown)";
					this->port = "(unknown)";
				}

				break;
			}

			close(sfd);
		}

		freeaddrinfo(res);

		if (sock == -1)
			// No address succeeded
			throw wibble::exception::Consistency(
					str::fmtf("binding to %s:%s", host, port),
					"could not bind to any of the resolved addresses");

		// Set close-on-exec on socket
		if (fcntl(sock, F_SETFD, FD_CLOEXEC) < 0)
			throw wibble::exception::System("setting FD_CLOEXEC on server socket");
	}

	// Set socket to listen, with given backlog
	void listen(int backlog = 32)
	{
		if (::listen(sock, backlog) < 0)
			throw wibble::exception::System("listening on port " + port);
	}

	// Loop forever accepting connections on the socket
	void accept_loop()
	{
		while (true)
		{
			struct sockaddr_storage peer_addr;
			socklen_t peer_addr_len = sizeof(struct sockaddr_storage);
			int fd = accept(sock, (sockaddr*)&peer_addr, (socklen_t*)&peer_addr_len);
			if (fd == -1)
				throw wibble::exception::System("listening on " + host + ":" + port);

			// Resolve the peer
			char hbuf[NI_MAXHOST], sbuf[NI_MAXSERV];
			int gaires = getnameinfo((struct sockaddr *)&peer_addr,
					peer_addr_len,
					hbuf, NI_MAXHOST,
					sbuf, NI_MAXSERV,
					NI_NUMERICSERV);
			if (gaires == 0)
				handle_client(fd, hbuf, sbuf);
			else
				throw wibble::exception::Consistency(
						"resolving peer name",
						gai_strerror(gaires));
		}
	}

	virtual void handle_client(int sock, const std::string& peer_host, const std::string& peer_port) = 0;
};

struct Echo : public Server
{
	virtual void handle_client(int sock, const std::string& peer_host, const std::string& peer_port)
	{
		cout << "Connection from " << peer_host << ":" << peer_port << endl;
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

		Echo echo;
		echo.bind("12345");
		echo.listen();
		echo.accept_loop();

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
