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

#include <string>
#include <vector>
#include <map>
#include <fstream>
#include <iostream>
#include <cstring>
#include <cctype>
#include <cerrno>
#include <cstdlib>

#include <sys/types.h>
#include <sys/socket.h>
#include <netdb.h>
#include <unistd.h>
#include <fcntl.h>
#include <signal.h>

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
	// Signals used to stop the server's accept loop
	std::vector<int> stop_signals;

	// Saved signal handlers before accept
	struct sigaction *old_signal_actions;
	// Signal handlers in use during accept
	struct sigaction *signal_actions;

	Server() : sock(-1), old_signal_actions(0), signal_actions(0)
	{
		// By default, stop on sigterm and sigint
		stop_signals.push_back(SIGTERM);
		stop_signals.push_back(SIGINT);
	}

	virtual ~Server()
	{
		if (sock >= 0) close(sock);
		if (old_signal_actions) delete[] old_signal_actions;
		if (signal_actions) delete[] signal_actions;
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

			// Set SO_REUSEADDR 
			int flag = 1;
			if (setsockopt(sfd, SOL_SOCKET, SO_REUSEADDR, &flag, sizeof(int)) < 0)
				throw wibble::exception::System("setting SO_REUSEADDR on socket");

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

		// Set close-on-exec on master socket
		if (fcntl(sock, F_SETFD, FD_CLOEXEC) < 0)
			throw wibble::exception::System("setting FD_CLOEXEC on server socket");
	}

	// Set socket to listen, with given backlog
	void listen(int backlog = 32)
	{
		if (::listen(sock, backlog) < 0)
			throw wibble::exception::System("listening on port " + port);
	}

	static void noop_signal_handler(int sig) {}

	// Initialize signal handling structures
	void signal_setup()
	{
		if (old_signal_actions) delete[] old_signal_actions;
		if (signal_actions) delete[] signal_actions;
		old_signal_actions = new struct sigaction[stop_signals.size()];
		signal_actions = new struct sigaction[stop_signals.size()];
		for (size_t i = 0; i < stop_signals.size(); ++i)
		{
			signal_actions[i].sa_handler = noop_signal_handler;
			sigemptyset(&(signal_actions[i].sa_mask));
			signal_actions[i].sa_flags = 0;
		}
	}

	void signal_install()
	{
		for (size_t i = 0; i < stop_signals.size(); ++i)
			if (sigaction(stop_signals[i], &signal_actions[i], &old_signal_actions[i]) < 0)
				throw wibble::exception::System("installing handler for signal " + str::fmt(stop_signals[i]));
	}

	void signal_uninstall()
	{
		for (size_t i = 0; i < stop_signals.size(); ++i)
			if (sigaction(stop_signals[i], &old_signal_actions[i], NULL) < 0)
				throw wibble::exception::System("restoring handler for signal " + str::fmt(stop_signals[i]));
	}

	// Loop accepting connections on the socket, until interrupted by a
	// signal in stop_signals
	void accept_loop()
	{
		struct SignalInstaller {
			Server& s;
			SignalInstaller(Server& s) : s(s) { s.signal_install(); }
			~SignalInstaller() { s.signal_uninstall(); }
		};

		signal_setup();

		while (true)
		{
			struct sockaddr_storage peer_addr;
			socklen_t peer_addr_len = sizeof(struct sockaddr_storage);
			int fd = -1;
			{
				SignalInstaller sigs(*this);
				fd = accept(sock, (sockaddr*)&peer_addr, (socklen_t*)&peer_addr_len);
				if (fd == -1)
				{
					if (errno == EINTR)
						return;
					throw wibble::exception::System("listening on " + host + ":" + port);
				}
			}

			// Resolve the peer
			char hbuf[NI_MAXHOST], sbuf[NI_MAXSERV];
			int gaires = getnameinfo((struct sockaddr *)&peer_addr,
					peer_addr_len,
					hbuf, NI_MAXHOST,
					sbuf, NI_MAXSERV,
					NI_NUMERICSERV);
			if (gaires == 0)
			{
				string hostname = hbuf;
				gaires = getnameinfo((struct sockaddr *)&peer_addr,
						peer_addr_len,
						hbuf, NI_MAXHOST,
						sbuf, NI_MAXSERV,
						NI_NUMERICHOST | NI_NUMERICSERV);
				if (gaires == 0)
					handle_client(fd, hostname, hbuf, sbuf);
				else
					throw wibble::exception::Consistency(
							"resolving peer name numerically",
							gai_strerror(gaires));
			}
			else
				throw wibble::exception::Consistency(
						"resolving peer name",
						gai_strerror(gaires));
		}
	}

	virtual void handle_client(int sock, const std::string& peer_hostname, const std::string& peer_hostaddr, const std::string& peer_port) = 0;
};

struct HTTPRequest
{
	string method;
	string url;
	string version;
	map<string, string> headers;
	Splitter space_splitter;
	ERegexp header_splitter;

	HTTPRequest()
		: space_splitter("[[:blank:]]+", REG_EXTENDED),
		  header_splitter("^([^:[:blank:]]+)[[:blank:]]*:[[:blank:]]*(.+)", 3)
	{
	}

	bool read_request(int sock)
	{
		// Set all structures to default values
		method = "GET";
		url = "/";
		version = "HTTP/1.0";
		headers.clear();

		// Read request line
		if (!read_method(sock))
			return false;

		// Read request headers
		read_headers(sock);

		// Message body is not read here
		return true;
	}

	/**
	 * Read a line from the file descriptor.
	 *
	 * The line is terminated by <CR><LF>. The line terminator is not
	 * included in the resulting string.
	 *
	 * @returns true if a line was read, false if EOF
	 *
	 * Note that if EOF is returned, res can still be filled with a partial
	 * line. This may happen if the connection ends after some data has
	 * been sent but before <CR><LF> is sent.
	 */
	bool read_line(int sock, string& res)
	{
		bool has_cr = false;
		res.clear();
		while (true)
		{
			char c;
			ssize_t count = read(sock, &c, 1);
			if (count == 0) return false;
			switch (c)
			{
				case '\r':
					if (has_cr)
						res.append(1, '\r');
					has_cr = true;
					break;
				case '\n':
					if (has_cr)
						return true;
					else
						res.append(1, '\n');
					break;
				default:
					res.append(1, c);
					break;
			}
		}
	}

	// Read HTTP method and its following empty line
	bool read_method(int sock)
	{
		// Request line, such as GET /images/logo.png HTTP/1.1, which
		// requests a resource called /images/logo.png from server
		string cmdline;
		if (!read_line(sock, cmdline)) return false;

		// If we cannot fill some of method, url or version we just let
		// them be, as they have previously been filled with defaults
		// by read_request()
		Splitter::const_iterator i = space_splitter.begin(cmdline);
		if (i != space_splitter.end())
		{
			method = str::toupper(*i);
			++i;
			if (i != space_splitter.end())
			{
				url = *i;
				++i;
				if (i != space_splitter.end())
					version = *i;
			}
		}

		// An empty line
		return read_line(sock, cmdline);
	}

	/**
	 * Read HTTP headers
	 *
	 * @return true if there still data to read and headers are terminated
	 * by an empty line, false if headers are terminated by EOF
	 */
	bool read_headers(int sock)
	{
		string line;
		map<string, string>::iterator last_inserted = headers.end();
		while (read_line(sock, line))
		{
			// Headers are terminated by an empty line
			if (line.empty())
				return true;

			if (isblank(line[0]))
			{
				// Append continuation of previous header body
				if (last_inserted != headers.end())
				{
					last_inserted->second.append(" ");
					last_inserted->second.append(str::trim(line));
				}
			} else {
				if (header_splitter.match(line))
				{
					// rfc2616, 4.2 Message Headers:
					// Multiple message-header fields with
					// the same field-name MAY be present
					// in a message if and only if the
					// entire field-value for that header
					// field is defined as a
					// comma-separated list [i.e.,
					// #(values)].  It MUST be possible to
					// combine the multiple header fields
					// into one "field-name: field-value"
					// pair, without changing the semantics
					// of the message, by appending each
					// subsequent field-value to the first,
					// each separated by a comma.
					string key = str::tolower(header_splitter[1]);
					string val = str::trim(header_splitter[2]);
					map<string, string>::iterator old = headers.find(key);
					if (old == headers.end())
					{
						// Insert new
						pair< map<string, string>::iterator, bool > res =
							headers.insert(make_pair(key, val));
						last_inserted = res.first;
					} else {
						// Append comma-separated
						old->second.append(",");
						old->second.append(val);
						last_inserted = old;
					}
				} else
					last_inserted = headers.end();
			}
		}
		return false;
	}

	// Set the CGI environment variables for the current process using this
	// request
	void set_cgi_env()
	{
		// SERVER_PROTOCOL — HTTP/version.
		setenv("SERVER_PROTOCOL", version.c_str(), 1);
		// REQUEST_METHOD — name of HTTP method (see above).
		setenv("REQUEST_METHOD", method.c_str(), 1);
		// PATH_INFO — path suffix, if appended to URL after program name and a slash.
		// PATH_TRANSLATED — corresponding full path as supposed by server, if PATH_INFO is present.
		// SCRIPT_NAME — relative path to the program, like /cgi-bin/script.cgi.
		// QUERY_STRING — the part of URL after ? character. Must be composed of name=value pairs separated with ampersands (such as var1=val1&var2=val2…) and used when form data are transferred via GET method.
		// AUTH_TYPE — identification type, if applicable.
		// REMOTE_USER used for certain AUTH_TYPEs.
		// REMOTE_IDENT — see ident, only if server performed such lookup.
		// CONTENT_TYPE — MIME type of input data if PUT or POST method are used, as provided via HTTP header.
		// CONTENT_LENGTH — similarly, size of input data (decimal, in octets) if provided via HTTP header.
		// Variables passed by user agent (HTTP_ACCEPT, HTTP_ACCEPT_LANGUAGE, HTTP_USER_AGENT, HTTP_COOKIE and possibly others) contain values of corresponding HTTP headers and therefore have the same sense.
		for (map<string, string>::const_iterator i = headers.begin();
				i != headers.end(); ++i)
		{
			string name = "HTTP_";
			for (string::const_iterator j = i->first.begin();
					j != i->first.end(); ++j)
				if (isalnum(*j))
					name.append(1, toupper(*j));
				else
					name.append(1, '_');
			setenv(name.c_str(), i->second.c_str(), 1);
		}
	}
};

struct HTTP : public Server
{
	virtual void handle_client(int sock,
			const std::string& peer_hostname,
			const std::string& peer_hostaddr,
			const std::string& peer_port)
	{
		cout << "Connection from " << peer_hostname << " " << peer_hostaddr << ":" << peer_port << endl;

		// Set CGI server-specific variables

		// SERVER_SOFTWARE — name/version of HTTP server.
		setenv("SERVER_SOFTWARE", "arki-server/" PACKAGE_VERSION, 1);
		// SERVER_NAME — host name of the server, may be dot-decimal IP address.
		setenv("SERVER_NAME", host.c_str(), 1);
		// GATEWAY_INTERFACE — CGI/version.
		setenv("GATEWAY_INTERFACE", "CGI/1.1", 1);

		// Set some request-specific variables

		// SERVER_PORT — TCP port (decimal).
		setenv("SERVER_PORT", port.c_str(), 1);
		// REMOTE_HOST — host name of the client, unset if server did not perform such lookup.
		setenv("REMOTE_HOST", peer_hostname.c_str(), 1);
		// REMOTE_ADDR — IP address of the client (dot-decimal).
		setenv("REMOTE_ADDR", peer_hostaddr.c_str(), 1);

		HTTPRequest req;
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

			system("set");

			/*
			local res
			repeat
				req.params = nil
				parse_url (req)
				res = make_response (req)
			until handle_request (req, res) ~= "reparse"
			send_response (req, res)

			req.socket:flush ()
			if not res.keep_alive then
				break
			end
			*/
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

		HTTP http;
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
