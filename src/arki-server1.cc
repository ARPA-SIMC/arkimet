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
#include <wibble/sys/childprocess.h>
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

#include <sys/types.h>
#include <sys/time.h>
#include <sys/wait.h>
#include <sys/socket.h>

#include "config.h"

using namespace std;
using namespace arki;
using namespace wibble;

namespace wibble {
namespace commandline {

struct Options : public StandardParserWithManpage
{
	StringOption* host;
	StringOption* port;
	StringOption* url;
	StringOption* runtest;
	StringOption* accesslog;
	StringOption* errorlog;
	BoolOption* quiet;

	Options() : StandardParserWithManpage("arki-server", PACKAGE_VERSION, 1, PACKAGE_BUGREPORT)
	{
		usage = "[options] configfile";
		description =
			"Start the arkimet server, serving the datasets found"
			" in the configuration file";

		host = add<StringOption>("host", 0, "host", "hostname",
				"interface to listen to. Default: all interfaces");
		port = add<StringOption>("port", 'p', "port", "port",
				"port to listen not. Default: 8080");
		url = add<StringOption>("url", 0, "url", "url",
				"url to use to reach the server");
		runtest = add<StringOption>("runtest", 0, "runtest", "cmd",
				"start the server, run the given test command"
				" and return its exit status");
		accesslog = add<StringOption>("accesslog", 0, "accesslog", "file",
				"file where to log normal access information");
		errorlog = add<StringOption>("errorlog", 0, "errorlog", "file",
				"file where to log errors");
		quiet = add<BoolOption>("quiet", 0, "quiet", "",
			"do not log to standard output");
	}
};

}
}

struct Handler : public sys::ChildProcess
{
	int sock;
	std::string peer_hostname;
	std::string peer_hostaddr;
	std::string peer_port;
	std::string server_name;
	std::string server_port;

	// Executed in child thread
	virtual int main()
	{
		try {
			// Set CGI server-specific variables

			// SERVER_SOFTWARE — name/version of HTTP server.
			setenv("SERVER_SOFTWARE", "arki-server/" PACKAGE_VERSION, 1);
			// SERVER_NAME — host name of the server, may be dot-decimal IP address.
			setenv("SERVER_NAME", server_name.c_str(), 1);
			// GATEWAY_INTERFACE — CGI/version.
			setenv("GATEWAY_INTERFACE", "CGI/1.1", 1);

			// Set some request-specific variables

			// SERVER_PORT — TCP port (decimal).
			setenv("SERVER_PORT", server_port.c_str(), 1);
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
				cerr << "Method: " << req.method << endl;
				cerr << "URL: " << req.url << endl;
				cerr << "Version: " << req.version << endl;
				cerr << "Headers:" << endl;
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
				if (dup2(sock, 1) < 0)
					throw wibble::exception::System("redirecting input socket to stdout");

				system("set");

				close(0);
				close(1);

				// Here there can be some keep-alive bit
				break;
			}

			close(sock);

			return 0;
		} catch (std::exception& e) {
			cerr << e.what() << endl;
			return 1;
		}
	}
};

struct HTTP : public utils::net::TCPServer
{
	string server_name;
	map<pid_t, Handler*> children;

	void set_server_name(const std::string& server_name)
	{
		this->server_name = server_name;
	}

	virtual void handle_client(int sock,
			const std::string& peer_hostname,
			const std::string& peer_hostaddr,
			const std::string& peer_port)
	{
		cout << "Connection from " << peer_hostname << " " << peer_hostaddr << ":" << peer_port << endl;

		// Set some timeout on this socket to avoid getting stuck if
		// clients are stuck
		struct timeval timeout;
		timeout.tv_sec = 300;
		timeout.tv_usec = 0;
		if (setsockopt(sock, SOL_SOCKET, SO_RCVTIMEO, &timeout, sizeof(struct timeval)) < 0)
			throw wibble::exception::System("setting SO_RCVTIMEO on socket");
		if (setsockopt(sock, SOL_SOCKET, SO_SNDTIMEO, &timeout, sizeof(struct timeval)) < 0)
			throw wibble::exception::System("setting SO_SNDTIMEO on socket");

		auto_ptr<Handler> handler(new Handler);
		handler->sock = sock;
		handler->peer_hostname = peer_hostname;
		handler->peer_hostaddr = peer_hostaddr;
		handler->peer_port = peer_port;
		handler->server_name = server_name;
		handler->server_port = port;
		pid_t pid = handler->fork();
		children[pid] = handler.release();

		close(sock);
	}

	void run_server()
	{
		listen();
		stop_signals.push_back(SIGCHLD);

		while (true)
		{
			int sig_num = accept_loop();
			if (sig_num == SIGCHLD)
			{
				// Wait for children
				while (true)
				{
					int status;
					pid_t pid = waitpid(-1, &status, WNOHANG);
					if (pid == 0)
						break;
					else if (pid < 0)
					{
						if (errno == ECHILD)
							break;
						else
							throw wibble::exception::System("checking for childred that exited");
					}
					cout << "Child " << pid << " ended" << endl;

					map<pid_t, Handler*>::iterator i = children.find(pid);
					if (i != children.end())
					{
						delete i->second;
						children.erase(i);
					}
					cout << children.size() << " running child processes left." << endl;
				}
			} else {
				break;
			}
		}
	}
};

int main(int argc, const char* argv[])
{
	wibble::commandline::Options opts;
	try {
		if (opts.parse(argc, argv))
			return 0;

		runtime::init();

		/*
		runtest = add<StringOption>("runtest", 0, "runtest", "cmd",
				"start the server, run the given test command"
				" and return its exit status");
		accesslog = add<StringOption>("accesslog", 0, "accesslog", "file",
				"file where to log normal access information");
		errorlog = add<StringOption>("errorlog", 0, "errorlog", "file",
				"file where to log errors");
		quiet = add<BoolOption>("quiet", 0, "quiet", "",
			"do not log to standard output");
		*/

		HTTP http;

		const char* host = NULL;
		if (opts.host->isSet())
			host = opts.host->stringValue().c_str();
		const char* port = "8080";
		if (opts.port->isSet())
			port = opts.port->stringValue().c_str();

		http.bind(port, host);

		if (opts.url->isSet())
			http.set_server_name(opts.url->stringValue());
		else
			http.set_server_name("http://" + http.host + ":" + http.port);

		cout << "Listening on " << http.host << ":" << http.port << " for " << http.server_name << endl;

		http.run_server();

		cout << "Done." << endl;

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
