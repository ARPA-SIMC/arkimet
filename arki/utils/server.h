#ifndef ARKI_UTILS_SERVER_H
#define ARKI_UTILS_SERVER_H

/*
 * utils/server - Network server infrastructure
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

#include <string>
#include <vector>
#include <signal.h>

namespace arki {
namespace utils {
namespace net {

/**
 * Generic bind/listen/accept internet server
 */
struct Server
{
	// Human readable server hostname
	std::string host;
	// Human readable server port
	std::string port;
	// Type of server socket (default SOCK_STREAM)
	int socktype;
	// Server socket
	int sock;
	// Signals used to stop the server's accept loop
	std::vector<int> stop_signals;

	// Saved signal handlers before accept
	struct sigaction *old_signal_actions;
	// Signal handlers in use during accept
	struct sigaction *signal_actions;

	Server();
	~Server();

	// Bind to a given port (and optionally interface hostname)
	void bind(const char* port, const char* host=NULL);

	// Set socket to listen, with given backlog
	void listen(int backlog = 16);
};

struct TCPServer : public Server
{
	virtual ~TCPServer();

	// Loop accepting connections on the socket, until interrupted by a
	// signal in stop_signals
	void accept_loop();

	virtual void handle_client(int sock, const std::string& peer_hostname, const std::string& peer_hostaddr, const std::string& peer_port) = 0;

protected:
	static void noop_signal_handler(int sig);

	// Initialize signal handling structures
	void signal_setup();
	void signal_install();
	void signal_uninstall();
};

}
}
}

// vim:set ts=4 sw=4:
#endif
