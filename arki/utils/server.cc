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

#include <arki/utils/server.h>
#include <wibble/exception.h>
#include <wibble/string.h>

#include <sys/types.h>
#include <sys/socket.h>
#include <netdb.h>
#include <unistd.h>
#include <fcntl.h>
#include <signal.h>

#include <cstring>
#include <cerrno>

#include "config.h"

using namespace std;
using namespace wibble;

namespace arki {
namespace utils {
namespace net {

Server::Server() : socktype(SOCK_STREAM), sock(-1), old_signal_actions(0), signal_actions(0)
{
    // By default, stop on sigterm and sigint
    stop_signals.push_back(SIGTERM);
    stop_signals.push_back(SIGINT);
}

Server::~Server()
{
    if (sock >= 0) close(sock);
    if (old_signal_actions) delete[] old_signal_actions;
    if (signal_actions) delete[] signal_actions;
}

void Server::bind(const char* port, const char* host)
{
    struct addrinfo hints;
    memset(&hints, 0, sizeof(addrinfo));
    hints.ai_family = AF_UNSPEC;
    hints.ai_socktype = socktype;
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
void Server::listen(int backlog)
{
    if (::listen(sock, backlog) < 0)
        throw wibble::exception::System("listening on port " + port);
}


TCPServer::~TCPServer() {}

void TCPServer::noop_signal_handler(int sig) {}

void TCPServer::signal_setup()
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

void TCPServer::signal_install()
{
    for (size_t i = 0; i < stop_signals.size(); ++i)
        if (sigaction(stop_signals[i], &signal_actions[i], &old_signal_actions[i]) < 0)
            throw wibble::exception::System("installing handler for signal " + str::fmt(stop_signals[i]));
}

void TCPServer::signal_uninstall()
{
    for (size_t i = 0; i < stop_signals.size(); ++i)
        if (sigaction(stop_signals[i], &old_signal_actions[i], NULL) < 0)
            throw wibble::exception::System("restoring handler for signal " + str::fmt(stop_signals[i]));
}

// Loop accepting connections on the socket, until interrupted by a
// signal in stop_signals
void TCPServer::accept_loop()
{
    struct SignalInstaller {
        TCPServer& s;
        SignalInstaller(TCPServer& s) : s(s) { s.signal_install(); }
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

}
}
}
// vim:set ts=4 sw=4:
