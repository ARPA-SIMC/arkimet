#include "server.h"
#include "arki/exceptions.h"
#include "arki/utils/string.h"
#include <sys/types.h>
#include <sys/socket.h>
#include <netdb.h>
#include <unistd.h>
#include <fcntl.h>
#include <signal.h>
#include <cstring>
#include <cerrno>

using namespace std;

namespace arki {
namespace utils {
namespace net {

Server::Server() : socktype(SOCK_STREAM), sock(-1), old_signal_actions(0), signal_actions(0)
{
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
    {
        stringstream ss;
        ss << "cannot resolve hostname " << host << ":" << port << ": " << gai_strerror(gaires);
        throw std::runtime_error(ss.str());
    }

    for (addrinfo* rp = res; rp != NULL; rp = rp->ai_next)
    {
        int sfd = socket(rp->ai_family, rp->ai_socktype, rp->ai_protocol);
        if (sfd == -1)
            continue;

        // Set SO_REUSEADDR 
        int flag = 1;
        if (setsockopt(sfd, SOL_SOCKET, SO_REUSEADDR, &flag, sizeof(int)) < 0)
            throw_system_error("setting SO_REUSEADDR on socket");

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
    {
        // No address succeeded
        stringstream ss;
        ss << "cannot bind to " << host << ":" << port << ": could not bind to any of the resolved addresses";
        throw std::runtime_error(ss.str());
    }
}

// Set socket to listen, with given backlog
void Server::listen(int backlog)
{
    if (::listen(sock, backlog) < 0)
        throw_system_error("listening on port " + port);
}

void Server::set_sock_cloexec()
{
    // Set close-on-exec on master socket
    if (fcntl(sock, F_SETFD, FD_CLOEXEC) < 0)
        throw_system_error("setting FD_CLOEXEC on server socket");
}


TCPServer::TCPServer()
{
    // By default, stop on sigterm and sigint
    stop_signals.push_back(SIGTERM);
    stop_signals.push_back(SIGINT);
}
TCPServer::~TCPServer() {}

void TCPServer::signal_handler(int sig) { last_signal = sig; }
int TCPServer::last_signal = -1;

void TCPServer::signal_setup()
{
    if (old_signal_actions) delete[] old_signal_actions;
    if (signal_actions) delete[] signal_actions;
    old_signal_actions = new struct sigaction[stop_signals.size()];
    signal_actions = new struct sigaction[stop_signals.size()];
    for (size_t i = 0; i < stop_signals.size(); ++i)
    {
        signal_actions[i].sa_handler = signal_handler;
        sigemptyset(&(signal_actions[i].sa_mask));
        signal_actions[i].sa_flags = 0;
    }
}

void TCPServer::signal_install()
{
    for (size_t i = 0; i < stop_signals.size(); ++i)
        if (sigaction(stop_signals[i], &signal_actions[i], &old_signal_actions[i]) < 0)
        {
            stringstream ss;
            ss << "cannot install signal handler for signal " << stop_signals[i];
            throw std::system_error(errno, std::system_category(), ss.str());
        }
    TCPServer::last_signal = -1;
}

void TCPServer::signal_uninstall()
{
    for (size_t i = 0; i < stop_signals.size(); ++i)
        if (sigaction(stop_signals[i], &old_signal_actions[i], NULL) < 0)
        {
            stringstream ss;
            ss << "cannot restore signal handler for signal " << stop_signals[i];
            throw std::system_error(errno, std::system_category(), ss.str());
        }
}

// Loop accepting connections on the socket, until interrupted by a
// signal in stop_signals
int TCPServer::accept_loop()
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
            fd = accept(sock, reinterpret_cast<sockaddr*>(&peer_addr), static_cast<socklen_t*>(&peer_addr_len));
            if (fd == -1)
            {
                if (errno == EINTR)
                    return TCPServer::last_signal;
                throw_system_error("listening on " + host + ":" + port);
            }
        }

        // Resolve the peer
        char hbuf[NI_MAXHOST], sbuf[NI_MAXSERV];
        int gaires = getnameinfo(reinterpret_cast<struct sockaddr *>(&peer_addr),
                peer_addr_len,
                hbuf, NI_MAXHOST,
                sbuf, NI_MAXSERV,
                NI_NUMERICSERV);
        if (gaires == 0)
        {
            string hostname = hbuf;
            gaires = getnameinfo(reinterpret_cast<struct sockaddr *>(&peer_addr),
                    peer_addr_len,
                    hbuf, NI_MAXHOST,
                    sbuf, NI_MAXSERV,
                    NI_NUMERICHOST | NI_NUMERICSERV);
            if (gaires == 0)
                handle_client(fd, hostname, hbuf, sbuf);
            else
            {
                string msg = "cannot resolve peer name numerically: ";
                msg += gai_strerror(gaires);
                throw std::runtime_error(msg);
            }
        }
        else
        {
            string msg = "cannot resolve peer name: ";
            msg += gai_strerror(gaires);
            throw std::runtime_error(msg);
        }
    }
}

}
}
}
