/*
 * utils/http - HTTP server utilities
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

#include <arki/utils/http.h>
#include <wibble/exception.h>
#include <wibble/string.h>
#include <sstream>
#include <ctime>
#include <cerrno>

#include "config.h"

#define SERVER_SOFTWARE PACKAGE_NAME "/" PACKAGE_VERSION

using namespace std;
using namespace wibble;

namespace arki {
namespace utils {
namespace http {

Request::Request()
    : space_splitter("[[:blank:]]+", REG_EXTENDED),
      header_splitter("^([^:[:blank:]]+)[[:blank:]]*:[[:blank:]]*(.+)", 3)
{
}

bool Request::read_request(int sock)
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

bool Request::read_buf(int sock, std::string& res, size_t size)
{
    res.clear();
    res.resize(size);
    size_t pos = 0;
    while (true)
    {
        ssize_t r = read(sock, (void*)((char*)res.data() + pos), size - pos);
        if (r < 0)
            throw wibble::exception::System("reading data from socket");
        if ((size_t)r == size - pos)
            break;
        else if (r == 0)
        {
            res.resize(pos);
            return false;
        }
        pos += r;
    }
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
bool Request::read_line(int sock, string& res)
{
    bool has_cr = false;
    res.clear();
    while (true)
    {
        char c;
        ssize_t count = read(sock, &c, 1);
        if (count == 0) return false;
        if (count < 0)
            throw wibble::exception::System("reading from socket");
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
bool Request::read_method(int sock)
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
bool Request::read_headers(int sock)
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
void Request::set_cgi_env()
{
    // Set CGI server-specific variables

    // SERVER_SOFTWARE — name/version of HTTP server.
    setenv("SERVER_SOFTWARE", SERVER_SOFTWARE, 1);
    // SERVER_NAME — host name of the server, may be dot-decimal IP address.
    setenv("SERVER_NAME", server_name.c_str(), 1);
    // GATEWAY_INTERFACE — CGI/version.
    setenv("GATEWAY_INTERFACE", "CGI/1.1", 1);

    // Set request-specific variables

    // SCRIPT_NAME — relative path to the program, like /cgi-bin/script.cgi.
    setenv("SCRIPT_NAME", script_name.c_str(), 1);
    // PATH_INFO — path suffix, if appended to URL after program name and a slash.
    setenv("PATH_INFO", path_info.c_str(), 1);
    // TODO: is it really needed?
    // PATH_TRANSLATED — corresponding full path as supposed by server, if PATH_INFO is present.
    unsetenv("PATH_TRANSLATED");
    // SERVER_PORT — TCP port (decimal).
    setenv("SERVER_PORT", server_port.c_str(), 1);
    // REMOTE_HOST — host name of the client, unset if server did not perform such lookup.
    setenv("REMOTE_HOST", peer_hostname.c_str(), 1);
    // REMOTE_ADDR — IP address of the client (dot-decimal).
    setenv("REMOTE_ADDR", peer_hostaddr.c_str(), 1);
    // SERVER_PROTOCOL — HTTP/version.
    setenv("SERVER_PROTOCOL", version.c_str(), 1);
    // REQUEST_METHOD — name of HTTP method (see above).
    setenv("REQUEST_METHOD", method.c_str(), 1);
    // QUERY_STRING — the part of URL after ? character. Must be composed of name=value pairs separated with ampersands (such as var1=val1&var2=val2…) and used when form data are transferred via GET method.
    size_t pos = url.find('?');
    if (pos == string::npos)
        setenv("QUERY_STRING", "", 1);
    else
        setenv("QUERY_STRING", url.substr(pos+1).c_str(), 1);
    // AUTH_TYPE — identification type, if applicable.
    unsetenv("AUTH_TYPE");
    // REMOTE_USER used for certain AUTH_TYPEs.
    unsetenv("REMOTE_USER");
    // REMOTE_IDENT — see ident, only if server performed such lookup.
    unsetenv("REMOTE_IDENT");
    // CONTENT_TYPE — MIME type of input data if PUT or POST method are used, as provided via HTTP header.
    map<string, string>::const_iterator i = headers.find("content-type");
    if (i != headers.end())
        setenv("CONTENT_TYPE", i->second.c_str(), 1);
    else
        unsetenv("CONTENT_TYPE");
    // CONTENT_LENGTH — size of input data (decimal, in octets) if provided via HTTP header.
    i = headers.find("content-length");
    if (i != headers.end())
        setenv("CONTENT_LENGTH", i->second.c_str(), 1);
    else
        unsetenv("CONTENT_LENGTH");

    // Variables passed by user agent (HTTP_ACCEPT, HTTP_ACCEPT_LANGUAGE, HTTP_USER_AGENT, HTTP_COOKIE and possibly others) contain values of corresponding HTTP headers and therefore have the same sense.
    for (map<string, string>::const_iterator i = headers.begin();
            i != headers.end(); ++i)
    {
        if (i->first == "content-type" or i->first == "content-length")
            continue;

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

void Request::send(const std::string& buf)
{
    size_t pos = 0;
    while (pos < buf.size())
    {
        ssize_t sent = write(sock, buf.data() + pos, buf.size() - pos);
        if (sent < 0)
            throw wibble::exception::System("writing data to client");
        pos += sent;
    }
}

void Request::send_status_line(int code, const std::string& msg, const std::string& version)
{
    stringstream buf;
    buf << version << " " << code << " " << msg << "\r\n";
	send(buf.str());
}

void Request::send_server_header()
{
    stringstream buf;
    buf << "Server: " << SERVER_SOFTWARE << "\r\n";
	send(buf.str());
}

void Request::send_date_header()
{
    time_t now = time(NULL);
    struct tm t;
    gmtime_r(&now, &t);
    char tbuf[256];
    size_t size = strftime(tbuf, 256, "%a, %d %b %Y %H:%M:%S GMT", &t);
    if (size == 0)
        throw wibble::exception::Consistency("internal buffer too small to store date header");

    stringstream buf;
    buf << "Date: " << tbuf << "\r\n";
    send(buf.str());
}

void Request::send_result(const std::string& content, const std::string& content_type, const std::string& filename)
{
	send_status_line(200, "OK");
	send_date_header();
	send_server_header();
	send("Content-Type: " + content_type + "\r\n");
    if (!filename.empty())
        send("Content-Disposition: attachment; filename=" + filename + "\r\n");
	send(str::fmtf("Content-Length: %d\r\n", content.size()));
	send("\r\n");
	send(content);
}

void Request::discard_input()
{
    // First try seeking at end
    int res = lseek(sock, 0, SEEK_END);
    if (res == -1 && errno == ESPIPE)
    {
        // If it fails, we'll have to read our way
        char buf[4096];
        while (true)
        {
            int res = read(sock, buf, 4096);
            if (res < 0) throw wibble::exception::System("reading data from input socket");
            if (res == 0) break;
        }
    }
}

}
}
}
// vim:set ts=4 sw=4:
