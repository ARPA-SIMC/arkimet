#ifndef ARKI_UTILS_HTTP_H
#define ARKI_UTILS_HTTP_H

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

#include <string>
#include <map>
#include <wibble/regexp.h>
#include <iosfwd>

namespace arki {
namespace utils {

namespace mime {

struct Reader
{
	wibble::ERegexp header_splitter;

	Reader();

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
	bool read_line(int sock, std::string& res);

	/**
	 * Read MIME headers
	 *
	 * @return true if there still data to read and headers are terminated
	 * by an empty line, false if headers are terminated by EOF
	 */
	bool read_headers(int sock, std::map<std::string, std::string>& headers);

	/**
	 * Read until boundary is found, sending data to the given ostream
	 *
	 * @param max Maximum number of bytes to read, or 0 for unilimited until boundary
	 *
	 * @returns true if another MIME part follows, false if it is the last part
	 * (boundary has trailing --)
	 */
	bool read_until_boundary(int sock, const std::string& boundary, std::ostream& out, size_t max=0);

	/**
	 * Read until boundary is found, sending data to the given ostream
	 *
	 * @returns true if another MIME part follows, false if it is the last part
	 * (boundary has trailing --)
	 */
	bool discard_until_boundary(int sock, const std::string& boundary);

	/**
	 * Skip until the end of the boundary line
	 *
	 * @return true if the boundary does not end with --, else false
	 */
	bool readboundarytail(int sock);
};

}

namespace http {

struct Request;

struct error
{
	int code;
	std::string desc;
	std::string msg;

	error(int code, const std::string& desc)
		: code(code), desc(desc) {}
	error(int code, const std::string& desc, const std::string& msg)
		: code(code), desc(desc), msg(msg) {}

	virtual void send(Request& req);

};

struct error400 : public error
{
	error400() : error(400, "Bad request") {}
	error400(const std::string& msg) : error(400, "Bad request", msg) {}
};

struct error404 : public error
{
	error404() : error(404, "Not found") {}
	error404(const std::string& msg) : error(404, "Not found", msg) {}

	virtual void send(Request& req);
};

struct Request
{
    // Request does not take ownership of the socket: it is up to the caller to
    // close it
    int sock;
    std::string peer_hostname;
    std::string peer_hostaddr;
    std::string peer_port;
    std::string server_name;
    std::string server_port;
    std::string script_name;
    std::string path_info;

    std::string method;
	std::string url;
	std::string version;
    std::map<std::string, std::string> headers;
    wibble::Splitter space_splitter;

	mime::Reader mime_reader;

	Request();

    /**
     * Read request method and headers from sock
     *
     * Sock will be positioned at the beginning of the request body, after the
     * empty line that follows the header.
     *
     * @returns true if the request has been read, false if EOF was found
     * before the end of the headers.
     */
	bool read_request(int sock);

    /**
     * Read a fixed amount of data from the file descriptor
     *
     * @returns true if all the data were read, false if EOF was encountered
     * before the end of the buffer
     */
    bool read_buf(int sock, std::string& res, size_t size);

	// Read HTTP method and its following empty line
	bool read_method(int sock);

	/**
	 * Read HTTP headers
	 *
	 * @return true if there still data to read and headers are terminated
	 * by an empty line, false if headers are terminated by EOF
	 */
	bool read_headers(int sock);

	// Set the CGI environment variables for the current process using this
	// request
	void set_cgi_env();

    // Send the content of buf, verbatim, to the client
    void send(const std::string& buf);

    // Send the HTTP status line
    void send_status_line(int code, const std::string& msg, const std::string& version = "HTTP/1.0");

    // Send the HTTP server header
    void send_server_header();

    // Send the HTTP date header
    void send_date_header();

    // Send a string as result
    void send_result(const std::string& content, const std::string& content_type="text/html; charset=utf-8", const std::string& filename=std::string());

    // Discard all input from the socket
    void discard_input();
};

}
}
}

// vim:set ts=4 sw=4:
#endif
