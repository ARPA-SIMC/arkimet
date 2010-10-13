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

namespace arki {
namespace utils {
namespace http {

struct Request
{
    std::string method;
	std::string url;
	std::string version;
    std::map<std::string, std::string> headers;
    wibble::Splitter space_splitter;
	wibble::ERegexp header_splitter;

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
};

}
}
}

// vim:set ts=4 sw=4:
#endif
