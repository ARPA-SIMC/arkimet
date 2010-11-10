/**
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
#ifndef ARKI_DATASET_HTTP_TESTUTILS_H
#define ARKI_DATASET_HTTP_TESTUTILS_H

#include <arki/dataset/test-utils.h>
#include <string>
#include <map>

namespace wibble {
namespace net {
namespace http {
struct Request;
}
}
}

namespace arki {
namespace tests {

struct FakeRequest
{
    char* fname;
    int fd;
    size_t response_offset;
    std::string response_method;
    std::map<std::string, std::string> response_headers;
    std::string response_body;

    FakeRequest(const std::string& tmpfname = "fakereq.XXXXXX");
    ~FakeRequest();

    void write(const std::string& str);
    void write_get(const std::string& query);

    void reset();
    void setup_request(wibble::net::http::Request& req);
    void read_response();

    void dump_headers(std::ostream& out);
};

}
}

#endif
