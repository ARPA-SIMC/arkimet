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
#include <arki/dataset/http/test-utils.h>
#include <wibble/net/http.h>
#include <wibble/net/mime.h>

#include <cstring>
#include <cstdlib>
#if 0
#include <arki/metadata.h>
#include <arki/metadata/collection.h>
#include <arki/dataset/local.h>
#include <arki/dataset/ondisk2.h>
#include <arki/dataset/simple/reader.h>
#include <arki/dataset/simple/writer.h>
#include <arki/dataset/simple/index.h>
#include <arki/dispatcher.h>
#include <arki/scan/grib.h>
#include <arki/utils/files.h>
#include <wibble/string.h>
#include <wibble/regexp.h>
#include <wibble/grcal/grcal.h>
#include <wibble/sys/fs.h>
#include <fstream>
#include <strings.h>

using namespace arki;
using namespace arki::types;
using namespace arki::utils;
#endif

using namespace std;
using namespace wibble;

namespace arki {
namespace tests {

FakeRequest::FakeRequest(const std::string& tmpfname)
    : fname(0), fd(-1), response_offset(0)
{
    fname = new char[tmpfname.size() + 1];
    strncpy(fname, tmpfname.c_str(), tmpfname.size() + 1);
    fd = mkstemp(fname);
    if (fd == -1)
        throw wibble::exception::File(fname, "creating temporary file");
    unlink(fname);
}
FakeRequest::~FakeRequest()
{
    if (fd != -1) close(fd);
    if (fname) delete[] fname;
}

void FakeRequest::write(const std::string& str)
{
    size_t pos = 0;
    while (pos < str.size())
    {
        ssize_t res = ::write(fd, str.data() + pos, str.size() - pos);
        if (res < 0)
            throw wibble::exception::File(fname, "writing to file");
        pos += (size_t)res;
    }
    response_offset += str.size();
}

void FakeRequest::write_get(const std::string& query)
{
    arki::tests::FakeRequest r;
    r.write("GET " + query + " HTTP/1.1\r\n");
    r.write("User-Agent: arkimet-fakequery\r\n");
    r.write("Host: localhost:0\r\n");
    r.write("Accept: */*\r\n");
    r.write("\r\n");
}

void FakeRequest::reset()
{
    if (lseek(fd, 0, SEEK_SET) < 0)
        throw wibble::exception::File(fname, "seeking to start of file");
}

void FakeRequest::setup_request(net::http::Request& req)
{
    reset();

    req.sock = fd;
    req.peer_hostname = "localhost";
    req.peer_hostaddr = "127.0.0.1";
    req.peer_port = "0";
    req.server_software = "arkimet-test/0.0";
    req.server_name = "http://localhost:0";
    req.server_port = "0";

    req.read_request();
}

void FakeRequest::read_response()
{
    if (lseek(fd, response_offset, SEEK_SET) < 0)
        throw wibble::exception::File(fname, "seeking to start of response in file");

    // Read headers
    net::mime::Reader reader;
    if (!reader.read_line(fd, response_method))
        throw wibble::exception::Consistency("reading response method", "no headers found");
    if (!reader.read_headers(fd, response_headers))
        throw wibble::exception::Consistency("reading response body", "no body found");

    // Read the rest as reponse body
    while (true)
    {
        char buf[4096];
        ssize_t res = read(fd, buf, 4096);
        if (res < 0)
            throw wibble::exception::File(fname, "reading response body");
        if (res == 0) break;
        response_body.append(buf, res);
    }
}

void FakeRequest::dump_headers(std::ostream& out)
{
    for (map<string, string>::const_iterator i = response_headers.begin();
            i != response_headers.end(); ++i)
        out << "header " << i->first << ": " << i->second << endl;
}

}
}
// vim:set ts=4 sw=4:
