/*
 * Copyright (C) 2010  Enrico Zini <enrico@enricozini.org>
 *
 * This library is free software; you can redistribute it and/or
 * modify it under the terms of the GNU Lesser General Public
 * License as published by the Free Software Foundation; either
 * version 2.1 of the License, or (at your option) any later version.
 *
 * This library is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the GNU
 * Lesser General Public License for more details.
 *
 * You should have received a copy of the GNU Lesser General Public
 * License along with this library; if not, write to the Free Software
 * Foundation, Inc., 59 Temple Place, Suite 330, Boston, MA 02111-1307  USA
 */

#include <arki/dataset/http/test-utils.h>
#include <arki/dataset/http/inbound.h>
#include <arki/metadata/collection.h>

#include <sstream>
#include <iostream>

namespace tut {
using namespace std;
using namespace arki;
using namespace wibble;

struct arki_dataset_http_inbound_shar : public arki::tests::DatasetTest {
    ConfigFile import_config;

    arki_dataset_http_inbound_shar()
    {
        stringstream incfg;
        incfg << "[testds]" << endl;
        incfg << "path = testds" << endl;
        incfg << "name = testds" << endl;
        incfg << "type = simple" << endl;
        incfg << "step = daily" << endl;
        incfg << "filter = origin:GRIB1" << endl;
        incfg << "remote import = yes" << endl;
        incfg << endl;
        incfg << "[error]" << endl;
        incfg << "path = error" << endl;
        incfg << "name = error" << endl;
        incfg << "type = simple" << endl;
        incfg << "step = daily" << endl;
        incfg << "remote import = yes" << endl;
        incfg.seekg(0);
        import_config.parse(incfg, "memory");
    }

    // Run the fake request through the server-side handler
    void do_scan(arki::tests::FakeRequest& r)
    {
        net::http::Request req;
        r.setup_request(req);

        dataset::http::InboundParams params;
        params.parse_get_or_post(req);

        dataset::http::InboundServer srv(import_config, "inbound");
        srv.do_scan(params, req);

        r.read_response();
    }

    // Run the fake request through the server-side handler
    void do_testdispatch(arki::tests::FakeRequest& r)
    {
        net::http::Request req;
        r.setup_request(req);

        dataset::http::InboundParams params;
        params.parse_get_or_post(req);

        dataset::http::InboundServer srv(import_config, "inbound");
        srv.do_testdispatch(params, req);

        r.read_response();
    }
};
TESTGRP(arki_dataset_http_inbound);

// Test /inbound/scan/
template<> template<>
void to::test<1>()
{
    // Make the request
    arki::tests::FakeRequest r;
    r.write_get("/inbound/scan?file=test.grib1");

    // Handle the request, server side
    do_scan(r);

    // Handle the response, client side
    ensure_equals(r.response_method, "HTTP/1.0 200 OK");
    ensure_equals(r.response_headers["content-type"], "application/octet-stream");
    ensure_equals(r.response_headers["content-disposition"], "attachment; filename=test.grib1.arkimet");

    stringstream in(r.response_body);
    metadata::Collection mdc;
    Metadata::readFile(in, "response body", mdc);
    ensure_equals(mdc.size(), 3u);
}

// Test /inbound/testdispatch/
template<> template<>
void to::test<2>()
{
    // Make the request
    arki::tests::FakeRequest r;
    r.write_get("/inbound/testdispatch?file=test.grib1");

    // Handle the request, server side
    do_testdispatch(r);

    // Handle the response, client side
    ensure_equals(r.response_method, "HTTP/1.0 200 OK");
    ensure_equals(r.response_headers["content-type"], "text/plain");
    ensure_equals(r.response_headers["content-disposition"], "attachment; filename=test.grib1.log");

    ensure_contains(r.response_body, "test.grib1:0+7218): acquire to testds dataset");
    ensure_contains(r.response_body, "test.grib1:7218+34960): acquire to testds dataset");
    ensure_contains(r.response_body, "test.grib1:42178+2234): acquire to testds dataset");
}

}

// vim:set ts=4 sw=4:
