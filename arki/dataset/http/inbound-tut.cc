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
    arki_dataset_http_inbound_shar()
    {
        /*
        cfg.setValue("path", "testds");
        cfg.setValue("name", dsname);
        cfg.setValue("type", "simple");
        cfg.setValue("step", "daily");
        cfg.setValue("postprocess", "testcountbytes");
        */
    }

    // Run the fake request through the server-side handler
    void do_scan(arki::tests::FakeRequest& r)
    {
        net::http::Request req;
        r.setup_request(req);

        dataset::http::InboundParams params;
        params.parse_get_or_post(req);

        ConfigFile import_config;
        dataset::http::InboundServer srv(import_config, "inbound");
        srv.do_scan(params, req);

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


}

// vim:set ts=4 sw=4:
