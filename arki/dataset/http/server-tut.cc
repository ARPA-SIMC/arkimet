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
#include <arki/dataset/http/server.h>
#include <arki/summary.h>
#include <arki/metadata/collection.h>

#include <sstream>
#include <iostream>

namespace tut {
using namespace std;
using namespace arki;
using namespace wibble;

struct arki_dataset_http_server_shar : public arki::tests::DatasetTest {
    string dsname;

    arki_dataset_http_server_shar() : dsname("testds")
    {
        cfg.setValue("path", "testds");
        cfg.setValue("name", dsname);
        cfg.setValue("type", "simple");
        cfg.setValue("step", "daily");
        cfg.setValue("postprocess", "testcountbytes");

        clean_and_import();
    }

    // Run the fake request through a server-side summary handler
    void do_summary(arki::tests::FakeRequest& r)
    {
        net::http::Request req;
        r.setup_request(req);

        // Handle the request, server side
        auto_ptr<ReadonlyDataset> ds(makeReader());
        dataset::http::LegacySummaryParams params;
        params.parse_get_or_post(req);

        dataset::http::ReadonlyDatasetServer srv(*ds, dsname);
        srv.do_summary(params, req);

        r.read_response();
    }

    // Run the fake request through a server-side summary handler
    void do_query(arki::tests::FakeRequest& r)
    {
        net::http::Request req;
        r.setup_request(req);

        // Handle the request, server side
        auto_ptr<ReadonlyDataset> ds(makeReader());

        dataset::http::LegacyQueryParams params(".");
        params.parse_get_or_post(req);

        dataset::http::ReadonlyDatasetServer srv(*ds, dsname);
        srv.do_query(params, req);

        r.read_response();
    }

    // Run the fake request through a server-side summary handler
    void do_queryData(arki::tests::FakeRequest& r)
    {
        net::http::Request req;
        r.setup_request(req);

        // Handle the request, server side
        auto_ptr<ReadonlyDataset> ds(makeReader());

        dataset::http::QueryDataParams params;
        params.parse_get_or_post(req);

        dataset::http::ReadonlyDatasetServer srv(*ds, dsname);
        srv.do_queryData(params, req);

        r.read_response();
    }

    // Run the fake request through a server-side summary handler
    void do_queryBytes(arki::tests::FakeRequest& r)
    {
        net::http::Request req;
        r.setup_request(req);

        // Handle the request, server side
        auto_ptr<ReadonlyDataset> ds(makeReader());

        dataset::http::QueryBytesParams params(".");
        params.parse_get_or_post(req);

        dataset::http::ReadonlyDatasetServer srv(*ds, dsname);
        srv.do_queryBytes(params, req);

        r.read_response();
    }
};
TESTGRP(arki_dataset_http_server);

// Test /summary/
template<> template<>
void to::test<1>()
{
    // Make the request
    arki::tests::FakeRequest r;
    r.write_get("/foo");

    // Handle the request, server side
    do_summary(r);

    // Handle the response, client side
    ensure_equals(r.response_method, "HTTP/1.0 200 OK");
    ensure_equals(r.response_headers["content-type"], "application/octet-stream");
    ensure_equals(r.response_headers["content-disposition"], "attachment; filename=testds-summary.bin");

    Summary s;
    stringstream sstream(r.response_body);
    s.read(sstream, "response body");
    ensure_equals(s.count(), 3u);
}

// Test /query/
template<> template<>
void to::test<2>()
{
    // Make the request
    arki::tests::FakeRequest r;
    r.write_get("/foo");

    // Handle the request, server side
    do_query(r);

    // Handle the response, client side
    ensure_equals(r.response_method, "HTTP/1.0 200 OK");
    ensure_equals(r.response_headers["content-type"], "application/octet-stream");
    ensure_equals(r.response_headers["content-disposition"], "attachment; filename=testds.bin");

    stringstream sstream(r.response_body);
    metadata::Collection mdc;
    Metadata::readFile(sstream, "response body", mdc);

    ensure_equals(mdc.size(), 3u);
}

// Test /querydata/
template<> template<>
void to::test<3>()
{
    // Make the request
    arki::tests::FakeRequest r;
    r.write_get("/foo");

    // Handle the request, server side
    do_queryData(r);

    // Handle the response, client side
    ensure_equals(r.response_method, "HTTP/1.0 200 OK");
    ensure_equals(r.response_headers["content-type"], "application/octet-stream");
    ensure_equals(r.response_headers["content-disposition"], "attachment; filename=testds.arkimet");

    stringstream sstream(r.response_body);
    metadata::Collection mdc;
    Metadata::readFile(sstream, "response body", mdc);

    ensure_equals(mdc.size(), 3u);
}

// Test /querybytes/
template<> template<>
void to::test<4>()
{
    // Make the request
    arki::tests::FakeRequest r;
    r.write_get("/foo");

    // Handle the request, server side
    do_queryBytes(r);

    // Handle the response, client side
    ensure_equals(r.response_method, "HTTP/1.0 200 OK");
    ensure_equals(r.response_headers["content-type"], "application/octet-stream");
    ensure_equals(r.response_headers["content-disposition"], "attachment; filename=testds.txt");

    ensure_equals(r.response_body.size(), 44412);
    ensure_equals(r.response_body.substr(0, 4), "GRIB");
}


}

// vim:set ts=4 sw=4:
