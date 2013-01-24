/*
 * Copyright (C) 2010--2011  Enrico Zini <enrico@enricozini.org>
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

#include "config.h"

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

    // Run the fake request through a server-side summary handler
    void do_config(arki::tests::FakeRequest& r)
    {
        net::http::Request req;
        r.setup_request(req);

        // Handle the request, server side
        auto_ptr<ReadonlyDataset> ds(makeReader());

        dataset::http::ReadonlyDatasetServer srv(*ds, dsname);
        srv.do_config(cfg, req);

        r.read_response();
    }
};
TESTGRP(arki_dataset_http_server);

struct ServerTest : public arki::tests::DatasetTest
{
    string dsname;

    ServerTest(const std::string& dstype) : dsname("testds")
    {
        cfg.setValue("path", "testds");
        cfg.setValue("name", dsname);
        cfg.setValue("type", dstype);
        cfg.setValue("step", "daily");
        cfg.setValue("postprocess", "testcountbytes");

        clean_and_import();
    }

    // Run the fake request through a server-side summary handler
    void do_summary(LOCPRM, arki::tests::FakeRequest& r)
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
    void do_query(LOCPRM, arki::tests::FakeRequest& r)
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
    void do_queryData(LOCPRM, arki::tests::FakeRequest& r)
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
    void do_queryBytes(LOCPRM, arki::tests::FakeRequest& r)
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

    // Run the fake request through a server-side summary handler
    void do_config(LOCPRM, arki::tests::FakeRequest& r)
    {
        net::http::Request req;
        r.setup_request(req);

        // Handle the request, server side
        auto_ptr<ReadonlyDataset> ds(makeReader());

        dataset::http::ReadonlyDatasetServer srv(*ds, dsname);
        srv.do_config(cfg, req);

        r.read_response();
    }

    // Test /summary/
    void test_summary(LOCPRM)
    {
        // Make the request
        arki::tests::FakeRequest r;
        r.write_get("/foo");

        // Handle the request, server side
        iftest(do_summary, r);

        // Handle the response, client side
        iatest(equals, "HTTP/1.0 200 OK", r.response_method);
        iatest(equals, "application/octet-stream", r.response_headers["content-type"]);
        iatest(equals, "attachment; filename=testds-summary.bin", r.response_headers["content-disposition"]);

        Summary s;
        stringstream sstream(r.response_body);
        s.read(sstream, "response body");
        iatest(equals, 3u, s.count());
    }

    // Test /query/
    void test_query(LOCPRM)
    {
        // Make the request
        arki::tests::FakeRequest r;
        r.write_get("/foo");

        // Handle the request, server side
        iftest(do_query, r);

        // Handle the response, client side
        iatest(equals, "HTTP/1.0 200 OK", r.response_method);
        iatest(equals, "application/octet-stream", r.response_headers["content-type"]);
        iatest(equals, "attachment; filename=testds.bin", r.response_headers["content-disposition"]);

        stringstream sstream(r.response_body);
        metadata::Collection mdc;
        Metadata::readFile(sstream, metadata::ReadContext("", "(response body)"), mdc);

        iatest(equals, 3u, mdc.size());
    }

    // Test /querydata/
    void test_querydata(LOCPRM)
    {
        // Make the request
        arki::tests::FakeRequest r;
        r.write_get("/foo");

        // Handle the request, server side
        iftest(do_queryData, r);

        // Handle the response, client side
        iatest(equals, "HTTP/1.0 200 OK", r.response_method);
        iatest(equals, "application/octet-stream", r.response_headers["content-type"]);
        iatest(equals, "attachment; filename=testds.arkimet", r.response_headers["content-disposition"]);

        stringstream sstream(r.response_body);
        metadata::Collection mdc;
        Metadata::readFile(sstream, metadata::ReadContext("", "(response body)"), mdc);

        iatest(equals, 3u, mdc.size());
    }

    // Test /querybytes/
    void test_querybytes(LOCPRM)
    {
        // Make the request
        arki::tests::FakeRequest r;
        r.write_get("/foo");

        // Handle the request, server side
        iftest(do_queryBytes, r);

        // Handle the response, client side
        iatest(equals, "HTTP/1.0 200 OK", r.response_method);
        iatest(equals, "application/octet-stream", r.response_headers["content-type"]);
        iatest(equals, "attachment; filename=testds.txt", r.response_headers["content-disposition"]);

        iatest(equals, 44412u, r.response_body.size());
        iatest(equals, "GRIB", r.response_body.substr(0, 4));
    }

    // Test /config/
    void test_config(LOCPRM)
    {
        // Make the request
        arki::tests::FakeRequest r;
        r.write_get("/foo");

        // Handle the request, server side
        iftest(do_config, r);

        // Handle the response, client side
        iatest(equals, "HTTP/1.0 200 OK", r.response_method);
        iatest(equals, "text/plain", r.response_headers["content-type"]);
        iatest(equals, "", r.response_headers["content-disposition"]);

        stringstream buf;
        buf << "[testds]" << endl;
        cfg.output(buf, "memory");
        iatest(equals, buf.str(), r.response_body);
    }

    // Test /config/ with a locked DB
    void test_configlocked(LOCPRM)
    {
        auto_ptr<WritableDataset> ds(makeWriter());
        Pending p = ds->test_writelock();

        iftest(test_config);
    }


    void test_all(LOCPRM)
    {
        iftest(test_summary);
        iftest(test_query);
        iftest(test_querydata);
        iftest(test_querybytes);
        iftest(test_config);
        iftest(test_configlocked);
    }
};

// Query a simple dataset, with plain manifest
template<> template<>
void to::test<1>()
{
    ServerTest test("simple");
    ftest(test.test_all);
}

// Query a simple dataset, with sqlite manifest
template<> template<>
void to::test<2>()
{
    arki::tests::ForceSqlite fs;
    ServerTest test("simple");
    ftest(test.test_all);
}

// Query an ondisk2 dataset, with sqlite manifest
template<> template<>
void to::test<3>()
{
    ServerTest test("ondisk2");
    ftest(test.test_all);
}

}

// vim:set ts=4 sw=4:
