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
using namespace arki::utils;
using namespace wibble::tests;

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
        unique_ptr<ReadonlyDataset> ds(makeReader());
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
        unique_ptr<ReadonlyDataset> ds(makeReader());

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
        unique_ptr<ReadonlyDataset> ds(makeReader());

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
        unique_ptr<ReadonlyDataset> ds(makeReader());

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
        unique_ptr<ReadonlyDataset> ds(makeReader());

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
    void do_summary(WIBBLE_TEST_LOCPRM, arki::tests::FakeRequest& r)
    {
        net::http::Request req;
        r.setup_request(req);

        // Handle the request, server side
        unique_ptr<ReadonlyDataset> ds(makeReader());
        dataset::http::LegacySummaryParams params;
        params.parse_get_or_post(req);

        dataset::http::ReadonlyDatasetServer srv(*ds, dsname);
        srv.do_summary(params, req);

        r.read_response();
    }

    // Run the fake request through a server-side summary handler
    void do_query(WIBBLE_TEST_LOCPRM, arki::tests::FakeRequest& r)
    {
        net::http::Request req;
        r.setup_request(req);

        // Handle the request, server side
        unique_ptr<ReadonlyDataset> ds(makeReader());

        dataset::http::LegacyQueryParams params(".");
        params.parse_get_or_post(req);

        dataset::http::ReadonlyDatasetServer srv(*ds, dsname);
        srv.do_query(params, req);

        r.read_response();
    }

    // Run the fake request through a server-side summary handler
    void do_queryData(WIBBLE_TEST_LOCPRM, arki::tests::FakeRequest& r)
    {
        net::http::Request req;
        r.setup_request(req);

        // Handle the request, server side
        unique_ptr<ReadonlyDataset> ds(makeReader());

        dataset::http::QueryDataParams params;
        params.parse_get_or_post(req);

        dataset::http::ReadonlyDatasetServer srv(*ds, dsname);
        srv.do_queryData(params, req);

        r.read_response();
    }

    // Run the fake request through a server-side summary handler
    void do_queryBytes(WIBBLE_TEST_LOCPRM, arki::tests::FakeRequest& r)
    {
        net::http::Request req;
        r.setup_request(req);

        // Handle the request, server side
        unique_ptr<ReadonlyDataset> ds(makeReader());

        dataset::http::QueryBytesParams params(".");
        params.parse_get_or_post(req);

        dataset::http::ReadonlyDatasetServer srv(*ds, dsname);
        srv.do_queryBytes(params, req);

        r.read_response();
    }

    // Run the fake request through a server-side summary handler
    void do_config(WIBBLE_TEST_LOCPRM, arki::tests::FakeRequest& r)
    {
        net::http::Request req;
        r.setup_request(req);

        // Handle the request, server side
        unique_ptr<ReadonlyDataset> ds(makeReader());

        dataset::http::ReadonlyDatasetServer srv(*ds, dsname);
        srv.do_config(cfg, req);

        r.read_response();
    }

    // Test /summary/
    void test_summary(WIBBLE_TEST_LOCPRM)
    {
        // Make the request
        arki::tests::FakeRequest r;
        r.write_get("/foo");

        // Handle the request, server side
        wruntest(do_summary, r);

        // Handle the response, client side
        wassert(actual(r.response_method) == "HTTP/1.0 200 OK");
        wassert(actual(r.response_headers["content-type"]) == "application/octet-stream");
        wassert(actual(r.response_headers["content-disposition"]) == "attachment; filename=testds-summary.bin");

        Summary s;
        stringstream sstream(r.response_body);
        s.read(sstream, "response body");
        wassert(actual(s.count()) == 3u);
    }

    // Test /query/
    void test_query(WIBBLE_TEST_LOCPRM)
    {
        // Make the request
        arki::tests::FakeRequest r;
        r.write_get("/foo");

        // Handle the request, server side
        wruntest(do_query, r);

        // Handle the response, client side
        wassert(actual(r.response_method) == "HTTP/1.0 200 OK");
        wassert(actual(r.response_headers["content-type"]) == "application/octet-stream");
        wassert(actual(r.response_headers["content-disposition"]) == "attachment; filename=testds.bin");

        stringstream sstream(r.response_body);
        metadata::Collection mdc;
        Metadata::readFile(sstream, metadata::ReadContext("", "(response body)"), mdc);

        wassert(actual(mdc.size()) == 3u);
    }

    // Test /querydata/
    void test_querydata(WIBBLE_TEST_LOCPRM)
    {
        // Make the request
        arki::tests::FakeRequest r;
        r.write_get("/foo");

        // Handle the request, server side
        wruntest(do_queryData, r);

        // Handle the response, client side
        wassert(actual(r.response_method) == "HTTP/1.0 200 OK");
        wassert(actual(r.response_headers["content-type"]) == "application/octet-stream");
        wassert(actual(r.response_headers["content-disposition"]) == "attachment; filename=testds.arkimet");

        stringstream sstream(r.response_body);
        metadata::Collection mdc;
        Metadata::readFile(sstream, metadata::ReadContext("", "(response body)"), mdc);

        wassert(actual(mdc.size()) == 3u);
    }

    // Test /querybytes/
    void test_querybytes(WIBBLE_TEST_LOCPRM)
    {
        // Make the request
        arki::tests::FakeRequest r;
        r.write_get("/foo");

        // Handle the request, server side
        wruntest(do_queryBytes, r);

        // Handle the response, client side
        wassert(actual(r.response_method) == "HTTP/1.0 200 OK");
        wassert(actual(r.response_headers["content-type"]) == "application/octet-stream");
        wassert(actual(r.response_headers["content-disposition"]) == "attachment; filename=testds.txt");

        wassert(actual(r.response_body.size()) == 44412u);
        wassert(actual(r.response_body.substr(0, 4)) == "GRIB");
    }

    // Test /config/
    void test_config(WIBBLE_TEST_LOCPRM)
    {
        // Make the request
        arki::tests::FakeRequest r;
        r.write_get("/foo");

        // Handle the request, server side
        wruntest(do_config, r);

        // Handle the response, client side
        wassert(actual(r.response_method) == "HTTP/1.0 200 OK");
        wassert(actual(r.response_headers["content-type"]) == "text/plain");
        wassert(actual(r.response_headers["content-disposition"]) == "");

        stringstream buf;
        buf << "[testds]" << endl;
        cfg.output(buf, "memory");
        wassert(actual(r.response_body) == buf.str());
    }

    // Test /config/ with a locked DB
    void test_configlocked(WIBBLE_TEST_LOCPRM)
    {
        unique_ptr<WritableDataset> ds(makeWriter());
        Pending p = ds->test_writelock();

        wruntest(test_config);
    }


    void test_all(WIBBLE_TEST_LOCPRM)
    {
        wruntest(test_summary);
        wruntest(test_query);
        wruntest(test_querydata);
        wruntest(test_querybytes);
        wruntest(test_config);
        wruntest(test_configlocked);
    }
};

// Query a simple dataset, with plain manifest
template<> template<>
void to::test<1>()
{
    ServerTest test("simple");
    wruntest(test.test_all);
}

// Query a simple dataset, with sqlite manifest
template<> template<>
void to::test<2>()
{
    arki::tests::ForceSqlite fs;
    ServerTest test("simple");
    wruntest(test.test_all);
}

// Query an ondisk2 dataset, with sqlite manifest
template<> template<>
void to::test<3>()
{
    ServerTest test("ondisk2");
    wruntest(test.test_all);
}

}

// vim:set ts=4 sw=4:
