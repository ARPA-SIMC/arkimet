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
using namespace arki::tests;

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
        unique_ptr<Reader> ds(makeReader());
        dataset::http::LegacySummaryParams params;
        params.parse_get_or_post(req);

        dataset::http::ReaderServer srv(*ds, dsname);
        srv.do_summary(params, req);

        r.read_response();
    }

    // Run the fake request through a server-side summary handler
    void do_query(arki::tests::FakeRequest& r)
    {
        net::http::Request req;
        r.setup_request(req);

        // Handle the request, server side
        unique_ptr<Reader> ds(makeReader());

        dataset::http::LegacyQueryParams params(".");
        params.parse_get_or_post(req);

        dataset::http::ReaderServer srv(*ds, dsname);
        srv.do_query(params, req);

        r.read_response();
    }

    // Run the fake request through a server-side summary handler
    void do_queryData(arki::tests::FakeRequest& r)
    {
        net::http::Request req;
        r.setup_request(req);

        // Handle the request, server side
        unique_ptr<Reader> ds(makeReader());

        dataset::http::QueryDataParams params;
        params.parse_get_or_post(req);

        dataset::http::ReaderServer srv(*ds, dsname);
        srv.do_queryData(params, req);

        r.read_response();
    }

    // Run the fake request through a server-side summary handler
    void do_queryBytes(arki::tests::FakeRequest& r)
    {
        net::http::Request req;
        r.setup_request(req);

        // Handle the request, server side
        unique_ptr<Reader> ds(makeReader());

        dataset::http::QueryBytesParams params(".");
        params.parse_get_or_post(req);

        dataset::http::ReaderServer srv(*ds, dsname);
        srv.do_queryBytes(params, req);

        r.read_response();
    }

    // Run the fake request through a server-side summary handler
    void do_config(arki::tests::FakeRequest& r)
    {
        net::http::Request req;
        r.setup_request(req);

        // Handle the request, server side
        unique_ptr<Reader> ds(makeReader());

        dataset::http::ReaderServer srv(*ds, dsname);
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
    void do_summary(arki::tests::FakeRequest& r)
    {
        net::http::Request req;
        r.setup_request(req);

        // Handle the request, server side
        unique_ptr<Reader> ds(makeReader());
        dataset::http::LegacySummaryParams params;
        params.parse_get_or_post(req);

        dataset::http::ReaderServer srv(*ds, dsname);
        srv.do_summary(params, req);

        r.read_response();
    }

    // Run the fake request through a server-side summary handler
    void do_query(arki::tests::FakeRequest& r)
    {
        net::http::Request req;
        r.setup_request(req);

        // Handle the request, server side
        unique_ptr<Reader> ds(makeReader());

        dataset::http::LegacyQueryParams params(".");
        params.parse_get_or_post(req);

        dataset::http::ReaderServer srv(*ds, dsname);
        srv.do_query(params, req);

        r.read_response();
    }

    // Run the fake request through a server-side summary handler
    void do_queryData(arki::tests::FakeRequest& r)
    {
        net::http::Request req;
        r.setup_request(req);

        // Handle the request, server side
        unique_ptr<Reader> ds(makeReader());

        dataset::http::QueryDataParams params;
        params.parse_get_or_post(req);

        dataset::http::ReaderServer srv(*ds, dsname);
        srv.do_queryData(params, req);

        r.read_response();
    }

    // Run the fake request through a server-side summary handler
    void do_queryBytes(arki::tests::FakeRequest& r)
    {
        net::http::Request req;
        r.setup_request(req);

        // Handle the request, server side
        unique_ptr<Reader> ds(makeReader());

        dataset::http::QueryBytesParams params(".");
        params.parse_get_or_post(req);

        dataset::http::ReaderServer srv(*ds, dsname);
        srv.do_queryBytes(params, req);

        r.read_response();
    }

    // Run the fake request through a server-side summary handler
    void do_config(arki::tests::FakeRequest& r)
    {
        net::http::Request req;
        r.setup_request(req);

        // Handle the request, server side
        unique_ptr<Reader> ds(makeReader());

        dataset::http::ReaderServer srv(*ds, dsname);
        srv.do_config(cfg, req);

        r.read_response();
    }

    // Test /summary/
    void test_summary()
    {
        // Make the request
        arki::tests::BufferFakeRequest r;
        r.write_get("/foo");

        // Handle the request, server side
        wruntest(do_summary, r);

        // Handle the response, client side
        wassert(actual(r.response_method) == "HTTP/1.0 200 OK");
        wassert(actual(r.response_headers["content-type"]) == "application/octet-stream");
        wassert(actual(r.response_headers["content-disposition"]) == "attachment; filename=testds-summary.bin");

        Summary s;
        s.read(r.response_body, "response body");
        wassert(actual(s.count()) == 3u);
    }

    // Test /query/
    void test_query()
    {
        // Make the request
        arki::tests::BufferFakeRequest r;
        r.write_get("/foo");

        // Handle the request, server side
        wruntest(do_query, r);

        // Handle the response, client side
        wassert(actual(r.response_method) == "HTTP/1.0 200 OK");
        wassert(actual(r.response_headers["content-type"]) == "application/octet-stream");
        wassert(actual(r.response_headers["content-disposition"]) == "attachment; filename=testds.bin");

        metadata::Collection mdc;
        Metadata::read_buffer(r.response_body, metadata::ReadContext("", "(response body)"), mdc.inserter_func());

        wassert(actual(mdc.size()) == 3u);
    }

    // Test /querydata/
    void test_querydata()
    {
        // Make the request
        arki::tests::BufferFakeRequest r;
        r.write_get("/foo");

        // Handle the request, server side
        wruntest(do_queryData, r);

        // Handle the response, client side
        wassert(actual(r.response_method) == "HTTP/1.0 200 OK");
        wassert(actual(r.response_headers["content-type"]) == "application/octet-stream");
        wassert(actual(r.response_headers["content-disposition"]) == "attachment; filename=testds.arkimet");

        metadata::Collection mdc;
        Metadata::read_buffer(r.response_body, metadata::ReadContext("", "(response body)"), mdc.inserter_func());

        wassert(actual(mdc.size()) == 3u);
    }

    // Test /querybytes/
    void test_querybytes()
    {
        // Make the request
        arki::tests::BufferFakeRequest r;
        r.write_get("/foo");

        // Handle the request, server side
        wruntest(do_queryBytes, r);

        // Handle the response, client side
        wassert(actual(r.response_method) == "HTTP/1.0 200 OK");
        wassert(actual(r.response_headers["content-type"]) == "application/octet-stream");
        wassert(actual(r.response_headers["content-disposition"]) == "attachment; filename=testds.txt");

        wassert(actual(r.response_body.size()) == 44412u);
        wassert(actual(r.response_body[0]) == 'G');
        wassert(actual(r.response_body[1]) == 'R');
        wassert(actual(r.response_body[2]) == 'I');
        wassert(actual(r.response_body[3]) == 'B');
    }

    // Test /config/
    void test_config()
    {
        // Make the request
        arki::tests::StringFakeRequest r;
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
    void test_configlocked()
    {
        unique_ptr<Writer> ds(makeWriter());
        Pending p = ds->test_writelock();

        wruntest(test_config);
    }


    void test_all()
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
