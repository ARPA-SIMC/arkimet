#include "config.h"

#include <arki/dataset/http/test-utils.h>
#include <arki/dataset/http/inbound.h>
#include <arki/metadata/collection.h>
#include <arki/types/assigneddataset.h>
#include <arki/utils/sys.h>

#include <sstream>
#include <iostream>

namespace tut {
using namespace std;
using namespace arki;
using namespace arki::utils;
using namespace arki::tests;

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
        for (ConfigFile::const_section_iterator i = import_config.sectionBegin();
                i != import_config.sectionEnd(); ++i)
            clean(i->second);
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

    // Run the fake request through the server-side handler
    void do_dispatch(arki::tests::FakeRequest& r)
    {
        net::http::Request req;
        r.setup_request(req);

        dataset::http::InboundParams params;
        params.parse_get_or_post(req);

        dataset::http::InboundServer srv(import_config, "inbound");
        srv.do_dispatch(params, req);

        r.read_response();
    }
};
TESTGRP(arki_dataset_http_inbound);

// Test /inbound/scan/
def_test(1)
{
    // Make the request
    arki::tests::BufferFakeRequest r;
    r.write_get("/inbound/scan?file=test.grib1");

    // Handle the request, server side
    do_scan(r);

    // Handle the response, client side
    ensure_equals(r.response_method, "HTTP/1.0 200 OK");
    ensure_equals(r.response_headers["content-type"], "application/octet-stream");
    ensure_equals(r.response_headers["content-disposition"], "attachment; filename=test.grib1.arkimet");

    metadata::Collection mdc;
    Metadata::read_buffer(r.response_body, metadata::ReadContext("", "(response body)"), mdc.inserter_func());
    ensure_equals(mdc.size(), 3u);
}

// Test /inbound/testdispatch/
def_test(2)
{
    // Make the request
    arki::tests::StringFakeRequest r;
    r.write_get("/inbound/testdispatch?file=test.grib1");

    // Handle the request, server side
    do_testdispatch(r);

    // Handle the response, client side
    ensure_equals(r.response_method, "HTTP/1.0 200 OK");
    ensure_equals(r.response_headers["content-type"], "text/plain");
    ensure_equals(r.response_headers["content-disposition"], "attachment; filename=test.grib1.log");

    wassert(actual(r.response_body).contains("test.grib1:0+7218): acquire to testds dataset"));
    wassert(actual(r.response_body).contains("test.grib1:7218+34960): acquire to testds dataset"));
    wassert(actual(r.response_body).contains("test.grib1:42178+2234): acquire to testds dataset"));
}

// Test /inbound/dispatch/
def_test(3)
{
    system("cp inbound/test.grib1 inbound/copy.grib1");

    // Make the request
    arki::tests::BufferFakeRequest r;
    r.write_get("/inbound/dispatch?file=copy.grib1");

    // Handle the request, server side
    do_dispatch(r);

    // Handle the response, client side
    ensure_equals(r.response_method, "HTTP/1.0 200 OK");
    ensure_equals(r.response_headers["content-type"], "application/octet-stream");
    ensure_equals(r.response_headers["content-disposition"], "attachment; filename=copy.grib1.arkimet");

    metadata::Collection mdc;
    Metadata::read_buffer(r.response_body, metadata::ReadContext("(response body)", ""), mdc.inserter_func());
    ensure_equals(mdc.size(), 3u);
    ensure_equals(mdc[0].get<types::AssignedDataset>()->name, "testds");
    ensure_equals(mdc[1].get<types::AssignedDataset>()->name, "testds");
    ensure_equals(mdc[2].get<types::AssignedDataset>()->name, "testds");

    ensure(!sys::exists("inbound/copy.grib1"));
}


}

// vim:set ts=4 sw=4:
