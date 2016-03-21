#include <arki/dataset/http/test-utils.h>
#include <arki/dataset/http/server.h>
#include <arki/summary.h>
#include <arki/metadata/collection.h>
#include <arki/binary.h>
#include <sstream>

using namespace std;
using namespace arki;
using namespace arki::utils;
using namespace arki::tests;

namespace {

struct Fixture : public DatasetTest {
    using DatasetTest::DatasetTest;

    void test_setup()
    {
        DatasetTest::test_setup(R"(
            type=simple
            step=daily
            postprocess=testcountbytes
        )");

        clean_and_import();
    }

    // Run the fake request through a server-side summary handler
    void do_summary(arki::tests::FakeRequest& r)
    {
        net::http::Request req;
        r.setup_request(req);

        // Handle the request, server side
        unique_ptr<dataset::Reader> ds(makeReader());
        dataset::http::LegacySummaryParams params;
        params.parse_get_or_post(req);

        dataset::http::ReaderServer srv(*ds, ds_name);
        srv.do_summary(params, req);

        r.read_response();
    }

    // Run the fake request through a server-side summary handler
    void do_query(arki::tests::FakeRequest& r)
    {
        net::http::Request req;
        r.setup_request(req);

        // Handle the request, server side
        unique_ptr<dataset::Reader> ds(makeReader());

        dataset::http::LegacyQueryParams params(".");
        params.parse_get_or_post(req);

        dataset::http::ReaderServer srv(*ds, ds_name);
        srv.do_query(params, req);

        r.read_response();
    }

    // Run the fake request through a server-side summary handler
    void do_queryData(arki::tests::FakeRequest& r)
    {
        net::http::Request req;
        r.setup_request(req);

        // Handle the request, server side
        unique_ptr<dataset::Reader> ds(makeReader());

        dataset::http::QueryDataParams params;
        params.parse_get_or_post(req);

        dataset::http::ReaderServer srv(*ds, ds_name);
        srv.do_queryData(params, req);

        r.read_response();
    }

    // Run the fake request through a server-side summary handler
    void do_queryBytes(arki::tests::FakeRequest& r)
    {
        net::http::Request req;
        r.setup_request(req);

        // Handle the request, server side
        unique_ptr<dataset::Reader> ds(makeReader());

        dataset::http::QueryBytesParams params(".");
        params.parse_get_or_post(req);

        dataset::http::ReaderServer srv(*ds, ds_name);
        srv.do_queryBytes(params, req);

        r.read_response();
    }

    // Run the fake request through a server-side summary handler
    void do_config(arki::tests::FakeRequest& r)
    {
        net::http::Request req;
        r.setup_request(req);

        // Handle the request, server side
        unique_ptr<dataset::Reader> ds(makeReader());

        dataset::http::ReaderServer srv(*ds, ds_name);
        srv.do_config(cfg, req);

        r.read_response();
    }

    void test_config()
    {
        // Make the request
        arki::tests::StringFakeRequest r;
        r.write_get("/foo");

        // Handle the request, server side
        wassert(do_config(r));

        // Handle the response, client side
        wassert(actual(r.response_method) == "HTTP/1.0 200 OK");
        wassert(actual(r.response_headers["content-type"]) == "text/plain");
        wassert(actual(r.response_headers["content-disposition"]) == "");

        stringstream buf;
        buf << "[testds]" << endl;
        cfg.output(buf, "memory");
        wassert(actual(r.response_body) == buf.str());
    }
};

class Tests : public FixtureTestCase<Fixture>
{
    using FixtureTestCase::FixtureTestCase;

    void register_tests() override;
};

Tests test_simple_plain("arki_dataset_http_server_simple_plain", "type=simple");
Tests test_simple_sqlite("arki_dataset_http_server_simple_sqlite", "type=simple\nindex_type=sqlite\n");
Tests test_ondisk2("arki_dataset_http_server_ondisk2", "type=ondisk2");

void Tests::register_tests() {

// Test /summary/
add_method("summary", [](Fixture& f) {
    // Make the request
    arki::tests::BufferFakeRequest r;
    r.write_get("/foo");

    // Handle the request, server side
    wassert(f.do_summary(r));

    // Handle the response, client side
    wassert(actual(r.response_method) == "HTTP/1.0 200 OK");
    wassert(actual(r.response_headers["content-type"]) == "application/octet-stream");
    wassert(actual(r.response_headers["content-disposition"]) == "attachment; filename=testds-summary.bin");

    Summary s;
    BinaryDecoder dec(r.response_body);
    s.read(dec, "response body");
    wassert(actual(s.count()) == 3u);
});

// Test /query/
add_method("query", [](Fixture& f) {
    // Make the request
    arki::tests::BufferFakeRequest r;
    r.write_get("/foo");

    // Handle the request, server side
    wassert(f.do_query(r));

    // Handle the response, client side
    wassert(actual(r.response_method) == "HTTP/1.0 200 OK");
    wassert(actual(r.response_headers["content-type"]) == "application/octet-stream");
    wassert(actual(r.response_headers["content-disposition"]) == "attachment; filename=testds.bin");

    metadata::Collection mdc;
    Metadata::read_buffer(r.response_body, metadata::ReadContext("", "(response body)"), mdc.inserter_func());

    wassert(actual(mdc.size()) == 3u);
});

// Test /querydata/
add_method("querydata", [](Fixture& f) {
    // Make the request
    arki::tests::BufferFakeRequest r;
    r.write_get("/foo");

    // Handle the request, server side
    wassert(f.do_queryData(r));

    // Handle the response, client side
    wassert(actual(r.response_method) == "HTTP/1.0 200 OK");
    wassert(actual(r.response_headers["content-type"]) == "application/octet-stream");
    wassert(actual(r.response_headers["content-disposition"]) == "attachment; filename=testds.arkimet");

    metadata::Collection mdc;
    Metadata::read_buffer(r.response_body, metadata::ReadContext("", "(response body)"), mdc.inserter_func());

    wassert(actual(mdc.size()) == 3u);
});

// Test /querybytes/
add_method("querybytes", [](Fixture& f) {
    // Make the request
    arki::tests::BufferFakeRequest r;
    r.write_get("/foo");

    // Handle the request, server side
    wassert(f.do_queryBytes(r));

    // Handle the response, client side
    wassert(actual(r.response_method) == "HTTP/1.0 200 OK");
    wassert(actual(r.response_headers["content-type"]) == "application/octet-stream");
    wassert(actual(r.response_headers["content-disposition"]) == "attachment; filename=testds.txt");

    wassert(actual(r.response_body.size()) == 44412u);
    wassert(actual(r.response_body[0]) == 'G');
    wassert(actual(r.response_body[1]) == 'R');
    wassert(actual(r.response_body[2]) == 'I');
    wassert(actual(r.response_body[3]) == 'B');
});

// Test /config/
add_method("config", [](Fixture& f) {
    wassert(f.test_config());
});

// Test /config/ with a locked DB
add_method("config_locked", [](Fixture& f) {
    unique_ptr<dataset::Writer> ds(f.makeWriter());
    Pending p = ds->test_writelock();

    wassert(f.test_config());
});


}
}
