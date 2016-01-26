#include "test-utils.h"
#include "inbound.h"
#include "arki/metadata/collection.h"
#include "arki/types/source/blob.h"
#include "arki/utils/sys.h"
#include "config.h"
#include <iostream>

using namespace std;
using namespace arki;
using namespace arki::utils;
using namespace arki::tests;

namespace {
inline std::string dsname(const Metadata& md)
{
    if (!md.has_source_blob()) return "(md source is not a blob source)";
    string pathname = md.sourceBlob().filename;
    size_t pos = pathname.find('/');
    if (pos == string::npos)
        return pathname;
    else
        return pathname.substr(0, pos);
}

struct Fixture : public arki::tests::Fixture {
    using arki::tests::Fixture::Fixture;

    ConfigFile import_config;

    void test_setup()
    {
        import_config.clear();
        import_config.parse(R"(
[testds]
path = testds
name = testds
type = simple
step = daily
filter = origin:GRIB1
remote import = yes

[error]
path = error
name = error
type = simple
step = daily
remote import = yes
)");

        for (ConfigFile::const_section_iterator i = import_config.sectionBegin();
                i != import_config.sectionEnd(); ++i)
        {
            if (sys::exists(i->second->value("path")))
                sys::rmtree(i->second->value("path"));
            sys::makedirs(i->second->value("path"));
        }
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

class Tests : public FixtureTestCase<Fixture>
{
    using FixtureTestCase::FixtureTestCase;

    void register_tests() override;
} test("arki_dataset_http_inbound");

void Tests::register_tests() {

// Test /inbound/scan/
add_method("scan", [](Fixture& f) {
    // Make the request
    arki::tests::BufferFakeRequest r;
    r.write_get("/inbound/scan?file=test.grib1");

    // Handle the request, server side
    wassert(f.do_scan(r));

    // Handle the response, client side
    ensure_equals(r.response_method, "HTTP/1.0 200 OK");
    ensure_equals(r.response_headers["content-type"], "application/octet-stream");
    ensure_equals(r.response_headers["content-disposition"], "attachment; filename=test.grib1.arkimet");

    metadata::Collection mdc;
    Metadata::read_buffer(r.response_body, metadata::ReadContext("", "(response body)"), mdc.inserter_func());
    ensure_equals(mdc.size(), 3u);
});

// Test /inbound/testdispatch/
add_method("testdispatch", [](Fixture& f) {
    // Make the request
    arki::tests::StringFakeRequest r;
    r.write_get("/inbound/testdispatch?file=test.grib1");

    // Handle the request, server side
    wassert(f.do_testdispatch(r));

    // Handle the response, client side
    ensure_equals(r.response_method, "HTTP/1.0 200 OK");
    ensure_equals(r.response_headers["content-type"], "text/plain");
    ensure_equals(r.response_headers["content-disposition"], "attachment; filename=test.grib1.log");

    wassert(actual(r.response_body).contains("test.grib1:0+7218): acquire to testds dataset"));
    wassert(actual(r.response_body).contains("test.grib1:7218+34960): acquire to testds dataset"));
    wassert(actual(r.response_body).contains("test.grib1:42178+2234): acquire to testds dataset"));
});

// Test /inbound/dispatch/
add_method("dispatch", [](Fixture& f) {
    system("cp inbound/test.grib1 inbound/copy.grib1");

    // Make the request
    arki::tests::BufferFakeRequest r;
    r.write_get("/inbound/dispatch?file=copy.grib1");

    // Handle the request, server side
    wassert(f.do_dispatch(r));

    // Handle the response, client side
    ensure_equals(r.response_method, "HTTP/1.0 200 OK");
    ensure_equals(r.response_headers["content-type"], "application/octet-stream");
    ensure_equals(r.response_headers["content-disposition"], "attachment; filename=copy.grib1.arkimet");

    metadata::Collection mdc;
    Metadata::read_buffer(r.response_body, metadata::ReadContext("(response body)", ""), mdc.inserter_func());
    wassert(actual(mdc.size()) == 3u);
    wassert(actual(dsname(mdc[0])) == "testds");
    wassert(actual(dsname(mdc[1])) == "testds");
    wassert(actual(dsname(mdc[2])) == "testds");

    ensure(!sys::exists("inbound/copy.grib1"));
});

}
}
