#include "arki/types/tests.h"
#include "arki/dataset.h"
#include "arki/dataset/http.h"
#include "arki/metadata.h"
#include "arki/metadata/data.h"
#include "arki/metadata/collection.h"
#include "arki/matcher.h"
#include "arki/summary.h"
#include "arki/scan/grib.h"
#include "arki/dispatcher.h"
#include "arki/types/source/blob.h"
#include "arki/utils.h"
#include "arki/utils/sys.h"
#include "arki/binary.h"
#include <sys/types.h>
#include <sys/stat.h>
#include <fcntl.h>

namespace {
using namespace std;
using namespace arki;
using namespace arki::types;
using namespace arki::utils;
using namespace arki::tests;

class Tests : public TestCase
{
    using TestCase::TestCase;

    void register_tests() override;
} test("arki_server");

void Tests::register_tests() {

// Test data integrity of postprocessed queries through a server (used to fail
// after offset 0xc00)
add_method("postproc_error1", [] {
    using namespace arki::dataset;

    // Get the normal data
    vector<uint8_t> plain;
    {
        core::cfg::Section cfg;
        cfg.set("type", "ondisk2");
        cfg.set("path", "test80");
        cfg.set("name", "test80");
        cfg.set("step", "daily");
        cfg.set("filter", "origin:GRIB1,80");
        cfg.set("postprocess", "cat,echo,say,checkfiles,error,outthenerr");
        unique_ptr<dataset::Reader> ds(dataset::Reader::create(cfg));

        DataQuery dq(Matcher::parse(""), true);
        BinaryEncoder enc(plain);
        ds->query_data(dq, [&](unique_ptr<Metadata> md) {
            md->makeInline();
            md->encodeBinary(enc);
            enc.add_raw(md->get_data().read());
            return true;
        });
    }

    // Capture the data after going through the postprocessor
    string postprocessed;
    {
        auto config = dataset::http::Reader::load_cfg_sections("http://localhost:7117");

        sys::File out(sys::File::mkstemp("test"));
        unique_ptr<dataset::Reader> testds(dataset::Reader::create(*config.section("test80")));
        wassert(actual(dynamic_cast<dataset::http::Reader*>(testds.get())).istrue());

        dataset::ByteQuery bq;
        bq.setPostprocess(Matcher::parse("origin:GRIB1,80"), "cat");
        testds->query_bytes(bq, out);
        out.close();
        postprocessed = sys::read_file(out.name());
    }

    wassert(actual(plain.size()) == postprocessed.size());

    /*
    size_t diffs = 0;
    for (size_t i = 0; i < plain.size(); ++i)
    {
        if (plain[i] != postprocessed.str()[i])
        {
            fprintf(stderr, "Difference at offset %d %x %u!=%u\n", (int)i, (int)i, (int)plain[i], (int)postprocessed.str()[i]);
            if (++diffs > 20)
                break;
        }
    }

    ensure(plain == postprocessed.str());
    */

    metadata::Collection mdc1, mdc2;
    {
        Metadata::read_buffer(plain, metadata::ReadContext("", "plain"), mdc1.inserter_func());
    }
    {
        vector<uint8_t> buf(postprocessed.begin(), postprocessed.end());
        Metadata::read_buffer(buf, metadata::ReadContext("", "postprocessed"), mdc2.inserter_func());
    }

    // Remove those metadata that contain test-dependent timestamps
    std::vector<Note> nonotes;
    mdc1[0].unset(TYPE_ASSIGNEDDATASET);
    mdc2[0].unset(TYPE_ASSIGNEDDATASET);
    mdc1[0].set_notes(nonotes);
    mdc2[0].set_notes(nonotes);

    // Compare YAML versions so we get readable output
    wassert(actual(mdc1[0].to_yaml()) == mdc2[0].to_yaml());

    // Compare data
    const auto& d1 = mdc1[0].get_data().read();
    const auto& d2 = mdc2[0].get_data().read();

    wassert(actual(d1) == d2);
});

// Test downloading the server alias database
add_method("aliases", [] {
    auto cfg = dataset::http::Reader::getAliasDatabase("http://localhost:7117");
    wassert(actual(cfg.section("origin") != nullptr).istrue());
});

// Test uploading postprocessor data
add_method("postproc_data", [] {
    auto cfg = dataset::http::Reader::load_cfg_section("http://localhost:7117/dataset/test200");
    unique_ptr<dataset::Reader> testds(dataset::Reader::create(cfg));
    setenv("ARKI_POSTPROC_FILES", "inbound/test.grib1:inbound/padded.grib1", 1);

    // Files should be uploaded and notified to the postprocessor script
    sys::File out(sys::File::mkstemp("test"));
    dataset::ByteQuery bq;
    bq.setPostprocess(Matcher(), "checkfiles");
    testds->query_bytes(bq, out);
    out.close();
    string res = sys::read_file(out.name());
    wassert(actual(res) == "padded.grib1:test.grib1\n");
});

// Test access and error logs
add_method("logs", [] {
    auto cfg = dataset::http::Reader::load_cfg_section("http://localhost:7117/dataset/test200");
    unique_ptr<dataset::Reader> testds(dataset::Reader::create(cfg));

    // Run a successful query
    testds->query_data(Matcher(), [](unique_ptr<Metadata> md) { return true; });

    // Run a broken query
    dynamic_cast<dataset::http::Reader*>(testds.get())->produce_one_wrong_query();
    try {
        testds->query_data(Matcher(), [](unique_ptr<Metadata> md) { return true; });
        wassert(actual(false).istrue());
    } catch (std::runtime_error& e) {
        wassert(actual(e.what()).contains("MISCHIEF"));
    }

    // Check that something got logged
    wassert(actual(sys::size("access.log")) > 0u);
    wassert(actual(sys::size("error.log")) > 0u);
});

// Test style=binary summary queries
add_method("query_global_summary_style_binary", [] {
    dataset::http::CurlEasy curl;

    dataset::http::BufState<std::string> request(curl);
    request.set_url("http://localhost:7117/summary?style=binary");
    request.set_method("POST");
    request.post_data.add_string("query", Matcher().toStringExpanded());
    request.perform();

    wassert(actual(request.buf).startswith("SU"));
});

// Test style=binary summary queries
add_method("query_summary_style_binary", [] {
    dataset::http::CurlEasy curl;

    dataset::http::BufState<std::string> request(curl);
    request.set_url("http://localhost:7117/dataset/test200/summary?style=binary");
    request.set_method("POST");
    request.post_data.add_string("query", Matcher().toStringExpanded());
    request.perform();

    wassert(actual(request.buf).startswith("SU"));
});

// Test style=json summary queries
add_method("query_summary_style_json", [] {
    dataset::http::CurlEasy curl;

    dataset::http::BufState<std::string> request(curl);
    request.set_url("http://localhost:7117/dataset/test200/summary?style=json");
    request.set_method("POST");
    request.post_data.add_string("query", Matcher().toStringExpanded());
    request.perform();

    wassert(actual(request.buf).startswith("{"));
});

// Test style=yaml summary queries
add_method("query_summary_style_yaml", [] {
    dataset::http::CurlEasy curl;

    dataset::http::BufState<std::string> request(curl);
    request.set_url("http://localhost:7117/dataset/test200/summary?style=yaml");
    request.set_method("POST");
    request.post_data.add_string("query", Matcher().toStringExpanded());
    request.perform();

    wassert(actual(request.buf).startswith("SummaryItem"));
});

// Test style=json summary queries
add_method("query_summaryshort_style_json", [] {
    dataset::http::CurlEasy curl;

    dataset::http::BufState<std::string> request(curl);
    request.set_url("http://localhost:7117/dataset/test200/summaryshort?style=json");
    request.set_method("POST");
    request.post_data.add_string("query", Matcher().toStringExpanded());
    request.perform();

    wassert(actual(request.buf).startswith("{"));
});

// Test style=yaml summary queries
add_method("query_summaryshort_style_yaml", [] {
    dataset::http::CurlEasy curl;

    dataset::http::BufState<std::string> request(curl);
    request.set_url("http://localhost:7117/dataset/test200/summaryshort?style=yaml");
    request.set_method("POST");
    request.post_data.add_string("query", Matcher().toStringExpanded());
    request.perform();

    wassert(actual(request.buf).startswith("SummaryStats"));
});

}

}
