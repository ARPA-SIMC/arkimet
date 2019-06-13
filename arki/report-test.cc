#include "arki/metadata/tests.h"
#include "arki/utils/sys.h"
#include "arki/configfile.h"
#include "arki/summary.h"
#include "arki/dataset/file.h"
#include "report.h"

namespace {
using namespace std;
using namespace arki;
using namespace arki::tests;
using namespace arki::utils;

inline unique_ptr<Metadata> wrap(const Metadata& md)
{
    return unique_ptr<Metadata>(new Metadata(md));
}

class Tests : public TestCase
{
    using TestCase::TestCase;
    void register_tests() override;
} test("arki_report");

void Tests::register_tests() {

add_method("simple", [] {
    Metadata md;
    arki::tests::fill(md);

    Report rep;
    std::stringstream res;
    rep.captureOutput(res);
    rep.loadString(R"(
Report = {
  mdcount = 0,
  sumcount = 0,
  readMetadata = function(self, md)
    self.mdcount = self.mdcount + 1
  end,
  readSummary = function(self, s)
    self.sumcount = self.sumcount + 1
  end,
  report = function(self)
    print(self.mdcount, self.sumcount)
  end
}
)");

    rep.report();
    wassert(actual(res.str()) == "0\t0\n");

    res.str(string());
    rep.eat(wrap(md));
    rep.report();
    wassert(actual(res.str()) == "1\t0\n");

    Summary s;
    res.str(string());
    rep(s);
    rep.report();
    wassert(actual(res.str()) == "1\t1\n");
});

// Test a case of mdreport that used to fail
add_method("regression", [] {
    ConfigFile cfg;
    dataset::File::read_config("inbound/test.grib1", cfg);
    unique_ptr<dataset::Reader> ds(dataset::Reader::create(*cfg.section("test.grib1")));

    // Scan it to be sure it can be read
    dataset::ByteQuery q;
    q.matcher = Matcher::parse("");
    q.type = dataset::ByteQuery::BQ_REP_METADATA;
    q.param = "mdstats";

    sys::File out(sys::File::mkstemp("test"));
    ds->query_bytes(q, out);
    out.close();

    string res = sys::read_file(out.name());
    wassert(actual(res).contains("origin: GRIB1(080, 255, 100)"));
    wassert(actual(res).contains("origin: GRIB1(098, 000, 129)"));
    wassert(actual(res).contains("origin: GRIB1(200, 000, 101)"));
});

}

}
