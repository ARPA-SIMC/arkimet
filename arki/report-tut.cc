#include "config.h"
#include <arki/matcher/tests.h>
#include <arki/report.h>
#include <arki/metadata.h>
#include <arki/summary.h>
#include <arki/types.h>
#include <arki/configfile.h>
#include <arki/dataset.h>
#include <arki/dataset/file.h>
#include <arki/utils/sys.h>
#include <sstream>
#include <iostream>

namespace tut {
using namespace std;
using namespace arki;
using namespace arki::utils;
using namespace arki::tests;

struct arki_report_shar
{
    Metadata md;

    arki_report_shar()
    {
        arki::tests::fill(md);
    }
};
TESTGRP(arki_report);

namespace {
inline unique_ptr<Metadata> wrap(const Metadata& md)
{
    return unique_ptr<Metadata>(new Metadata(md));
}
}

// Test a simple case
def_test(1)
{
	Report rep;
	std::stringstream res;
	rep.captureOutput(res);
	rep.loadString(
		"Report = { \n"
		"  mdcount = 0, \n"
		"  sumcount = 0, \n"
		"  readMetadata = function(self, md) \n"
		"    self.mdcount = self.mdcount + 1 \n"
		"  end, \n"
		"  readSummary = function(self, s) \n"
		"    self.sumcount = self.sumcount + 1 \n"
		"  end, \n"
		"  report = function(self) \n"
		"    print(self.mdcount, self.sumcount) \n"
		"  end \n"
		"} \n"
	);
	
	rep.report();
	ensure_equals(res.str(), "0\t0\n");

	res.str(string());
    rep.eat(wrap(md));
	rep.report();
	ensure_equals(res.str(), "1\t0\n");

	Summary s;
	res.str(string());
	rep(s);
	rep.report();
	ensure_equals(res.str(), "1\t1\n");
}

// Test a case of mdreport that used to fail
def_test(2)
{
    ConfigFile cfg;
    dataset::File::readConfig("inbound/test.grib1", cfg);
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
}

}

// vim:set ts=4 sw=4:
