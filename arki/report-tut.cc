/*
 * Copyright (C) 2008--2011  ARPA-SIM <urpsim@smr.arpa.emr.it>
 *
 * This program is free software; you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation; either version 2 of the License, or
 * (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License along
 * with this program; if not, write to the Free Software Foundation, Inc.,
 * 51 Franklin Street, Fifth Floor, Boston, MA 02110-1301 USA.
 *
 * Author: Enrico Zini <enrico@enricozini.com>
 */

#include "config.h"

#include <arki/matcher/tests.h>
#include <arki/report.h>
#include <arki/metadata.h>
#include <arki/summary.h>
#include <arki/types.h>
#include <arki/configfile.h>
#include <arki/dataset.h>
#include <arki/dataset/file.h>

#include <sstream>
#include <iostream>

namespace tut {
using namespace std;
using namespace arki;

struct arki_report_shar {
    Metadata md;

    arki_report_shar()
    {
        md.create();
        arki::tests::fill(md);
	}
};
TESTGRP(arki_report);

// Test a simple case
template<> template<>
void to::test<1>()
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
	rep(md);
	rep.report();
	ensure_equals(res.str(), "1\t0\n");

	Summary s;
	res.str(string());
	rep(s);
	rep.report();
	ensure_equals(res.str(), "1\t1\n");
}

// Test a case of mdreport that used to fail
template<> template<>
void to::test<2>()
{
    ConfigFile cfg;
    dataset::File::readConfig("inbound/test.grib1", cfg);
    auto_ptr<ReadonlyDataset> ds(ReadonlyDataset::create(*cfg.section("test.grib1")));

    // Scan it to be sure it can be read
    dataset::ByteQuery q;
    q.matcher = Matcher::parse("");
    q.type = dataset::ByteQuery::BQ_REP_METADATA;
    q.param = "mdstats";

    stringstream out;
    ds->queryBytes(q, out);

    ensure_contains(out.str(), "origin: GRIB1(080, 255, 100)");
    ensure_contains(out.str(), "origin: GRIB1(098, 000, 129)");
    ensure_contains(out.str(), "origin: GRIB1(200, 000, 101)");
}

}

// vim:set ts=4 sw=4:
